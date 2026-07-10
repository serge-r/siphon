//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cilium/ebpf"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

const (
	ipfixVersion       = 10
	templateSetID      = 2
	flowTemplateID     = 256
	observationDomain  = 1
	ipfixHeaderLen     = 16
	ipfixSetHeaderLen  = 4
	ipfixTemplateLen   = 4 + 7*4
	ipfixFlowRecordLen = 4 + 4 + 2 + 2 + 1 + 8 + 8
	maxUDPIPFIXLen     = 65507
	maxMapEntries      = ^uint32(0)
)

const (
	statNewFlowInsertAttempts uint32 = iota
	statFlowInsertFailures
)

type flowKey struct {
	SrcIP   uint32
	DstIP   uint32
	SrcPort uint16
	DstPort uint16
	Proto   uint8
	Pad     [3]byte
}

type flowStats struct {
	Packets uint64
	Bytes   uint64
}

type flowRecord struct {
	Key     flowKey
	Packets uint64
	Bytes   uint64
}

type bpfStats struct {
	NewFlowInsertAttempts uint64
	FlowInsertFailures    uint64
}

type config struct {
	iface     string
	collector string
	direction string
	interval  time.Duration
	maxFlows  uint
	debug     bool
}

// debugEnabled gates verbose logging; set once from the -debug flag in run().
var debugEnabled bool

func debugf(format string, args ...any) {
	if debugEnabled {
		log.Printf("[debug] "+format, args...)
	}
}

// formatFlowKey renders a flow key as "src_ip:port -> dst_ip:port proto=N".
// IPs are stored host-order (the BPF program applies ntohl), so the high byte
// is the first octet.
func formatFlowKey(k flowKey) string {
	src := net.IPv4(byte(k.SrcIP>>24), byte(k.SrcIP>>16), byte(k.SrcIP>>8), byte(k.SrcIP))
	dst := net.IPv4(byte(k.DstIP>>24), byte(k.DstIP>>16), byte(k.DstIP>>8), byte(k.DstIP))
	return fmt.Sprintf("%s:%d -> %s:%d proto=%d", src, k.SrcPort, dst, k.DstPort, k.Proto)
}

func main() {
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.iface, "iface", "eth0", "network interface to attach the TC program to")
	flag.StringVar(&cfg.collector, "collector", "127.0.0.1:4739", "IPFIX collector UDP address")
	flag.StringVar(&cfg.direction, "direction", "ingress", "TC hook direction: ingress or egress")
	flag.DurationVar(&cfg.interval, "interval", 10*time.Second, "flow map polling interval")
	flag.UintVar(&cfg.maxFlows, "max-flows", 10000, "maximum number of flow entries kept in the eBPF LRU map")
	flag.BoolVar(&cfg.debug, "debug", false, "enable verbose debug logging")
	flag.Parse()
	return cfg
}

func run(ctx context.Context, cfg config) error {
	if cfg.interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	if cfg.maxFlows == 0 {
		return fmt.Errorf("max-flows must be positive")
	}
	if cfg.maxFlows > uint(maxMapEntries) {
		return fmt.Errorf("max-flows must be <= %d", maxMapEntries)
	}

	debugEnabled = cfg.debug
	log.Printf("siphon-probe %s starting", version)

	spec, err := loadProbe()
	if err != nil {
		return fmt.Errorf("load embedded eBPF object: %w", err)
	}
	if flowMapSpec := spec.Maps["flow_map"]; flowMapSpec != nil {
		flowMapSpec.MaxEntries = uint32(cfg.maxFlows)
	} else {
		return fmt.Errorf("eBPF object does not contain map spec flow_map")
	}

	coll, err := ebpf.NewCollection(spec)
	if err != nil {
		return fmt.Errorf("create eBPF collection: %w", err)
	}
	defer coll.Close()

	prog := coll.Programs["tc_monitor"]
	if prog == nil {
		return fmt.Errorf("eBPF object does not contain program tc_monitor")
	}

	flowMap := coll.Maps["flow_map"]
	if flowMap == nil {
		return fmt.Errorf("eBPF object does not contain map flow_map")
	}

	statsMap := coll.Maps["stats_map"]
	if statsMap == nil {
		return fmt.Errorf("eBPF object does not contain map stats_map")
	}

	if debugEnabled {
		numCPU, _ := ebpf.PossibleCPU()
		debugf("flow_map: type=%s keySize=%d valueSize=%d maxEntries=%d flags=%d",
			flowMap.Type(), flowMap.KeySize(), flowMap.ValueSize(), flowMap.MaxEntries(), flowMap.Flags())
		debugf("stats_map: type=%s valueSize=%d maxEntries=%d",
			statsMap.Type(), statsMap.ValueSize(), statsMap.MaxEntries())
		debugf("possibleCPU=%d", numCPU)
	}

	link, err := netlink.LinkByName(cfg.iface)
	if err != nil {
		return fmt.Errorf("find interface %q: %w", cfg.iface, err)
	}

	if err := ensureClsact(link); err != nil {
		return err
	}

	filter, err := attachTC(link, cfg.direction, prog)
	if err != nil {
		return err
	}
	defer func() {
		if err := netlink.FilterDel(filter); err != nil && !errors.Is(err, unix.ENOENT) {
			log.Printf("delete TC filter: %v", err)
		}
	}()

	conn, err := net.Dial("udp", cfg.collector)
	if err != nil {
		return fmt.Errorf("connect to collector %q: %w", cfg.collector, err)
	}
	defer conn.Close()

	log.Printf("attached tc_monitor to %s/%s, max flows %d, polling every %s, exporting IPFIX to %s",
		cfg.iface, cfg.direction, cfg.maxFlows, cfg.interval, cfg.collector)

	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	var sequence uint32
	var previousStats bpfStats
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			records, err := drainFlowMap(flowMap)
			if err != nil {
				log.Printf("poll flow_map: %v", err)
				continue
			}

			if debugEnabled {
				debugf("poll: drained %d flow records", len(records))
				for _, r := range records {
					debugf("  flow %s packets=%d bytes=%d", formatFlowKey(r.Key), r.Packets, r.Bytes)
				}
			}

			currentStats, err := readBPFStats(statsMap)
			if err != nil {
				log.Printf("read stats_map: %v", err)
			} else {
				deltaStats := currentStats.Sub(previousStats)
				previousStats = currentStats
				logMapPressure(len(records), cfg.maxFlows, deltaStats)
				debugf("stats: new_flow_insert_attempts_delta=%d flow_insert_failures_delta=%d",
					deltaStats.NewFlowInsertAttempts, deltaStats.FlowInsertFailures)
			}

			if len(records) > 0 {
				for _, chunk := range chunkRecords(records) {
					packet := encodeIPFIX(chunk, sequence)
					sequence += uint32(len(chunk))
					if _, err := conn.Write(packet); err != nil {
						log.Printf("send IPFIX: %v", err)
						break
					}
				}
				debugf("exported %d flow records as IPFIX to %s, next sequence=%d",
					len(records), cfg.collector, sequence)
			}
		}
	}
}

func ensureClsact(link netlink.Link) error {
	qdisc := &netlink.Clsact{
		QdiscAttrs: netlink.QdiscAttrs{
			LinkIndex: link.Attrs().Index,
			Handle:    netlink.MakeHandle(0xffff, 0),
			Parent:    netlink.HANDLE_CLSACT,
		},
	}
	if err := netlink.QdiscAdd(qdisc); err != nil && !errors.Is(err, unix.EEXIST) {
		return fmt.Errorf("add clsact qdisc to %s: %w", link.Attrs().Name, err)
	}
	return nil
}

func attachTC(link netlink.Link, direction string, prog *ebpf.Program) (*netlink.BpfFilter, error) {
	parent, err := tcParent(direction)
	if err != nil {
		return nil, err
	}

	filter := &netlink.BpfFilter{
		FilterAttrs: netlink.FilterAttrs{
			LinkIndex: link.Attrs().Index,
			Parent:    parent,
			Handle:    netlink.MakeHandle(0, 1),
			Protocol:  unix.ETH_P_ALL,
			Priority:  1,
		},
		Fd:           prog.FD(),
		Name:         "tc_monitor",
		DirectAction: true,
	}

	if err := netlink.FilterReplace(filter); err != nil {
		return nil, fmt.Errorf("attach TC %s filter to %s: %w", direction, link.Attrs().Name, err)
	}
	return filter, nil
}

func tcParent(direction string) (uint32, error) {
	switch direction {
	case "ingress":
		return netlink.HANDLE_MIN_INGRESS, nil
	case "egress":
		return netlink.HANDLE_MIN_EGRESS, nil
	default:
		return 0, fmt.Errorf("direction must be ingress or egress")
	}
}

func drainFlowMap(flowMap *ebpf.Map) ([]flowRecord, error) {
	var records []flowRecord
	var keys []flowKey

	iter := flowMap.Iterate()
	var key flowKey
	var perCPU []flowStats
	for iter.Next(&key, &perCPU) {
		var packets, bytes uint64
		for _, cpuStats := range perCPU {
			packets += cpuStats.Packets
			bytes += cpuStats.Bytes
		}
		if packets > 0 || bytes > 0 {
			records = append(records, flowRecord{
				Key:     key,
				Packets: packets,
				Bytes:   bytes,
			})
		}
		keys = append(keys, key)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	for _, key := range keys {
		if err := flowMap.Delete(key); err != nil && !errors.Is(err, ebpf.ErrKeyNotExist) {
			return nil, err
		}
	}
	return records, nil
}

func readBPFStats(statsMap *ebpf.Map) (bpfStats, error) {
	newFlowInsertAttempts, err := readPerCPUCounter(statsMap, statNewFlowInsertAttempts)
	if err != nil {
		return bpfStats{}, fmt.Errorf("read new flow insert attempts: %w", err)
	}
	flowInsertFailures, err := readPerCPUCounter(statsMap, statFlowInsertFailures)
	if err != nil {
		return bpfStats{}, fmt.Errorf("read flow insert failures: %w", err)
	}

	return bpfStats{
		NewFlowInsertAttempts: newFlowInsertAttempts,
		FlowInsertFailures:    flowInsertFailures,
	}, nil
}

func readPerCPUCounter(statsMap *ebpf.Map, key uint32) (uint64, error) {
	var perCPU []uint64
	if err := statsMap.Lookup(key, &perCPU); err != nil {
		return 0, err
	}

	var total uint64
	for _, value := range perCPU {
		total += value
	}
	return total, nil
}

func (stats bpfStats) Sub(previous bpfStats) bpfStats {
	return bpfStats{
		NewFlowInsertAttempts: stats.NewFlowInsertAttempts - previous.NewFlowInsertAttempts,
		FlowInsertFailures:    stats.FlowInsertFailures - previous.FlowInsertFailures,
	}
}

func logMapPressure(exportedFlows int, maxFlows uint, stats bpfStats) {
	if exportedFlows >= int(maxFlows) {
		log.Printf("flow_map_capacity_pressure=1 exported_flows=%d max_flows=%d new_flow_insert_attempts_delta=%d flow_insert_failures_delta=%d; LRU evictions may have happened",
			exportedFlows, maxFlows, stats.NewFlowInsertAttempts, stats.FlowInsertFailures)
		return
	}

	if stats.FlowInsertFailures > 0 {
		log.Printf("flow_map_insert_failures=%d new_flow_insert_attempts_delta=%d exported_flows=%d max_flows=%d",
			stats.FlowInsertFailures, stats.NewFlowInsertAttempts, exportedFlows, maxFlows)
	}
}

func encodeIPFIX(records []flowRecord, sequence uint32) []byte {
	templateLen := ipfixSetHeaderLen + ipfixTemplateLen
	dataLen := ipfixSetHeaderLen + len(records)*ipfixFlowRecordLen
	totalLen := ipfixHeaderLen + templateLen + dataLen

	buf := bytes.NewBuffer(make([]byte, 0, totalLen))
	writeU16(buf, ipfixVersion)
	writeU16(buf, uint16(totalLen))
	writeU32(buf, uint32(time.Now().Unix()))
	writeU32(buf, sequence)
	writeU32(buf, observationDomain)

	writeTemplateSet(buf)
	writeDataSet(buf, records)

	return buf.Bytes()
}

func chunkRecords(records []flowRecord) [][]flowRecord {
	maxRecords := (maxUDPIPFIXLen - ipfixHeaderLen - ipfixSetHeaderLen - ipfixTemplateLen - ipfixSetHeaderLen) / ipfixFlowRecordLen
	if len(records) <= maxRecords {
		return [][]flowRecord{records}
	}

	chunks := make([][]flowRecord, 0, (len(records)+maxRecords-1)/maxRecords)
	for start := 0; start < len(records); start += maxRecords {
		end := min(start+maxRecords, len(records))
		chunks = append(chunks, records[start:end])
	}
	return chunks
}

func writeTemplateSet(buf *bytes.Buffer) {
	writeU16(buf, templateSetID)
	writeU16(buf, uint16(ipfixSetHeaderLen+ipfixTemplateLen))
	writeU16(buf, flowTemplateID)
	writeU16(buf, 7)

	writeField(buf, 8, 4)  // sourceIPv4Address
	writeField(buf, 12, 4) // destinationIPv4Address
	writeField(buf, 7, 2)  // sourceTransportPort
	writeField(buf, 11, 2) // destinationTransportPort
	writeField(buf, 4, 1)  // protocolIdentifier
	writeField(buf, 2, 8)  // packetDeltaCount
	writeField(buf, 1, 8)  // octetDeltaCount
}

func writeDataSet(buf *bytes.Buffer, records []flowRecord) {
	writeU16(buf, flowTemplateID)
	writeU16(buf, uint16(ipfixSetHeaderLen+len(records)*ipfixFlowRecordLen))
	for _, record := range records {
		writeU32(buf, record.Key.SrcIP)
		writeU32(buf, record.Key.DstIP)
		writeU16(buf, record.Key.SrcPort)
		writeU16(buf, record.Key.DstPort)
		buf.WriteByte(record.Key.Proto)
		writeU64(buf, record.Packets)
		writeU64(buf, record.Bytes)
	}
}

func writeField(buf *bytes.Buffer, elementID uint16, length uint16) {
	writeU16(buf, elementID)
	writeU16(buf, length)
}

func writeU16(buf *bytes.Buffer, v uint16) {
	_ = binary.Write(buf, binary.BigEndian, v)
}

func writeU32(buf *bytes.Buffer, v uint32) {
	_ = binary.Write(buf, binary.BigEndian, v)
}

func writeU64(buf *bytes.Buffer, v uint64) {
	_ = binary.Write(buf, binary.BigEndian, v)
}
