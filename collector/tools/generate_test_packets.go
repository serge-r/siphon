package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"time"
)

const (
	ipfixVersion       = 10
	templateSetID      = 2
	flowTemplateID     = 256
	observationDomain  = 1
	ipfixHeaderLen     = 16
	ipfixSetHeaderLen  = 4
	ipfixTemplateLen   = 4 + 7*4
	ipfixFlowRecordLen = 29
)

type config struct {
	collector string
	count     int
	interval  time.Duration
	srcIP     string
	dstIP     string
	srcPort   uint
	dstPort   uint
	protocol  uint
	packets   uint64
	bytes     uint64
}

func main() {
	cfg := parseFlags()

	srcIP := mustIPv4(cfg.srcIP)
	dstIP := mustIPv4(cfg.dstIP)

	conn, err := net.Dial("udp", cfg.collector)
	if err != nil {
		log.Fatalf("connect to collector %q: %v", cfg.collector, err)
	}
	defer conn.Close()

	for i := 0; i < cfg.count; i++ {
		packet := encodeIPFIX(flowRecord{
			srcIP:   srcIP,
			dstIP:   dstIP,
			srcPort: uint16(cfg.srcPort),
			dstPort: uint16(cfg.dstPort),
			proto:   uint8(cfg.protocol),
			packets: cfg.packets + uint64(i),
			bytes:   cfg.bytes + uint64(i)*128,
		}, uint32(i))

		if _, err := conn.Write(packet); err != nil {
			log.Fatalf("send packet %d: %v", i+1, err)
		}
		log.Printf("sent packet %d/%d to %s", i+1, cfg.count, cfg.collector)

		if i+1 < cfg.count && cfg.interval > 0 {
			time.Sleep(cfg.interval)
		}
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.collector, "collector", "127.0.0.1:4739", "collector UDP address")
	flag.IntVar(&cfg.count, "count", 1, "number of IPFIX packets to send")
	flag.DurationVar(&cfg.interval, "interval", time.Second, "delay between packets")
	flag.StringVar(&cfg.srcIP, "src-ip", "10.0.0.1", "source IPv4 address")
	flag.StringVar(&cfg.dstIP, "dst-ip", "8.8.8.8", "destination IPv4 address")
	flag.UintVar(&cfg.srcPort, "src-port", 12345, "source transport port")
	flag.UintVar(&cfg.dstPort, "dst-port", 443, "destination transport port")
	flag.UintVar(&cfg.protocol, "protocol", 6, "IP protocol number")
	flag.Uint64Var(&cfg.packets, "packets", 7, "initial packetDeltaCount")
	flag.Uint64Var(&cfg.bytes, "bytes", 4096, "initial octetDeltaCount")
	flag.Parse()

	if cfg.count < 1 {
		log.Fatal("count must be positive")
	}
	if cfg.srcPort > 65535 || cfg.dstPort > 65535 {
		log.Fatal("ports must be <= 65535")
	}
	if cfg.protocol > 255 {
		log.Fatal("protocol must be <= 255")
	}
	return cfg
}

type flowRecord struct {
	srcIP   [4]byte
	dstIP   [4]byte
	srcPort uint16
	dstPort uint16
	proto   uint8
	packets uint64
	bytes   uint64
}

func encodeIPFIX(record flowRecord, sequence uint32) []byte {
	templateLen := ipfixSetHeaderLen + ipfixTemplateLen
	dataLen := ipfixSetHeaderLen + ipfixFlowRecordLen
	totalLen := ipfixHeaderLen + templateLen + dataLen

	buf := bytes.NewBuffer(make([]byte, 0, totalLen))
	writeU16(buf, ipfixVersion)
	writeU16(buf, uint16(totalLen))
	writeU32(buf, uint32(time.Now().Unix()))
	writeU32(buf, sequence)
	writeU32(buf, observationDomain)

	writeTemplateSet(buf)
	writeDataSet(buf, record)
	return buf.Bytes()
}

func writeTemplateSet(buf *bytes.Buffer) {
	writeU16(buf, templateSetID)
	writeU16(buf, uint16(ipfixSetHeaderLen+ipfixTemplateLen))
	writeU16(buf, flowTemplateID)
	writeU16(buf, 7)

	writeField(buf, 8, 4)
	writeField(buf, 12, 4)
	writeField(buf, 7, 2)
	writeField(buf, 11, 2)
	writeField(buf, 4, 1)
	writeField(buf, 2, 8)
	writeField(buf, 1, 8)
}

func writeDataSet(buf *bytes.Buffer, record flowRecord) {
	writeU16(buf, flowTemplateID)
	writeU16(buf, uint16(ipfixSetHeaderLen+ipfixFlowRecordLen))
	buf.Write(record.srcIP[:])
	buf.Write(record.dstIP[:])
	writeU16(buf, record.srcPort)
	writeU16(buf, record.dstPort)
	buf.WriteByte(record.proto)
	writeU64(buf, record.packets)
	writeU64(buf, record.bytes)
}

func writeField(buf *bytes.Buffer, elementID uint16, length uint16) {
	writeU16(buf, elementID)
	writeU16(buf, length)
}

func writeU16(buf *bytes.Buffer, value uint16) {
	_ = binary.Write(buf, binary.BigEndian, value)
}

func writeU32(buf *bytes.Buffer, value uint32) {
	_ = binary.Write(buf, binary.BigEndian, value)
}

func writeU64(buf *bytes.Buffer, value uint64) {
	_ = binary.Write(buf, binary.BigEndian, value)
}

func mustIPv4(raw string) [4]byte {
	ip := net.ParseIP(raw).To4()
	if ip == nil {
		log.Fatalf("%q is not an IPv4 address", raw)
	}

	var out [4]byte
	copy(out[:], ip)
	return out
}

func init() {
	log.SetFlags(0)
	log.SetPrefix(fmt.Sprintf("%s: ", "generate_test_packets"))
}
