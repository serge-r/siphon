package collector

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"time"
)

const (
	ipfixVersion       = 10
	ipfixHeaderLen     = 16
	ipfixSetHeaderLen  = 4
	ipfixTemplateSetID = 2
	flowTemplateID     = 256
	ipfixFlowRecordLen = 29
)

func DecodeIPFIX(packet Packet) ([]Flow, error) {
	if len(packet.Payload) < ipfixHeaderLen {
		return nil, fmt.Errorf("packet too short: %d bytes", len(packet.Payload))
	}
	if binary.BigEndian.Uint16(packet.Payload[0:2]) != ipfixVersion {
		return nil, fmt.Errorf("unsupported IPFIX version")
	}

	totalLen := int(binary.BigEndian.Uint16(packet.Payload[2:4]))
	if totalLen > len(packet.Payload) {
		return nil, fmt.Errorf("invalid IPFIX length: header=%d payload=%d", totalLen, len(packet.Payload))
	}
	payload := packet.Payload[:totalLen]

	exportTime := time.Unix(int64(binary.BigEndian.Uint32(payload[4:8])), 0).UTC()
	sequence := binary.BigEndian.Uint32(payload[8:12])
	observationDomain := binary.BigEndian.Uint32(payload[12:16])

	var flows []Flow
	for offset := ipfixHeaderLen; offset < len(payload); {
		if offset+ipfixSetHeaderLen > len(payload) {
			return nil, fmt.Errorf("truncated set header at offset %d", offset)
		}

		setID := binary.BigEndian.Uint16(payload[offset : offset+2])
		setLen := int(binary.BigEndian.Uint16(payload[offset+2 : offset+4]))
		if setLen < ipfixSetHeaderLen {
			return nil, fmt.Errorf("invalid set length %d at offset %d", setLen, offset)
		}
		if offset+setLen > len(payload) {
			return nil, fmt.Errorf("set length %d exceeds packet at offset %d", setLen, offset)
		}

		setPayload := payload[offset+ipfixSetHeaderLen : offset+setLen]
		switch setID {
		case ipfixTemplateSetID:
			if err := validateTemplateSet(setPayload); err != nil {
				return nil, err
			}
		case flowTemplateID:
			records, err := decodeFlowSet(setPayload, packet, exportTime, sequence, observationDomain)
			if err != nil {
				return nil, err
			}
			flows = append(flows, records...)
		default:
			return nil, fmt.Errorf("unsupported IPFIX set id %d", setID)
		}
		offset += setLen
	}

	return flows, nil
}

func validateTemplateSet(payload []byte) error {
	if len(payload) < 4 {
		return fmt.Errorf("truncated template set")
	}
	templateID := binary.BigEndian.Uint16(payload[0:2])
	fieldCount := binary.BigEndian.Uint16(payload[2:4])
	if templateID != flowTemplateID {
		return fmt.Errorf("unsupported template id %d", templateID)
	}
	if fieldCount != 7 {
		return fmt.Errorf("unsupported template field count %d", fieldCount)
	}
	return nil
}

func decodeFlowSet(payload []byte, packet Packet, exportTime time.Time, sequence uint32, observationDomain uint32) ([]Flow, error) {
	if len(payload)%ipfixFlowRecordLen != 0 {
		return nil, fmt.Errorf("flow set payload length %d is not divisible by record length %d", len(payload), ipfixFlowRecordLen)
	}

	flows := make([]Flow, 0, len(payload)/ipfixFlowRecordLen)
	for offset := 0; offset < len(payload); offset += ipfixFlowRecordLen {
		record := payload[offset : offset+ipfixFlowRecordLen]
		srcIP := netip.AddrFrom4([4]byte(record[0:4]))
		dstIP := netip.AddrFrom4([4]byte(record[4:8]))
		flows = append(flows, Flow{
			Sequence:          sequence + uint32(len(flows)),
			ObservationDomain: observationDomain,
			ExportTime:        exportTime,
			ReceivedAt:        packet.ReceivedAt,
			Exporter:          packet.RemoteAddr,
			SourceIP:          srcIP,
			DestinationIP:     dstIP,
			SourcePort:        binary.BigEndian.Uint16(record[8:10]),
			DestinationPort:   binary.BigEndian.Uint16(record[10:12]),
			Protocol:          record[12],
			Packets:           binary.BigEndian.Uint64(record[13:21]),
			Bytes:             binary.BigEndian.Uint64(record[21:29]),
		})
	}
	return flows, nil
}
