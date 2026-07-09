package collector

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestDecodeIPFIXTrafficCapturePacket(t *testing.T) {
	packet := Packet{
		RemoteAddr: "127.0.0.1:4739",
		ReceivedAt: time.Unix(20, 0).UTC(),
		Payload:    testIPFIXPacket(t),
	}

	flows, err := DecodeIPFIX(packet)
	if err != nil {
		t.Fatalf("DecodeIPFIX() error = %v", err)
	}
	if len(flows) != 1 {
		t.Fatalf("DecodeIPFIX() decoded %d flows, want 1", len(flows))
	}

	flow := flows[0]
	if flow.SourceIP.String() != "10.0.0.1" {
		t.Fatalf("source IP = %s, want 10.0.0.1", flow.SourceIP)
	}
	if flow.DestinationIP.String() != "8.8.8.8" {
		t.Fatalf("destination IP = %s, want 8.8.8.8", flow.DestinationIP)
	}
	if flow.SourcePort != 12345 || flow.DestinationPort != 443 {
		t.Fatalf("ports = %d/%d, want 12345/443", flow.SourcePort, flow.DestinationPort)
	}
	if flow.Protocol != 6 {
		t.Fatalf("protocol = %d, want 6", flow.Protocol)
	}
	if flow.Packets != 7 || flow.Bytes != 4096 {
		t.Fatalf("counters = %d/%d, want 7/4096", flow.Packets, flow.Bytes)
	}
}

func testIPFIXPacket(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer
	writeU16(t, &buf, ipfixVersion)
	writeU16(t, &buf, 16+36+33)
	writeU32(t, &buf, 10)
	writeU32(t, &buf, 100)
	writeU32(t, &buf, 1)

	writeU16(t, &buf, ipfixTemplateSetID)
	writeU16(t, &buf, 36)
	writeU16(t, &buf, flowTemplateID)
	writeU16(t, &buf, 7)
	for _, field := range [][2]uint16{
		{8, 4},
		{12, 4},
		{7, 2},
		{11, 2},
		{4, 1},
		{2, 8},
		{1, 8},
	} {
		writeU16(t, &buf, field[0])
		writeU16(t, &buf, field[1])
	}

	writeU16(t, &buf, flowTemplateID)
	writeU16(t, &buf, 33)
	buf.Write([]byte{10, 0, 0, 1})
	buf.Write([]byte{8, 8, 8, 8})
	writeU16(t, &buf, 12345)
	writeU16(t, &buf, 443)
	buf.WriteByte(6)
	writeU64(t, &buf, 7)
	writeU64(t, &buf, 4096)

	return buf.Bytes()
}

func writeU16(t *testing.T, buf *bytes.Buffer, value uint16) {
	t.Helper()
	if err := binary.Write(buf, binary.BigEndian, value); err != nil {
		t.Fatal(err)
	}
}

func writeU32(t *testing.T, buf *bytes.Buffer, value uint32) {
	t.Helper()
	if err := binary.Write(buf, binary.BigEndian, value); err != nil {
		t.Fatal(err)
	}
}

func writeU64(t *testing.T, buf *bytes.Buffer, value uint64) {
	t.Helper()
	if err := binary.Write(buf, binary.BigEndian, value); err != nil {
		t.Fatal(err)
	}
}
