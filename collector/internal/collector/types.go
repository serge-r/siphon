package collector

import (
	"net/netip"
	"time"
)

type Packet struct {
	RemoteAddr string
	ReceivedAt time.Time
	Payload    []byte
}

type Flow struct {
	Sequence          uint32    `json:"sequence"`
	ObservationDomain uint32    `json:"observation_domain"`
	ExportTime        time.Time `json:"export_time"`
	ReceivedAt        time.Time `json:"received_at"`
	Exporter          string    `json:"exporter"`
	SourceIP          netip.Addr
	DestinationIP     netip.Addr
	SourcePort        uint16 `json:"source_port"`
	DestinationPort   uint16 `json:"destination_port"`
	Protocol          uint8  `json:"protocol"`
	Packets           uint64 `json:"packets"`
	Bytes             uint64 `json:"bytes"`
}

type EnrichedFlow struct {
	Sequence          uint32    `json:"sequence"`
	ObservationDomain uint32    `json:"observation_domain"`
	ExportTime        time.Time `json:"export_time"`
	ReceivedAt        time.Time `json:"received_at"`
	Exporter          string    `json:"exporter"`

	SourceIP            string `json:"source_ip"`
	SourceName          string `json:"source_name,omitempty"`
	SourceStaticName    string `json:"source_static_name,omitempty"`
	SourceInventoryName string `json:"source_inventory_name,omitempty"`
	SourceNamespace     string `json:"source_namespace,omitempty"`
	SourcePodName       string `json:"source_pod_name,omitempty"`
	SourceAWSService    string `json:"source_aws_service,omitempty"`
	SourceAWSRegion     string `json:"source_aws_region,omitempty"`
	SourceDNSName       string `json:"source_dns_name,omitempty"`
	SourceGeoIPISP      string `json:"source_geoip_isp,omitempty"`
	SourceGeoIPASN      uint   `json:"source_geoip_asn,omitempty"`
	SourceGeoIPCountry  string `json:"source_geoip_country,omitempty"`
	SourceGeoIPCity     string `json:"source_geoip_city,omitempty"`
	SourcePort          uint16 `json:"source_port"`

	DestinationIP            string `json:"destination_ip"`
	DestinationName          string `json:"destination_name,omitempty"`
	DestinationStaticName    string `json:"destination_static_name,omitempty"`
	DestinationInventoryName string `json:"destination_inventory_name,omitempty"`
	DestinationNamespace     string `json:"destination_namespace,omitempty"`
	DestinationPodName       string `json:"destination_pod_name,omitempty"`
	DestinationAWSService    string `json:"destination_aws_service,omitempty"`
	DestinationAWSRegion     string `json:"destination_aws_region,omitempty"`
	DestinationDNSName       string `json:"destination_dns_name,omitempty"`
	DestinationGeoIPISP      string `json:"destination_geoip_isp,omitempty"`
	DestinationGeoIPASN      uint   `json:"destination_geoip_asn,omitempty"`
	DestinationGeoIPCountry  string `json:"destination_geoip_country,omitempty"`
	DestinationGeoIPCity     string `json:"destination_geoip_city,omitempty"`
	DestinationPort          uint16 `json:"destination_port"`

	Protocol uint8  `json:"protocol"`
	Packets  uint64 `json:"packets"`
	Bytes    uint64 `json:"bytes"`
}

func NewEnrichedFlow(flow Flow) EnrichedFlow {
	return EnrichedFlow{
		Sequence:          flow.Sequence,
		ObservationDomain: flow.ObservationDomain,
		ExportTime:        flow.ExportTime,
		ReceivedAt:        flow.ReceivedAt,
		Exporter:          flow.Exporter,
		SourceIP:          flow.SourceIP.String(),
		SourcePort:        flow.SourcePort,
		DestinationIP:     flow.DestinationIP.String(),
		DestinationPort:   flow.DestinationPort,
		Protocol:          flow.Protocol,
		Packets:           flow.Packets,
		Bytes:             flow.Bytes,
	}
}
