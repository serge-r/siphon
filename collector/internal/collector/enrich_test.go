package collector

import (
	"context"
	"net/netip"
	"testing"
	"time"
)

func TestKubernetesEnricherEnrichesDestinationIP(t *testing.T) {
	internalNetwork := netip.MustParsePrefix("10.0.0.0/8")
	enricher := &KubernetesEnricher{
		internalNetworks: []netip.Prefix{internalNetwork},
		podsByIP: map[string]podRef{
			"10.10.0.25": {
				namespace: "payments",
				name:      "api-123",
			},
		},
	}

	flow := NewEnrichedFlow(Flow{
		Sequence:          1,
		ObservationDomain: 1,
		ExportTime:        time.Unix(1, 0).UTC(),
		ReceivedAt:        time.Unix(2, 0).UTC(),
		Exporter:          "127.0.0.1:4739",
		SourceIP:          netip.MustParseAddr("8.8.8.8"),
		DestinationIP:     netip.MustParseAddr("10.10.0.25"),
		SourcePort:        12345,
		DestinationPort:   443,
		Protocol:          6,
		Packets:           10,
		Bytes:             4096,
	})

	if err := enricher.Enrich(context.Background(), &flow); err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}

	if flow.SourcePodName != "" {
		t.Fatalf("source pod = %q, want empty for external source IP", flow.SourcePodName)
	}
	if flow.DestinationNamespace != "payments" {
		t.Fatalf("destination namespace = %q, want payments", flow.DestinationNamespace)
	}
	if flow.DestinationPodName != "api-123" {
		t.Fatalf("destination pod = %q, want api-123", flow.DestinationPodName)
	}
	if flow.DestinationName != "payments/api-123" {
		t.Fatalf("destination name = %q, want payments/api-123", flow.DestinationName)
	}
}
