package collector

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"
)

func TestBuildInventoryIPNameMap(t *testing.T) {
	var response inventoryResponse
	if err := json.Unmarshal([]byte(testInventoryJSON), &response); err != nil {
		t.Fatal(err)
	}

	namesByIP := buildInventoryIPNameMap(response.Data)
	tests := map[string]string{
		"198.51.100.20": "ns-1.example.com",
		"10.214.11.246": "ns-1.example.com",
		"203.0.113.108": "web-59.example.com",
	}

	for ip, want := range tests {
		if got := namesByIP[ip]; got != want {
			t.Fatalf("namesByIP[%q] = %q, want %q", ip, got, want)
		}
	}
	if got := namesByIP["10.99.0.10"]; got != "" {
		t.Fatalf("container IP was mapped to %q, want empty", got)
	}
}

func TestInventoryEnricherEnrichesDestinationIP(t *testing.T) {
	enricher := &InventoryEnricher{
		namesByIP: map[string]string{
			"203.0.113.108": "web-59.example.com",
		},
	}

	flow := NewEnrichedFlow(Flow{
		Sequence:          1,
		ObservationDomain: 1,
		ExportTime:        time.Unix(1, 0).UTC(),
		ReceivedAt:        time.Unix(2, 0).UTC(),
		Exporter:          "127.0.0.1:4739",
		SourceIP:          netip.MustParseAddr("10.0.0.1"),
		DestinationIP:     netip.MustParseAddr("203.0.113.108"),
		SourcePort:        12345,
		DestinationPort:   443,
		Protocol:          6,
		Packets:           10,
		Bytes:             4096,
	})

	if err := enricher.Enrich(t.Context(), &flow); err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if flow.DestinationInventoryName != "web-59.example.com" {
		t.Fatalf("DestinationInventoryName = %q, want web-59.example.com", flow.DestinationInventoryName)
	}
	if flow.DestinationName != "web-59.example.com" {
		t.Fatalf("DestinationName = %q, want web-59.example.com", flow.DestinationName)
	}
}

const testInventoryJSON = `{
  "data": [
    {
      "name": "ns-1.example.com",
      "metadata": {"provider": "unknown", "id": "", "always_zero_cost": false},
      "cost": 0,
      "cost_currency": "",
      "status": {"operational_status": false, "power_status": false, "success": false},
      "location": "",
      "hardware": "",
      "server_ip": [
        {"interface": "ens4", "ip": "198.51.100.20", "is_public": true},
        {"interface": "ens5", "ip": "10.214.11.246", "is_public": false}
      ],
      "containers": [
        {
          "container_name": "ignored",
          "container_ip": [
            {"interface": "eth0", "ip": "10.99.0.10", "is_public": false}
          ]
        }
      ]
    },
    {
      "name": "web-59.example.com",
      "metadata": {"provider": "acme", "id": "5e846905-e4bf-4688-8024-33d406661912", "always_zero_cost": false},
      "cost": 100,
      "cost_currency": "USD",
      "status": {"operational_status": true, "power_status": true, "success": true},
      "location": "dc-1",
      "hardware": "",
      "server_ip": [
        {"interface": "ethernet", "ip": "203.0.113.108", "is_public": true}
      ],
      "containers": null
    }
  ]
}`
