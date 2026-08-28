package collector

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
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

func TestInventoryEnricherStaysReadyOnFailedRefresh(t *testing.T) {
	var failing atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, testInventoryJSON)
	}))
	defer server.Close()

	enricher := &InventoryEnricher{
		cfg: InventoryEnrichmentConfig{
			EndpointURL: server.URL,
			CacheTTL:    time.Hour,
			Timeout:     5 * time.Second,
		},
		logger:    discardLogger(),
		client:    server.Client(),
		namesByIP: map[string]string{},
		stopCh:    make(chan struct{}),
	}

	if err := enricher.refresh(); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}

	failing.Store(true)
	if err := enricher.refresh(); err == nil {
		t.Fatal("refresh() error is nil, want the upstream 502")
	}

	if err := enricher.Ready(); err != nil {
		t.Fatalf("Ready() = %v, want nil while the cache is still populated", err)
	}
	if got := enricher.lookup("203.0.113.108"); got != "web-59.example.com" {
		t.Fatalf("lookup() = %q, want web-59.example.com", got)
	}
}

func TestInventoryEnricherNotReadyWithoutData(t *testing.T) {
	enricher := &InventoryEnricher{namesByIP: map[string]string{}}
	enricher.lastErr.Store(errHolder{err: errors.New("fetch inventory data: unexpected status 502 Bad Gateway")})

	if err := enricher.Ready(); err == nil {
		t.Fatal("Ready() is nil, want an error when no data was ever loaded")
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
