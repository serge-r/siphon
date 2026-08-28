package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
)

// InventoryEnricher maps flow endpoint IPs to human-readable server names by
// periodically fetching a server inventory from an HTTP JSON endpoint.
//
// The endpoint is expected to return a JSON document shaped like:
//
//	{
//	  "data": [
//	    {
//	      "name": "web-01.example.com",
//	      "metadata": {"provider": "acme", "id": "abc-123", "always_zero_cost": false},
//	      "cost": 100,
//	      "cost_currency": "USD",
//	      "status": {"operational_status": true, "power_status": true, "success": true},
//	      "location": "dc-1",
//	      "hardware": "",
//	      "server_ip": [
//	        {"interface": "eth0", "ip": "192.0.2.10", "is_public": true},
//	        {"interface": "eth1", "ip": "10.0.0.10", "is_public": false}
//	      ],
//	      "containers": [
//	        {
//	          "container_name": "app",
//	          "container_ip": [
//	            {"interface": "eth0", "ip": "10.99.0.10", "is_public": false}
//	          ]
//	        }
//	      ]
//	    }
//	  ]
//	}
//
// Only the top-level "name" and "server_ip[].ip" fields are used to build the
// "ip -> name" lookup. All other fields (cost, status, location, hardware,
// metadata) and the "containers" list are accepted but ignored; they exist so
// that a richer inventory source can be consumed without changing the response
// schema. Any field may be omitted.
type InventoryEnricher struct {
	cfg       InventoryEnrichmentConfig
	logger    *slog.Logger
	client    *http.Client
	mu        sync.RWMutex
	namesByIP map[string]string
	ready     atomic.Bool
	lastErr   atomic.Value
	stopOnce  sync.Once
	stopCh    chan struct{}
}

type inventoryResponse struct {
	Data []inventoryServer `json:"data"`
}

type inventoryServer struct {
	Name       string               `json:"name"`
	Metadata   inventoryMetadata    `json:"metadata"`
	Cost       float64              `json:"cost"`
	Currency   string               `json:"cost_currency"`
	Status     inventoryStatus      `json:"status"`
	Location   string               `json:"location"`
	Hardware   string               `json:"hardware"`
	ServerIP   []inventoryIP        `json:"server_ip"`
	Containers []inventoryContainer `json:"containers"`
}

type inventoryMetadata struct {
	Provider       string `json:"provider"`
	ID             string `json:"id"`
	AlwaysZeroCost bool   `json:"always_zero_cost"`
}

type inventoryStatus struct {
	OperationalStatus bool `json:"operational_status"`
	PowerStatus       bool `json:"power_status"`
	Success           bool `json:"success"`
}

type inventoryContainer struct {
	ContainerName string        `json:"container_name"`
	ContainerIP   []inventoryIP `json:"container_ip"`
}

type inventoryIP struct {
	Interface string `json:"interface"`
	IP        string `json:"ip"`
	IsPublic  bool   `json:"is_public"`
}

func NewInventoryEnricher(cfg InventoryEnrichmentConfig, logger *slog.Logger) *InventoryEnricher {
	enricher := &InventoryEnricher{
		cfg:       cfg,
		logger:    logger,
		client:    &http.Client{Timeout: cfg.Timeout},
		namesByIP: map[string]string{},
		stopCh:    make(chan struct{}),
	}

	logger.Info(
		"initializing inventory enricher",
		"endpoint_url", cfg.EndpointURL,
		"cache_ttl", cfg.CacheTTL,
		"timeout", cfg.Timeout,
	)
	go enricher.refreshLoop()
	return enricher
}

func (e *InventoryEnricher) Name() string { return "inventory" }

func (e *InventoryEnricher) Ready() error {
	if e.ready.Load() {
		return nil
	}

	// A failed refresh leaves the map built by the last successful one in place,
	// so Enrich keeps resolving IPs — the data only gets staler, which with an
	// hour-long cache TTL is a normal operating condition anyway. Reporting that
	// as not-ready drops the pod from its Service EndpointSlice, and behind a UDP
	// load balancer that turns degraded enrichment into a total loss of
	// ingestion. Only an enricher that never loaded anything is unready.
	if e.cached() > 0 {
		return nil
	}
	if err := loadErr(&e.lastErr); err != nil {
		return err
	}
	return fmt.Errorf("inventory data is not loaded")
}

func (e *InventoryEnricher) Fields() []SchemaField {
	return []SchemaField{
		{Name: "source_inventory_name", Type: "string"},
		{Name: "destination_inventory_name", Type: "string"},
	}
}

func (e *InventoryEnricher) Enrich(_ context.Context, flow *EnrichedFlow) error {
	if name := e.lookup(flow.SourceIP); name != "" {
		flow.SourceInventoryName = name
		setSourceName(flow, name)
	}
	if name := e.lookup(flow.DestinationIP); name != "" {
		flow.DestinationInventoryName = name
		setDestinationName(flow, name)
	}
	return nil
}

func (e *InventoryEnricher) Close() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
	})
}

func (e *InventoryEnricher) refreshLoop() {
	runRefreshLoop(e.stopCh, e.cfg.CacheTTL, e.logger, e.Name(), e.refresh)
}

func (e *InventoryEnricher) refresh() error {
	namesByIP, err := e.fetch()
	if err != nil {
		e.ready.Store(false)
		e.lastErr.Store(errHolder{err: err})
		e.logger.Error("refresh inventory data", "error", err)
		return err
	}

	e.mu.Lock()
	e.namesByIP = namesByIP
	e.mu.Unlock()
	e.ready.Store(true)
	e.lastErr.Store(errHolder{})
	e.logger.Info("inventory data loaded", "ips", len(namesByIP))
	return nil
}

func (e *InventoryEnricher) fetch() (map[string]string, error) {
	req, err := http.NewRequest(http.MethodGet, e.cfg.EndpointURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create inventory request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch inventory data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetch inventory data: unexpected status %s", resp.Status)
	}

	var parsed inventoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode inventory data: %w", err)
	}
	return buildInventoryIPNameMap(parsed.Data), nil
}

func (e *InventoryEnricher) lookup(ip string) string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.namesByIP[ip]
}

// cached is the number of IPs the enricher can currently resolve.
func (e *InventoryEnricher) cached() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.namesByIP)
}

func buildInventoryIPNameMap(servers []inventoryServer) map[string]string {
	namesByIP := make(map[string]string)
	for _, server := range servers {
		if server.Name == "" {
			continue
		}
		for _, ip := range server.ServerIP {
			if ip.IP == "" {
				continue
			}
			namesByIP[ip.IP] = server.Name
		}
	}
	return namesByIP
}
