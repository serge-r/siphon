package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewLogger(cfg LoggingConfig) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(cfg.Format, "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func Run(ctx context.Context, cfg Config, enrichers []Enricher, logger *slog.Logger) error {
	if cfg.Global.Listener.Protocol != "udp" {
		return fmt.Errorf("listener protocol %q is not supported yet; use udp", cfg.Global.Listener.Protocol)
	}
	defer closeEnrichers(enrichers)

	metrics := NewMetrics()
	sink, err := BuildSink(cfg, logger)
	if err != nil {
		return err
	}
	defer sink.Close()

	httpServer := startMetricsServer(cfg.Global.Listener.Metrics, BuildJSONSchema(enrichers), enrichers, sink, logger)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	packetQueue := make(chan Packet, cfg.Global.Pipeline.PacketQueueSize)
	flowQueue := make(chan Flow, cfg.Global.Pipeline.FlowQueueSize)
	enrichedQueue := make(chan EnrichedFlow, cfg.Global.Pipeline.FlowQueueSize)
	batchQueue := make(chan []EnrichedFlow, 1)

	metrics.ObserveQueue("packets", packetQueue)
	metrics.ObserveQueue("flows", flowQueue)
	metrics.ObserveQueue("enriched", enrichedQueue)
	metrics.ObserveQueue("batches", batchQueue)

	workers := cfg.Global.Pipeline.Workers
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ListenUDP(ctx, cfg.Global.Listener, packetQueue, metrics, logger)
		close(packetQueue)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		DecodePackets(ctx, packetQueue, flowQueue, metrics, logger)
		close(flowQueue)
	}()

	var enrichWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		enrichWG.Add(1)
		go func(workerID int) {
			defer enrichWG.Done()
			EnrichFlows(ctx, workerID, flowQueue, enrichedQueue, enrichers, metrics, logger)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		enrichWG.Wait()
		close(enrichedQueue)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		BatchFlows(ctx, cfg.Global.Pipeline.Batch, enrichedQueue, batchQueue)
		close(batchQueue)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		WriteBatches(ctx, sink.Name(), batchQueue, sink, metrics, logger)
	}()

	wg.Wait()
	return ctx.Err()
}

func closeEnrichers(enrichers []Enricher) {
	for _, enricher := range enrichers {
		closer, ok := enricher.(interface{ Close() })
		if ok {
			closer.Close()
		}
	}
}

func startMetricsServer(address string, schema JSONSchema, enrichers []Enricher, sink Sink, logger *slog.Logger) *http.Server {
	if address == "" {
		address = ":9090"
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		status := buildReadinessStatus(r.Context(), enrichers, sink)
		w.Header().Set("Content-Type", "application/json")
		if !status.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		if err := json.NewEncoder(w).Encode(status); err != nil {
			logger.Warn("write readiness response", "error", err)
		}
	})
	mux.HandleFunc("/schema", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/schema+json")
		if err := json.NewEncoder(w).Encode(schema); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("starting metrics server", "address", address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped", "error", err)
		}
	}()

	return server
}

type readinessStatus struct {
	Ready     bool                      `json:"ready"`
	Enrichers []enricherReadinessStatus `json:"enrichers"`
	Outputs   []outputReadinessStatus   `json:"outputs"`
}

type enricherReadinessStatus struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	Error string `json:"error,omitempty"`
}

type outputReadinessStatus struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	Error string `json:"error,omitempty"`
}

func buildReadinessStatus(ctx context.Context, enrichers []Enricher, sink Sink) readinessStatus {
	status := readinessStatus{
		Ready:     true,
		Enrichers: make([]enricherReadinessStatus, 0, len(enrichers)),
		Outputs:   make([]outputReadinessStatus, 0, 1),
	}

	for _, enricher := range enrichers {
		enricherStatus := enricherReadinessStatus{
			Name:  enricher.Name(),
			Ready: true,
		}
		if err := enricher.Ready(); err != nil {
			enricherStatus.Ready = false
			enricherStatus.Error = err.Error()
			status.Ready = false
		}
		status.Enrichers = append(status.Enrichers, enricherStatus)
	}
	for _, sinkStatus := range sink.Ready(ctx) {
		outputStatus := outputReadinessStatus{
			Name:  sinkStatus.Name,
			Ready: sinkStatus.Ready,
			Error: sinkStatus.Error,
		}
		if !sinkStatus.Ready {
			status.Ready = false
		}
		status.Outputs = append(status.Outputs, outputStatus)
	}

	return status
}

func ListenUDP(ctx context.Context, cfg ListenerConfig, out chan<- Packet, metrics *Metrics, logger *slog.Logger) {
	addr, err := net.ResolveUDPAddr("udp", cfg.Address)
	if err != nil {
		logger.Error("resolve UDP listener address", "address", cfg.Address, "error", err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		logger.Error("listen UDP", "address", cfg.Address, "error", err)
		return
	}
	defer conn.Close()

	if cfg.ReadBufferBytes > 0 {
		if err := conn.SetReadBuffer(cfg.ReadBufferBytes); err != nil {
			logger.Warn("set UDP read buffer", "bytes", cfg.ReadBufferBytes, "error", err)
		}
	}

	logger.Info("listening for IPFIX packets", "protocol", "udp", "address", cfg.Address)
	buf := make([]byte, 65535)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			logger.Warn("read UDP packet", "error", err)
			// Back off briefly so a persistently failing socket does not spin
			// the loop and flood logs.
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}

		packet := Packet{
			RemoteAddr: remote.String(),
			ReceivedAt: time.Now().UTC(),
			Payload:    append([]byte(nil), buf[:n]...),
		}
		metrics.PacketsReceived.Inc()
		select {
		case out <- packet:
		default:
			metrics.PacketDrops.WithLabelValues("listener").Inc()
		}
	}
}

func DecodePackets(ctx context.Context, in <-chan Packet, out chan<- Flow, metrics *Metrics, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case packet, ok := <-in:
			if !ok {
				return
			}
			flows, err := DecodeIPFIX(packet)
			if err != nil {
				metrics.PacketDrops.WithLabelValues("decoder").Inc()
				logger.Debug("decode IPFIX packet", "remote_addr", packet.RemoteAddr, "error", err)
				continue
			}
			for _, flow := range flows {
				metrics.FlowsDecoded.Inc()
				select {
				case out <- flow:
				default:
					metrics.PacketDrops.WithLabelValues("flow_queue").Inc()
				}
			}
		}
	}
}

func EnrichFlows(ctx context.Context, workerID int, in <-chan Flow, out chan<- EnrichedFlow, enrichers []Enricher, metrics *Metrics, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case flow, ok := <-in:
			if !ok {
				return
			}
			enriched := NewEnrichedFlow(flow)
			for _, enricher := range enrichers {
				if err := enricher.Enrich(ctx, &enriched); err != nil {
					logger.Debug("enrich flow", "worker", workerID, "enricher", enricher.Name(), "error", err)
				}
			}
			metrics.FlowsProcessed.Inc()
			metrics.CountHost(enriched.SourceName)
			metrics.CountHost(enriched.DestinationName)
			select {
			case out <- enriched:
			default:
				metrics.PacketDrops.WithLabelValues("enriched_queue").Inc()
			}
		}
	}
}

func BatchFlows(ctx context.Context, cfg BatchConfig, in <-chan EnrichedFlow, out chan<- []EnrichedFlow) {
	maxRecords := cfg.MaxRecords
	if maxRecords <= 0 {
		maxRecords = 1000
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]EnrichedFlow, 0, maxRecords)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		next := make([]EnrichedFlow, len(batch))
		copy(next, batch)
		batch = batch[:0]
		select {
		case out <- next:
			return true
		case <-ctx.Done():
			return false
		}
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush()
			return
		case flow, ok := <-in:
			if !ok {
				_ = flush()
				return
			}
			batch = append(batch, flow)
			if len(batch) >= maxRecords && !flush() {
				return
			}
		case <-ticker.C:
			if !flush() {
				return
			}
		}
	}
}

func WriteBatches(ctx context.Context, sinkName string, in <-chan []EnrichedFlow, sink Sink, metrics *Metrics, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-in:
			if !ok {
				return
			}
			if err := sink.WriteBatch(ctx, batch); err != nil {
				metrics.SinkErrors.WithLabelValues(sinkName).Inc()
				logger.Error("write batch", "sink", sinkName, "records", len(batch), "error", err)
				continue
			}
			metrics.BatchesSent.Inc()
		}
	}
}
