package collector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type Sink interface {
	Name() string
	Ready(ctx context.Context) []SinkReadiness
	WriteBatch(ctx context.Context, batch []EnrichedFlow) error
	Close() error
}

type SinkReadiness struct {
	Name  string
	Ready bool
	Error string
}

func BuildSink(cfg Config, logger *slog.Logger) (Sink, error) {
	var sinks []Sink
	for _, output := range cfg.Outputs {
		if !output.Enabled {
			continue
		}
		switch output.Type {
		case "stdout":
			logger.Info("initializing stdout output")
			sinks = append(sinks, NewStdoutSink())
		case "kafka":
			kafkaSink, err := NewKafkaSink(output, logger)
			if err != nil {
				return nil, err
			}
			sinks = append(sinks, kafkaSink)
		default:
			return nil, fmt.Errorf("unsupported sink %q", output.Type)
		}
	}
	if len(sinks) == 1 {
		return sinks[0], nil
	}
	return MultiSink(sinks), nil
}

type MultiSink []Sink

func (s MultiSink) Name() string { return "multi" }

func (s MultiSink) Ready(ctx context.Context) []SinkReadiness {
	readiness := make([]SinkReadiness, 0, len(s))
	for _, sink := range s {
		readiness = append(readiness, sink.Ready(ctx)...)
	}
	return readiness
}

func (s MultiSink) WriteBatch(ctx context.Context, batch []EnrichedFlow) error {
	for _, sink := range s {
		if err := sink.WriteBatch(ctx, batch); err != nil {
			return fmt.Errorf("%s: %w", sink.Name(), err)
		}
	}
	return nil
}

func (s MultiSink) Close() error {
	var closeErr error
	for _, sink := range s {
		if err := sink.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

type StdoutSink struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func NewStdoutSink() *StdoutSink {
	return &StdoutSink{
		encoder: json.NewEncoder(os.Stdout),
	}
}

func (s *StdoutSink) Name() string { return "stdout" }

func (s *StdoutSink) Ready(context.Context) []SinkReadiness {
	return []SinkReadiness{{Name: s.Name(), Ready: true}}
}

func (s *StdoutSink) WriteBatch(_ context.Context, batch []EnrichedFlow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.encoder.Encode(batch)
}

func (s *StdoutSink) Close() error { return nil }

type KafkaSink struct {
	client      *kgo.Client
	topic       string
	pingTimeout time.Duration
	logger      *slog.Logger
	ready       atomic.Bool
}

func NewKafkaSink(cfg OutputConfig, logger *slog.Logger) (*KafkaSink, error) {
	kafkaConfig := cfg.Config
	logger.Info(
		"initializing kafka producer",
		"brokers", kafkaConfig.Brokers,
		"topic", kafkaConfig.Topic,
		"sasl_plain_enabled", kafkaConfig.SASL.Enabled,
		"ssl_enabled", kafkaConfig.SSL.Enabled,
		"ssl_ca_file", kafkaConfig.SSL.CAFile,
		"ssl_ca_pem_configured", kafkaConfig.SSL.CAPEM != "",
		"compression", kafkaConfig.Compression,
		"linger", kafkaConfig.Linger,
		"batch_max_bytes", kafkaConfig.BatchMaxBytes,
		"max_buffered_records", kafkaConfig.MaxBufferedRecords,
		"max_buffered_bytes", kafkaConfig.MaxBufferedBytes,
		"produce_request_timeout", kafkaConfig.ProduceRequestTimeout,
		"ping_timeout", kafkaConfig.PingTimeout,
	)

	opts := []kgo.Opt{
		kgo.SeedBrokers(kafkaConfig.Brokers...),
		kgo.DefaultProduceTopic(kafkaConfig.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.WithHooks(kafkaDebugHook{logger: logger}),
	}
	if kafkaConfig.SASL.Enabled {
		if !kafkaConfig.SSL.Enabled {
			logger.Warn("kafka SASL/PLAIN is enabled without TLS; credentials will be sent in cleartext, enable outputs[].config.ssl to protect them")
		}
		opts = append(opts, kgo.SASL(plain.Auth{
			User: kafkaConfig.SASL.Username,
			Pass: kafkaConfig.SASL.Password,
		}.AsMechanism()))
	}
	if kafkaConfig.SSL.Enabled {
		tlsConfig, err := buildKafkaTLSConfig(kafkaConfig.SSL)
		if err != nil {
			logger.Error("build kafka tls config", "error", err)
			return nil, err
		}
		logger.Debug(
			"kafka tls enabled",
			"ca_file", kafkaConfig.SSL.CAFile,
			"ca_pem_configured", kafkaConfig.SSL.CAPEM != "",
		)
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if kafkaConfig.Compression != "" {
		compression, err := kafkaCompressionCodec(kafkaConfig.Compression)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.ProducerBatchCompression(compression))
	}
	if kafkaConfig.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(kafkaConfig.Linger))
	}
	if kafkaConfig.BatchMaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(kafkaConfig.BatchMaxBytes))
	}
	if kafkaConfig.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(kafkaConfig.MaxBufferedRecords))
	}
	if kafkaConfig.MaxBufferedBytes > 0 {
		opts = append(opts, kgo.MaxBufferedBytes(kafkaConfig.MaxBufferedBytes))
	}
	if kafkaConfig.ProduceRequestTimeout > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(kafkaConfig.ProduceRequestTimeout))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		logger.Error("initialize kafka producer", "error", err)
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	sink := &KafkaSink{
		client:      client,
		topic:       kafkaConfig.Topic,
		pingTimeout: kafkaConfig.PingTimeout,
		logger:      logger,
	}
	if err := sink.ping(context.Background()); err != nil {
		logger.Error("kafka initial connectivity check failed", "topic", kafkaConfig.Topic, "error", err)
	} else {
		logger.Info("kafka producer initialized and connected", "topic", kafkaConfig.Topic)
	}
	return sink, nil
}

func (s *KafkaSink) Name() string { return "kafka" }

func (s *KafkaSink) Ready(ctx context.Context) []SinkReadiness {
	if err := s.ping(ctx); err != nil {
		return []SinkReadiness{{Name: s.Name(), Ready: false, Error: err.Error()}}
	}
	return []SinkReadiness{{Name: s.Name(), Ready: true}}
}

func (s *KafkaSink) ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, s.pingTimeout)
	defer cancel()

	s.logger.Debug("checking kafka connectivity", "topic", s.topic, "timeout", s.pingTimeout)
	if err := s.client.Ping(pingCtx); err != nil {
		s.ready.Store(false)
		s.logger.Debug("kafka connectivity check failed", "topic", s.topic, "error", err)
		return err
	}
	s.ready.Store(true)
	s.logger.Debug("kafka connectivity check succeeded", "topic", s.topic)
	return nil
}

func (s *KafkaSink) WriteBatch(ctx context.Context, batch []EnrichedFlow) error {
	records := make([]*kgo.Record, 0, len(batch))
	for _, flow := range batch {
		value, err := json.Marshal(flow)
		if err != nil {
			s.logger.Error("marshal kafka record", "topic", s.topic, "error", err)
			return err
		}
		records = append(records, &kgo.Record{
			Topic: s.topic,
			Value: value,
		})
	}
	if err := s.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		s.logger.Error("write kafka batch", "topic", s.topic, "records", len(records), "error", err)
		return err
	}
	s.logger.Debug("wrote kafka batch", "topic", s.topic, "records", len(records))
	return nil
}

func (s *KafkaSink) Close() error {
	s.client.Close()
	return nil
}

func buildKafkaTLSConfig(cfg KafkaSSLConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if cfg.CAFile == "" && cfg.CAPEM == "" {
		return tlsConfig, nil
	}

	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if roots == nil {
		roots = x509.NewCertPool()
	}

	var caPEM []byte
	switch {
	case cfg.CAFile != "":
		caPEM, err = os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read kafka ssl ca_file %q: %w", cfg.CAFile, err)
		}
	case cfg.CAPEM != "":
		caPEM = []byte(cfg.CAPEM)
	}

	if len(caPEM) > 0 && !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse kafka ssl CA PEM")
	}
	tlsConfig.RootCAs = roots
	return tlsConfig, nil
}

func kafkaCompressionCodec(name string) (kgo.CompressionCodec, error) {
	switch name {
	case "none":
		return kgo.NoCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf("unsupported kafka compression %q", name)
	}
}

type kafkaDebugHook struct {
	logger *slog.Logger
}

func (h kafkaDebugHook) OnBrokerConnect(meta kgo.BrokerMetadata, initDur time.Duration, _ net.Conn, err error) {
	attrs := []any{
		"broker_id", meta.NodeID,
		"host", meta.Host,
		"port", meta.Port,
		"duration", initDur,
	}
	if err != nil {
		attrs = append(attrs, "error", err)
		h.logger.Debug("kafka broker connect failed", attrs...)
		return
	}
	h.logger.Debug("kafka broker connected", attrs...)
}
