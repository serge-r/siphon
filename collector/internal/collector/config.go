package collector

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Global     GlobalConfig     `yaml:"global"`
	Enrichment EnrichmentConfig `yaml:"enrichment"`
	Outputs    []OutputConfig   `yaml:"outputs"`
}

type GlobalConfig struct {
	Listener ListenerConfig `yaml:"listener"`
	Logging  LoggingConfig  `yaml:"logging"`
	Pipeline PipelineConfig `yaml:"pipeline"`
}

type ListenerConfig struct {
	Protocol        string `yaml:"protocol"`
	Address         string `yaml:"address"`
	ReadBufferBytes int    `yaml:"read_buffer_bytes"`
	Metrics         string `yaml:"metrics"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type PipelineConfig struct {
	PacketQueueSize int         `yaml:"packet_queue_size"`
	FlowQueueSize   int         `yaml:"flow_queue_size"`
	Workers         int         `yaml:"workers"`
	Batch           BatchConfig `yaml:"batch"`
}

type BatchConfig struct {
	MaxRecords    int           `yaml:"max_records"`
	FlushInterval time.Duration `yaml:"flush_interval"`
}

func (b *BatchConfig) UnmarshalYAML(value *yaml.Node) error {
	type rawBatchConfig struct {
		MaxRecords    int    `yaml:"max_records"`
		FlushInterval string `yaml:"flush_interval"`
	}

	var raw rawBatchConfig
	if err := value.Decode(&raw); err != nil {
		return err
	}

	b.MaxRecords = raw.MaxRecords
	if raw.FlushInterval != "" {
		d, err := time.ParseDuration(raw.FlushInterval)
		if err != nil {
			return fmt.Errorf("parse global.pipeline.batch.flush_interval: %w", err)
		}
		b.FlushInterval = d
	}
	return nil
}

type OutputConfig struct {
	Type    string             `yaml:"type"`
	Enabled bool               `yaml:"enabled"`
	Config  OutputConfigValues `yaml:"config"`
}

type OutputConfigValues struct {
	Brokers               []string       `yaml:"brokers"`
	Topic                 string         `yaml:"topic"`
	SASL                  KafkaSASLPlain `yaml:"sasl"`
	SSL                   KafkaSSLConfig `yaml:"ssl"`
	Compression           string         `yaml:"compression"`
	Linger                time.Duration  `yaml:"linger"`
	BatchMaxBytes         int32          `yaml:"batch_max_bytes"`
	MaxBufferedRecords    int            `yaml:"max_buffered_records"`
	MaxBufferedBytes      int            `yaml:"max_buffered_bytes"`
	ProduceRequestTimeout time.Duration  `yaml:"produce_request_timeout"`
	PingTimeout           time.Duration  `yaml:"ping_timeout"`
}

func (o *OutputConfigValues) UnmarshalYAML(value *yaml.Node) error {
	type rawOutputConfigValues struct {
		Brokers               []string       `yaml:"brokers"`
		Topic                 string         `yaml:"topic"`
		SASL                  KafkaSASLPlain `yaml:"sasl"`
		SSL                   KafkaSSLConfig `yaml:"ssl"`
		Compression           string         `yaml:"compression"`
		Linger                string         `yaml:"linger"`
		BatchMaxBytes         int32          `yaml:"batch_max_bytes"`
		MaxBufferedRecords    int            `yaml:"max_buffered_records"`
		MaxBufferedBytes      int            `yaml:"max_buffered_bytes"`
		ProduceRequestTimeout string         `yaml:"produce_request_timeout"`
		PingTimeout           string         `yaml:"ping_timeout"`
	}

	if value.Kind == 0 || value.Tag == "!!null" {
		return nil
	}

	var raw rawOutputConfigValues
	if err := value.Decode(&raw); err != nil {
		return err
	}

	o.Brokers = raw.Brokers
	o.Topic = raw.Topic
	o.SASL = raw.SASL
	o.SSL = raw.SSL
	o.Compression = raw.Compression
	o.BatchMaxBytes = raw.BatchMaxBytes
	o.MaxBufferedRecords = raw.MaxBufferedRecords
	o.MaxBufferedBytes = raw.MaxBufferedBytes

	if raw.Linger != "" {
		linger, err := time.ParseDuration(raw.Linger)
		if err != nil {
			return fmt.Errorf("parse outputs[].config.linger: %w", err)
		}
		o.Linger = linger
	}
	if raw.ProduceRequestTimeout != "" {
		timeout, err := time.ParseDuration(raw.ProduceRequestTimeout)
		if err != nil {
			return fmt.Errorf("parse outputs[].config.produce_request_timeout: %w", err)
		}
		o.ProduceRequestTimeout = timeout
	}
	if raw.PingTimeout != "" {
		timeout, err := time.ParseDuration(raw.PingTimeout)
		if err != nil {
			return fmt.Errorf("parse outputs[].config.ping_timeout: %w", err)
		}
		o.PingTimeout = timeout
	}
	return nil
}

type KafkaSASLPlain struct {
	Enabled  bool   `yaml:"enabled"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type KafkaSSLConfig struct {
	Enabled bool   `yaml:"enabled"`
	CAFile  string `yaml:"ca_file"`
	CAPEM   string `yaml:"ca_pem"`
}

type EnrichmentConfig struct {
	Static      map[string]string         `yaml:"static"`
	Inventory   InventoryEnrichmentConfig `yaml:"inventory"`
	Kubernetes  KubernetesEnrichmentConf  `yaml:"kubernetes"`
	AWSIPRanges AWSIPRangesEnrichmentConf `yaml:"aws_ip_ranges"`
	DNS         DNSEnrichmentConfig       `yaml:"dns"`
	GeoIP       GeoIPEnrichmentConfig     `yaml:"geoip"`
}

type InventoryEnrichmentConfig struct {
	Enabled     bool          `yaml:"enabled"`
	EndpointURL string        `yaml:"endpoint_url"`
	CacheTTL    time.Duration `yaml:"cache_ttl"`
	Timeout     time.Duration `yaml:"timeout"`
}

func (f *InventoryEnrichmentConfig) UnmarshalYAML(value *yaml.Node) error {
	type rawInventoryConfig struct {
		Enabled     bool   `yaml:"enabled"`
		EndpointURL string `yaml:"endpoint_url"`
		CacheTTL    string `yaml:"cache_ttl"`
		Timeout     string `yaml:"timeout"`
	}

	var raw rawInventoryConfig
	if err := value.Decode(&raw); err != nil {
		return err
	}

	f.Enabled = raw.Enabled
	f.EndpointURL = raw.EndpointURL
	if raw.CacheTTL != "" {
		ttl, err := time.ParseDuration(raw.CacheTTL)
		if err != nil {
			return fmt.Errorf("parse enrichment.inventory.cache_ttl: %w", err)
		}
		f.CacheTTL = ttl
	}
	if raw.Timeout != "" {
		timeout, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return fmt.Errorf("parse enrichment.inventory.timeout: %w", err)
		}
		f.Timeout = timeout
	}
	return nil
}

type KubernetesEnrichmentConf struct {
	Enabled          bool     `yaml:"enabled"`
	Kubeconfig       string   `yaml:"kubeconfig"`
	InternalNetworks []string `yaml:"internal_networks"`
}

type AWSIPRangesEnrichmentConf struct {
	Enabled     bool          `yaml:"enabled"`
	EndpointURL string        `yaml:"endpoint_url"`
	CacheTTL    time.Duration `yaml:"cache_ttl"`
	CacheFile   string        `yaml:"cache_file"`
	Timeout     time.Duration `yaml:"timeout"`
}

func (a *AWSIPRangesEnrichmentConf) UnmarshalYAML(value *yaml.Node) error {
	type rawAWSIPRangesConfig struct {
		Enabled     bool   `yaml:"enabled"`
		EndpointURL string `yaml:"endpoint_url"`
		CacheTTL    string `yaml:"cache_ttl"`
		CacheFile   string `yaml:"cache_file"`
		Timeout     string `yaml:"timeout"`
	}

	var raw rawAWSIPRangesConfig
	if err := value.Decode(&raw); err != nil {
		return err
	}

	a.Enabled = raw.Enabled
	a.EndpointURL = raw.EndpointURL
	a.CacheFile = raw.CacheFile
	if raw.CacheTTL != "" {
		ttl, err := time.ParseDuration(raw.CacheTTL)
		if err != nil {
			return fmt.Errorf("parse enrichment.aws_ip_ranges.cache_ttl: %w", err)
		}
		a.CacheTTL = ttl
	}
	if raw.Timeout != "" {
		timeout, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return fmt.Errorf("parse enrichment.aws_ip_ranges.timeout: %w", err)
		}
		a.Timeout = timeout
	}
	return nil
}

type DNSEnrichmentConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Timeout         time.Duration `yaml:"timeout"`
	CacheTTL        time.Duration `yaml:"cache_ttl"`
	MaxCacheEntries int           `yaml:"max_cache_entries"`
}

func (d *DNSEnrichmentConfig) UnmarshalYAML(value *yaml.Node) error {
	type rawDNSConfig struct {
		Enabled         bool   `yaml:"enabled"`
		Timeout         string `yaml:"timeout"`
		CacheTTL        string `yaml:"cache_ttl"`
		MaxCacheEntries int    `yaml:"max_cache_entries"`
	}

	var raw rawDNSConfig
	if err := value.Decode(&raw); err != nil {
		return err
	}

	d.Enabled = raw.Enabled
	d.MaxCacheEntries = raw.MaxCacheEntries
	if raw.Timeout != "" {
		timeout, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return fmt.Errorf("parse enrichment.dns.timeout: %w", err)
		}
		d.Timeout = timeout
	}
	if raw.CacheTTL != "" {
		ttl, err := time.ParseDuration(raw.CacheTTL)
		if err != nil {
			return fmt.Errorf("parse enrichment.dns.cache_ttl: %w", err)
		}
		d.CacheTTL = ttl
	}
	return nil
}

type GeoIPEnrichmentConfig struct {
	Enabled          bool          `yaml:"enabled"`
	CacheTTL         time.Duration `yaml:"cache_ttl"`
	Timeout          time.Duration `yaml:"timeout"`
	DatabaseURL      string        `yaml:"database_url"`
	DatabaseSavePath string        `yaml:"database_save_path"`
}

func (g *GeoIPEnrichmentConfig) UnmarshalYAML(value *yaml.Node) error {
	type rawGeoIPConfig struct {
		Enabled          bool   `yaml:"enabled"`
		CacheTTL         string `yaml:"cache_ttl"`
		Timeout          string `yaml:"timeout"`
		DatabaseURL      string `yaml:"database_url"`
		DatabaseSavePath string `yaml:"database_save_path"`
	}

	var raw rawGeoIPConfig
	if err := value.Decode(&raw); err != nil {
		return err
	}

	g.Enabled = raw.Enabled
	g.DatabaseURL = raw.DatabaseURL
	g.DatabaseSavePath = raw.DatabaseSavePath
	if raw.CacheTTL != "" {
		ttl, err := time.ParseDuration(raw.CacheTTL)
		if err != nil {
			return fmt.Errorf("parse enrichment.geoip.cache_ttl: %w", err)
		}
		g.CacheTTL = ttl
	}
	if raw.Timeout != "" {
		timeout, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return fmt.Errorf("parse enrichment.geoip.timeout: %w", err)
		}
		g.Timeout = timeout
	}
	return nil
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (cfg *Config) SetDefaults() {
	if cfg.Global.Listener.Protocol == "" {
		cfg.Global.Listener.Protocol = "udp"
	}
	if cfg.Global.Listener.Address == "" {
		cfg.Global.Listener.Address = ":4739"
	}
	if cfg.Global.Listener.Metrics == "" {
		cfg.Global.Listener.Metrics = ":9090"
	}
	if cfg.Global.Logging.Level == "" {
		cfg.Global.Logging.Level = "info"
	}
	if cfg.Global.Logging.Format == "" {
		cfg.Global.Logging.Format = "json"
	}
	if cfg.Global.Pipeline.PacketQueueSize == 0 {
		cfg.Global.Pipeline.PacketQueueSize = 10000
	}
	if cfg.Global.Pipeline.FlowQueueSize == 0 {
		cfg.Global.Pipeline.FlowQueueSize = 50000
	}
	if cfg.Global.Pipeline.Workers == 0 {
		cfg.Global.Pipeline.Workers = 2
	}
	if cfg.Global.Pipeline.Batch.MaxRecords == 0 {
		cfg.Global.Pipeline.Batch.MaxRecords = 1000
	}
	if cfg.Global.Pipeline.Batch.FlushInterval == 0 {
		cfg.Global.Pipeline.Batch.FlushInterval = 5 * time.Second
	}
	if len(cfg.Outputs) == 0 {
		cfg.Outputs = []OutputConfig{{Type: "stdout", Enabled: true}}
	}
	for i := range cfg.Outputs {
		if cfg.Outputs[i].Type == "kafka" && cfg.Outputs[i].Config.PingTimeout == 0 {
			cfg.Outputs[i].Config.PingTimeout = 5 * time.Second
		}
	}
	if cfg.Enrichment.DNS.Timeout == 0 {
		cfg.Enrichment.DNS.Timeout = 500 * time.Millisecond
	}
	if cfg.Enrichment.DNS.CacheTTL == 0 {
		cfg.Enrichment.DNS.CacheTTL = time.Hour
	}
	if cfg.Enrichment.DNS.MaxCacheEntries == 0 {
		cfg.Enrichment.DNS.MaxCacheEntries = 100000
	}
	if cfg.Enrichment.Inventory.Enabled && cfg.Enrichment.Inventory.CacheTTL == 0 {
		cfg.Enrichment.Inventory.CacheTTL = time.Hour
	}
	if cfg.Enrichment.Inventory.Enabled && cfg.Enrichment.Inventory.Timeout == 0 {
		cfg.Enrichment.Inventory.Timeout = 30 * time.Second
	}
	if cfg.Enrichment.AWSIPRanges.Enabled && cfg.Enrichment.AWSIPRanges.EndpointURL == "" {
		cfg.Enrichment.AWSIPRanges.EndpointURL = "https://ip-ranges.amazonaws.com/ip-ranges.json"
	}
	if cfg.Enrichment.AWSIPRanges.Enabled && cfg.Enrichment.AWSIPRanges.CacheTTL == 0 {
		cfg.Enrichment.AWSIPRanges.CacheTTL = time.Hour
	}
	if cfg.Enrichment.AWSIPRanges.Enabled && cfg.Enrichment.AWSIPRanges.Timeout == 0 {
		cfg.Enrichment.AWSIPRanges.Timeout = 30 * time.Second
	}
	if cfg.Enrichment.GeoIP.Enabled && cfg.Enrichment.GeoIP.CacheTTL == 0 {
		cfg.Enrichment.GeoIP.CacheTTL = 24 * time.Hour
	}
	if cfg.Enrichment.GeoIP.Enabled && cfg.Enrichment.GeoIP.Timeout == 0 {
		cfg.Enrichment.GeoIP.Timeout = 5 * time.Minute
	}
}

func (cfg Config) Validate() error {
	if cfg.Global.Listener.Protocol != "udp" {
		return fmt.Errorf("unsupported listener protocol %q", cfg.Global.Listener.Protocol)
	}
	if cfg.Global.Pipeline.PacketQueueSize < 1 {
		return fmt.Errorf("global.pipeline.packet_queue_size must be positive")
	}
	if cfg.Global.Pipeline.FlowQueueSize < 1 {
		return fmt.Errorf("global.pipeline.flow_queue_size must be positive")
	}
	if cfg.Global.Pipeline.Workers < 1 {
		return fmt.Errorf("global.pipeline.workers must be positive")
	}
	if cfg.Global.Pipeline.Batch.MaxRecords < 1 {
		return fmt.Errorf("global.pipeline.batch.max_records must be positive")
	}
	if cfg.Global.Pipeline.Batch.FlushInterval <= 0 {
		return fmt.Errorf("global.pipeline.batch.flush_interval must be positive")
	}
	enabledOutputs := 0
	for i, output := range cfg.Outputs {
		if !output.Enabled {
			continue
		}
		enabledOutputs++
		switch output.Type {
		case "stdout":
		case "kafka":
			if len(output.Config.Brokers) == 0 {
				return fmt.Errorf("outputs[%d].config.brokers must be set when type is kafka", i)
			}
			if output.Config.Topic == "" {
				return fmt.Errorf("outputs[%d].config.topic must be set when type is kafka", i)
			}
			if output.Config.Linger < 0 {
				return fmt.Errorf("outputs[%d].config.linger must be non-negative", i)
			}
			if output.Config.BatchMaxBytes < 0 {
				return fmt.Errorf("outputs[%d].config.batch_max_bytes must be non-negative", i)
			}
			if output.Config.MaxBufferedRecords < 0 {
				return fmt.Errorf("outputs[%d].config.max_buffered_records must be non-negative", i)
			}
			if output.Config.MaxBufferedBytes < 0 {
				return fmt.Errorf("outputs[%d].config.max_buffered_bytes must be non-negative", i)
			}
			if output.Config.ProduceRequestTimeout < 0 {
				return fmt.Errorf("outputs[%d].config.produce_request_timeout must be non-negative", i)
			}
			if output.Config.PingTimeout <= 0 {
				return fmt.Errorf("outputs[%d].config.ping_timeout must be positive", i)
			}
			if output.Config.SSL.CAFile != "" && output.Config.SSL.CAPEM != "" {
				return fmt.Errorf("outputs[%d].config.ssl.ca_file and ca_pem are mutually exclusive", i)
			}
			switch output.Config.Compression {
			case "", "none", "snappy", "lz4", "zstd", "gzip":
			default:
				return fmt.Errorf("outputs[%d].config.compression must be one of none, snappy, lz4, zstd, gzip", i)
			}
		default:
			return fmt.Errorf("unsupported outputs[%d].type %q", i, output.Type)
		}
	}
	if enabledOutputs == 0 {
		return fmt.Errorf("at least one output must be enabled")
	}
	if cfg.Enrichment.GeoIP.Enabled {
		if cfg.Enrichment.GeoIP.DatabaseURL == "" {
			return fmt.Errorf("enrichment.geoip.database_url must be set when geoip is enabled")
		}
		if cfg.Enrichment.GeoIP.DatabaseSavePath == "" {
			return fmt.Errorf("enrichment.geoip.database_save_path must be set when geoip is enabled")
		}
		if cfg.Enrichment.GeoIP.CacheTTL <= 0 {
			return fmt.Errorf("enrichment.geoip.cache_ttl must be positive")
		}
	}
	if cfg.Enrichment.Inventory.Enabled {
		if cfg.Enrichment.Inventory.EndpointURL == "" {
			return fmt.Errorf("enrichment.inventory.endpoint_url must be set when inventory is enabled")
		}
		if cfg.Enrichment.Inventory.CacheTTL <= 0 {
			return fmt.Errorf("enrichment.inventory.cache_ttl must be positive")
		}
		if cfg.Enrichment.Inventory.Timeout <= 0 {
			return fmt.Errorf("enrichment.inventory.timeout must be positive")
		}
	}
	if cfg.Enrichment.AWSIPRanges.Enabled {
		if cfg.Enrichment.AWSIPRanges.EndpointURL == "" {
			return fmt.Errorf("enrichment.aws_ip_ranges.endpoint_url must be set when aws_ip_ranges is enabled")
		}
		if cfg.Enrichment.AWSIPRanges.CacheFile == "" {
			return fmt.Errorf("enrichment.aws_ip_ranges.cache_file must be set when aws_ip_ranges is enabled")
		}
		if cfg.Enrichment.AWSIPRanges.CacheTTL <= 0 {
			return fmt.Errorf("enrichment.aws_ip_ranges.cache_ttl must be positive")
		}
		if cfg.Enrichment.AWSIPRanges.Timeout <= 0 {
			return fmt.Errorf("enrichment.aws_ip_ranges.timeout must be positive")
		}
	}
	return nil
}
