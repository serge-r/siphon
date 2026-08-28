package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type AWSIPRangesEnricher struct {
	cfg      AWSIPRangesEnrichmentConf
	logger   *slog.Logger
	client   *http.Client
	mu       sync.RWMutex
	ranges   []awsIPRange
	ready    atomic.Bool
	lastErr  atomic.Value
	stopOnce sync.Once
	stopCh   chan struct{}
}

type awsIPRangesResponse struct {
	SyncToken  string             `json:"syncToken"`
	CreateDate string             `json:"createDate"`
	Prefixes   []awsIPRangePrefix `json:"prefixes"`
}

type awsIPRangePrefix struct {
	IPPrefix string `json:"ip_prefix"`
	Region   string `json:"region"`
	Service  string `json:"service"`
}

type awsIPRange struct {
	prefix  netip.Prefix
	region  string
	service string
}

type awsIPRangeResult struct {
	Service string
	Region  string
}

var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

func NewAWSIPRangesEnricher(cfg AWSIPRangesEnrichmentConf, logger *slog.Logger) *AWSIPRangesEnricher {
	enricher := &AWSIPRangesEnricher{
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: cfg.Timeout},
		stopCh: make(chan struct{}),
	}

	logger.Info(
		"initializing aws ip ranges enricher",
		"endpoint_url", cfg.EndpointURL,
		"cache_ttl", cfg.CacheTTL,
		"cache_file", cfg.CacheFile,
		"timeout", cfg.Timeout,
	)
	go enricher.refreshLoop()
	return enricher
}

func (e *AWSIPRangesEnricher) Name() string { return "aws_ip_ranges" }

func (e *AWSIPRangesEnricher) Ready() error {
	if e.ready.Load() {
		return nil
	}

	// A failed refresh leaves the ranges from the last successful one in place,
	// so lookups keep working on staler data. See InventoryEnricher.Ready for
	// why that must not fail the readiness probe.
	if e.cached() > 0 {
		return nil
	}
	if err := loadErr(&e.lastErr); err != nil {
		return err
	}
	return fmt.Errorf("aws ip ranges data is not loaded")
}

func (e *AWSIPRangesEnricher) Fields() []SchemaField {
	return awsIPRangesSchemaFields()
}

func (e *AWSIPRangesEnricher) Enrich(_ context.Context, flow *EnrichedFlow) error {
	if result, ok := e.lookup(flow.SourceIP); ok {
		flow.SourceAWSService = result.Service
		flow.SourceAWSRegion = result.Region
	}
	if result, ok := e.lookup(flow.DestinationIP); ok {
		flow.DestinationAWSService = result.Service
		flow.DestinationAWSRegion = result.Region
	}
	return nil
}

func (e *AWSIPRangesEnricher) Close() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
	})
}

func (e *AWSIPRangesEnricher) refreshLoop() {
	runRefreshLoop(e.stopCh, e.cfg.CacheTTL, e.logger, e.Name(), e.refresh)
}

func (e *AWSIPRangesEnricher) refresh() error {
	if err := e.loadFreshOrDownload(); err != nil {
		e.ready.Store(false)
		e.lastErr.Store(errHolder{err: err})
		e.logger.Error("refresh aws ip ranges", "error", err)
		return err
	}
	e.ready.Store(true)
	e.lastErr.Store(errHolder{})
	return nil
}

func (e *AWSIPRangesEnricher) loadFreshOrDownload() error {
	if awsIPRangesCacheFresh(e.cfg.CacheFile, e.cfg.CacheTTL) {
		if err := e.loadFromCache(); err != nil {
			return err
		}
		return nil
	}

	if err := e.downloadCache(); err != nil {
		if _, statErr := os.Stat(e.cfg.CacheFile); statErr == nil {
			e.logger.Warn("using stale aws ip ranges cache", "cache_file", e.cfg.CacheFile, "error", err)
			return e.loadFromCache()
		}
		return err
	}
	return e.loadFromCache()
}

func (e *AWSIPRangesEnricher) downloadCache() error {
	req, err := http.NewRequest(http.MethodGet, e.cfg.EndpointURL, nil)
	if err != nil {
		return fmt.Errorf("create aws ip ranges request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch aws ip ranges: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("fetch aws ip ranges: unexpected status %s", resp.Status)
	}

	if err := os.MkdirAll(filepath.Dir(e.cfg.CacheFile), 0o755); err != nil {
		return fmt.Errorf("create aws ip ranges cache dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(e.cfg.CacheFile), ".ip-ranges-*.json")
	if err != nil {
		return fmt.Errorf("create aws ip ranges temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		return fmt.Errorf("write aws ip ranges temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close aws ip ranges temp file: %w", err)
	}
	if err := os.Rename(tmpPath, e.cfg.CacheFile); err != nil {
		return fmt.Errorf("replace aws ip ranges cache file: %w", err)
	}
	e.logger.Info("aws ip ranges cache downloaded", "cache_file", e.cfg.CacheFile)
	return nil
}

func (e *AWSIPRangesEnricher) loadFromCache() error {
	file, err := os.Open(e.cfg.CacheFile)
	if err != nil {
		return fmt.Errorf("open aws ip ranges cache %q: %w", e.cfg.CacheFile, err)
	}
	defer file.Close()

	var response awsIPRangesResponse
	if err := json.NewDecoder(file).Decode(&response); err != nil {
		return fmt.Errorf("decode aws ip ranges cache %q: %w", e.cfg.CacheFile, err)
	}

	ranges, err := buildAWSIPRanges(response.Prefixes)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.ranges = ranges
	e.mu.Unlock()

	e.logger.Info(
		"aws ip ranges loaded",
		"cache_file", e.cfg.CacheFile,
		"ranges", len(ranges),
		"sync_token", response.SyncToken,
		"create_date", response.CreateDate,
	)
	return nil
}

func (e *AWSIPRangesEnricher) lookup(rawIP string) (awsIPRangeResult, bool) {
	addr, err := netip.ParseAddr(rawIP)
	if err != nil || !isPublicIPv4(addr) {
		return awsIPRangeResult{}, false
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, r := range e.ranges {
		if r.prefix.Contains(addr) {
			return awsIPRangeResult{Service: r.service, Region: r.region}, true
		}
	}
	return awsIPRangeResult{}, false
}

// cached is the number of prefixes the enricher can currently match against.
func (e *AWSIPRangesEnricher) cached() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.ranges)
}

func buildAWSIPRanges(prefixes []awsIPRangePrefix) ([]awsIPRange, error) {
	ranges := make([]awsIPRange, 0, len(prefixes))
	for _, raw := range prefixes {
		if raw.IPPrefix == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(raw.IPPrefix)
		if err != nil {
			return nil, fmt.Errorf("parse aws ip prefix %q: %w", raw.IPPrefix, err)
		}
		prefix = prefix.Masked()
		if !isPublicIPv4(prefix.Addr()) {
			continue
		}
		ranges = append(ranges, awsIPRange{
			prefix:  prefix,
			region:  raw.Region,
			service: raw.Service,
		})
	}
	return ranges, nil
}

func awsIPRangesCacheFresh(path string, ttl time.Duration) bool {
	info, err := os.Stat(path)
	return err == nil && time.Since(info.ModTime()) < ttl
}

func isPublicIPv4(addr netip.Addr) bool {
	return addr.Is4() && isExternalIP(addr) && !sharedAddressSpace.Contains(addr)
}
