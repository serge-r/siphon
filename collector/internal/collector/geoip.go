package collector

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oschwald/maxminddb-golang"
)

const (
	geoIPCityDBName    = "GeoIP2-City"
	geoIPCountryDBName = "GeoIP2-Country"
	geoIPISPDBName     = "GeoIP2-ISP"
)

var geoIPDatabaseNames = []string{
	geoIPCityDBName,
	geoIPCountryDBName,
	geoIPISPDBName,
}

type GeoIPEnricher struct {
	cfg      GeoIPEnrichmentConfig
	dbPaths  map[string]string
	logger   *slog.Logger
	mu       sync.RWMutex
	readers  geoIPReaders
	ready    atomic.Bool
	lastErr  atomic.Value
	client   *http.Client
	stopOnce sync.Once
	stopCh   chan struct{}
}

type geoIPReaders struct {
	city    *maxminddb.Reader
	country *maxminddb.Reader
	isp     *maxminddb.Reader
}

type geoIPISPRecord struct {
	AutonomousSystemNumber       uint   `maxminddb:"autonomous_system_number"`
	AutonomousSystemOrganization string `maxminddb:"autonomous_system_organization"`
	ISP                          string `maxminddb:"isp"`
}

type geoIPCountryRecord struct {
	Country struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
}

type geoIPCityRecord struct {
	Country struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
}

type geoIPResult struct {
	ISP     string
	ASN     uint
	Country string
	City    string
}

func NewGeoIPEnricher(cfg GeoIPEnrichmentConfig, logger *slog.Logger) (*GeoIPEnricher, error) {
	if !strings.HasPrefix(cfg.DatabaseURL, "http://") && !strings.HasPrefix(cfg.DatabaseURL, "https://") {
		return nil, fmt.Errorf("geoip database_url must be http or https URL")
	}

	dbPaths := geoIPDatabasePaths(cfg.DatabaseSavePath)
	enricher := &GeoIPEnricher{
		cfg:     cfg,
		dbPaths: dbPaths,
		logger:  logger,
		client:  &http.Client{Timeout: cfg.Timeout},
		stopCh:  make(chan struct{}),
	}

	if err := os.MkdirAll(cfg.DatabaseSavePath, 0o755); err != nil {
		return nil, fmt.Errorf("create geoip database directory %q: %w", cfg.DatabaseSavePath, err)
	}

	enricher.logger.Info(
		"initializing geoip enricher",
		"database_url", cfg.DatabaseURL,
		"database_save_path", cfg.DatabaseSavePath,
		"cache_ttl", cfg.CacheTTL,
	)

	go enricher.refreshLoop()
	return enricher, nil
}

func (e *GeoIPEnricher) Name() string { return "geoip" }

func (e *GeoIPEnricher) Ready() error {
	if e.ready.Load() {
		return nil
	}
	if err, ok := e.lastErr.Load().(error); ok && err != nil {
		return err
	}
	return fmt.Errorf("geoip databases are not loaded")
}

func (e *GeoIPEnricher) Fields() []SchemaField {
	return geoIPSchemaFields()
}

func (e *GeoIPEnricher) Enrich(_ context.Context, flow *EnrichedFlow) error {
	if result, ok := e.lookup(flow.SourceIP); ok {
		flow.SourceGeoIPISP = result.ISP
		flow.SourceGeoIPASN = result.ASN
		flow.SourceGeoIPCountry = result.Country
		flow.SourceGeoIPCity = result.City
	}
	if result, ok := e.lookup(flow.DestinationIP); ok {
		flow.DestinationGeoIPISP = result.ISP
		flow.DestinationGeoIPASN = result.ASN
		flow.DestinationGeoIPCountry = result.Country
		flow.DestinationGeoIPCity = result.City
	}
	return nil
}

func (e *GeoIPEnricher) Close() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
		e.mu.Lock()
		readers := e.readers
		e.readers = geoIPReaders{}
		e.mu.Unlock()
		closeGeoIPReaders(readers)
	})
}

func (e *GeoIPEnricher) refreshLoop() {
	e.refresh()

	ticker := time.NewTicker(e.cfg.CacheTTL)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.refresh()
		}
	}
}

func (e *GeoIPEnricher) refresh() {
	if err := e.loadFreshOrDownload(); err != nil {
		e.ready.Store(false)
		e.lastErr.Store(err)
		e.logger.Error("refresh geoip databases", "error", err)
		return
	}
	e.ready.Store(true)
}

func (e *GeoIPEnricher) loadFreshOrDownload() error {
	if geoIPDatabasesFresh(e.dbPaths, e.cfg.CacheTTL) {
		return e.openDatabases()
	}

	if err := e.downloadDatabases(); err != nil {
		if geoIPDatabasesExist(e.dbPaths) {
			e.logger.Warn("using stale geoip databases after download failure", "error", err)
			return e.openDatabases()
		}
		return err
	}
	return e.openDatabases()
}

func (e *GeoIPEnricher) downloadDatabases() error {
	for _, databaseName := range geoIPDatabaseNames {
		if err := e.downloadDatabaseArchive(databaseName); err != nil {
			return err
		}
	}
	return nil
}

func (e *GeoIPEnricher) downloadDatabaseArchive(databaseName string) error {
	archiveURL := geoIPArchiveURL(e.cfg.DatabaseURL, databaseName)
	dbPath := e.dbPaths[databaseName]
	e.logger.Info("downloading geoip database archive", "url", archiveURL, "path", dbPath)

	resp, err := e.client.Get(archiveURL)
	if err != nil {
		return fmt.Errorf("download geoip database archive %q: %w", archiveURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download geoip database archive %q: unexpected status %s", archiveURL, resp.Status)
	}

	if err := extractMMDBFromTarGZ(resp.Body, databaseName, dbPath); err != nil {
		return fmt.Errorf("extract geoip database archive %q: %w", archiveURL, err)
	}
	e.logger.Info("geoip database archive downloaded and extracted", "database", databaseName, "path", dbPath)
	return nil
}

func (e *GeoIPEnricher) openDatabases() error {
	readers, err := openGeoIPReaders(e.dbPaths)
	if err != nil {
		return err
	}

	e.mu.Lock()
	oldReaders := e.readers
	e.readers = readers
	e.mu.Unlock()

	closeGeoIPReaders(oldReaders)
	e.logger.Info("geoip databases loaded", "database_save_path", e.cfg.DatabaseSavePath)
	return nil
}

func (e *GeoIPEnricher) lookup(rawIP string) (geoIPResult, bool) {
	addr, err := netip.ParseAddr(rawIP)
	if err != nil || !isExternalIP(addr) {
		return geoIPResult{}, false
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.readers.city == nil || e.readers.country == nil || e.readers.isp == nil {
		return geoIPResult{}, false
	}

	ip := net.IP(addr.AsSlice())
	result := geoIPResult{}

	var ispRecord geoIPISPRecord
	if err := e.readers.isp.Lookup(ip, &ispRecord); err != nil {
		e.logger.Debug("lookup geoip isp", "ip", rawIP, "error", err)
	}
	result.ISP = ispRecord.ISP
	if result.ISP == "" {
		result.ISP = ispRecord.AutonomousSystemOrganization
	}
	result.ASN = ispRecord.AutonomousSystemNumber

	var countryRecord geoIPCountryRecord
	if err := e.readers.country.Lookup(ip, &countryRecord); err != nil {
		e.logger.Debug("lookup geoip country", "ip", rawIP, "error", err)
	}
	result.Country = localizedName(countryRecord.Country.Names, countryRecord.Country.ISOCode)

	var cityRecord geoIPCityRecord
	if err := e.readers.city.Lookup(ip, &cityRecord); err != nil {
		e.logger.Debug("lookup geoip city", "ip", rawIP, "error", err)
	}
	if result.Country == "" {
		result.Country = localizedName(cityRecord.Country.Names, cityRecord.Country.ISOCode)
	}
	result.City = localizedName(cityRecord.City.Names, "")

	return result, result.ISP != "" || result.ASN != 0 || result.Country != "" || result.City != ""
}

func geoIPDatabasePaths(savePath string) map[string]string {
	return map[string]string{
		geoIPCityDBName:    filepath.Join(savePath, geoIPCityDBName+".mmdb"),
		geoIPCountryDBName: filepath.Join(savePath, geoIPCountryDBName+".mmdb"),
		geoIPISPDBName:     filepath.Join(savePath, geoIPISPDBName+".mmdb"),
	}
}

func geoIPArchiveURL(baseURL string, databaseName string) string {
	return strings.TrimRight(baseURL, "/") + "/" + databaseName + ".tar.gz"
}

func geoIPDatabasesFresh(dbPaths map[string]string, ttl time.Duration) bool {
	for _, databaseName := range geoIPDatabaseNames {
		info, err := os.Stat(dbPaths[databaseName])
		if err != nil || time.Since(info.ModTime()) >= ttl {
			return false
		}
	}
	return true
}

func geoIPDatabasesExist(dbPaths map[string]string) bool {
	for _, databaseName := range geoIPDatabaseNames {
		if _, err := os.Stat(dbPaths[databaseName]); err != nil {
			return false
		}
	}
	return true
}

func openGeoIPReaders(dbPaths map[string]string) (geoIPReaders, error) {
	city, err := maxminddb.Open(dbPaths[geoIPCityDBName])
	if err != nil {
		return geoIPReaders{}, fmt.Errorf("open geoip city database %q: %w", dbPaths[geoIPCityDBName], err)
	}
	country, err := maxminddb.Open(dbPaths[geoIPCountryDBName])
	if err != nil {
		city.Close()
		return geoIPReaders{}, fmt.Errorf("open geoip country database %q: %w", dbPaths[geoIPCountryDBName], err)
	}
	isp, err := maxminddb.Open(dbPaths[geoIPISPDBName])
	if err != nil {
		city.Close()
		country.Close()
		return geoIPReaders{}, fmt.Errorf("open geoip isp database %q: %w", dbPaths[geoIPISPDBName], err)
	}
	return geoIPReaders{city: city, country: country, isp: isp}, nil
}

func closeGeoIPReaders(readers geoIPReaders) {
	if readers.city != nil {
		readers.city.Close()
	}
	if readers.country != nil {
		readers.country.Close()
	}
	if readers.isp != nil {
		readers.isp.Close()
	}
}

func extractMMDBFromTarGZ(src io.Reader, databaseName string, dstPath string) error {
	gzipReader, err := gzip.NewReader(src)
	if err != nil {
		return fmt.Errorf("open gzip stream: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	tmpPath := dstPath + ".tmp"
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("read tar stream: %w", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}

		filename := filepath.Base(header.Name)
		if filename != databaseName+".mmdb" {
			continue
		}

		out, err := os.Create(tmpPath)
		if err != nil {
			return fmt.Errorf("create temp database %q: %w", tmpPath, err)
		}
		if _, err := io.Copy(out, tarReader); err != nil {
			out.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("write temp database %q: %w", tmpPath, err)
		}
		if err := out.Close(); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("close temp database %q: %w", tmpPath, err)
		}
		if err := os.Rename(tmpPath, dstPath); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("replace database %q: %w", dstPath, err)
		}
		return nil
	}
	return fmt.Errorf("archive does not contain %s.mmdb", databaseName)
}

func localizedName(names map[string]string, fallback string) string {
	if names == nil {
		return fallback
	}
	if value := names["en"]; value != "" {
		return value
	}
	for _, value := range names {
		if value != "" {
			return value
		}
	}
	return fallback
}

func isExternalIP(addr netip.Addr) bool {
	return addr.IsValid() &&
		!addr.IsPrivate() &&
		!addr.IsLoopback() &&
		!addr.IsLinkLocalUnicast() &&
		!addr.IsLinkLocalMulticast() &&
		!addr.IsMulticast() &&
		!addr.IsUnspecified()
}
