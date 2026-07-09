package collector

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
)

func TestGeoIPDatabasePaths(t *testing.T) {
	paths := geoIPDatabasePaths("/var/lib/geoip")

	tests := map[string]string{
		geoIPCityDBName:    "/var/lib/geoip/GeoIP2-City.mmdb",
		geoIPCountryDBName: "/var/lib/geoip/GeoIP2-Country.mmdb",
		geoIPISPDBName:     "/var/lib/geoip/GeoIP2-ISP.mmdb",
	}
	for databaseName, want := range tests {
		if got := paths[databaseName]; got != want {
			t.Fatalf("paths[%q] = %q, want %q", databaseName, got, want)
		}
	}
}

func TestGeoIPArchiveURL(t *testing.T) {
	got := geoIPArchiveURL("https://example.com/maxmind/", geoIPCityDBName)
	want := "https://example.com/maxmind/GeoIP2-City.tar.gz"
	if got != want {
		t.Fatalf("geoIPArchiveURL() = %q, want %q", got, want)
	}
}

func TestExtractMMDBFromTarGZ(t *testing.T) {
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "GeoIP2-City.mmdb")

	archive := testTarGZ(t, "GeoIP2-City_20260626/GeoIP2-City.mmdb", []byte("mmdb-data"))
	if err := extractMMDBFromTarGZ(bytes.NewReader(archive), geoIPCityDBName, dstPath); err != nil {
		t.Fatalf("extractMMDBFromTarGZ() error = %v", err)
	}

	data, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "mmdb-data" {
		t.Fatalf("extracted data = %q, want mmdb-data", data)
	}
}

func mustParseAddr(t *testing.T, rawIP string) netip.Addr {
	t.Helper()
	addr, err := netip.ParseAddr(rawIP)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestGeoIPExternalIPCheck(t *testing.T) {
	tests := map[string]bool{
		"8.8.8.8":     true,
		"1.1.1.1":     true,
		"10.0.0.1":    false,
		"192.168.1.1": false,
		"127.0.0.1":   false,
	}

	for rawIP, want := range tests {
		got := isExternalIP(mustParseAddr(t, rawIP))
		if got != want {
			t.Fatalf("isExternalIP(%q) = %v, want %v", rawIP, got, want)
		}
	}
}

func TestBuildJSONSchemaForGeoIP(t *testing.T) {
	schema := BuildJSONSchemaForConfig(Config{
		Enrichment: EnrichmentConfig{
			GeoIP: GeoIPEnrichmentConfig{Enabled: true},
		},
	})

	if _, ok := schema.Properties["source_geoip_isp"]; !ok {
		t.Fatal("schema does not contain source_geoip_isp")
	}
	if _, ok := schema.Properties["destination_geoip_asn"]; !ok {
		t.Fatal("schema does not contain destination_geoip_asn")
	}
}

func testTarGZ(t *testing.T, filename string, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzipWriter)

	if err := tarWriter.WriteHeader(&tar.Header{
		Name: filename,
		Mode: 0o644,
		Size: int64(len(data)),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
