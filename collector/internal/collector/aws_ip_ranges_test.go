package collector

import (
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildAWSIPRangesIgnoresIPv6AndPrivatePrefixes(t *testing.T) {
	ranges, err := buildAWSIPRanges([]awsIPRangePrefix{
		{IPPrefix: "3.5.140.0/22", Region: "ap-northeast-2", Service: "AMAZON"},
		{IPPrefix: "10.0.0.0/8", Region: "private", Service: "PRIVATE"},
		{IPPrefix: "100.64.0.0/10", Region: "shared", Service: "SHARED"},
		{IPPrefix: "2600:1f00::/36", Region: "us-east-1", Service: "AMAZON"},
	})
	if err != nil {
		t.Fatalf("buildAWSIPRanges() error = %v", err)
	}
	if len(ranges) != 1 {
		t.Fatalf("len(ranges) = %d, want 1", len(ranges))
	}
	if ranges[0].region != "ap-northeast-2" {
		t.Fatalf("region = %q, want ap-northeast-2", ranges[0].region)
	}
	if ranges[0].service != "AMAZON" {
		t.Fatalf("service = %q, want AMAZON", ranges[0].service)
	}
}

func TestAWSIPRangesEnricherEnrichesPublicIPv4(t *testing.T) {
	enricher := &AWSIPRangesEnricher{
		ranges: []awsIPRange{
			{
				prefix:  mustParsePrefix(t, "3.5.140.0/22"),
				region:  "ap-northeast-2",
				service: "AMAZON",
			},
		},
	}

	flow := EnrichedFlow{
		SourceIP:      "10.0.0.10",
		DestinationIP: "3.5.141.1",
	}
	if err := enricher.Enrich(t.Context(), &flow); err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if flow.SourceAWSService != "" {
		t.Fatalf("SourceAWSService = %q, want empty", flow.SourceAWSService)
	}
	if flow.DestinationAWSService != "AMAZON" {
		t.Fatalf("DestinationAWSService = %q, want AMAZON", flow.DestinationAWSService)
	}
	if flow.DestinationAWSRegion != "ap-northeast-2" {
		t.Fatalf("DestinationAWSRegion = %q, want ap-northeast-2", flow.DestinationAWSRegion)
	}
}

func TestAWSIPRangesCacheFresh(t *testing.T) {
	cacheFile := filepath.Join(t.TempDir(), "ip-ranges.json")
	if awsIPRangesCacheFresh(cacheFile, time.Hour) {
		t.Fatal("awsIPRangesCacheFresh() = true for missing file")
	}
	if err := os.WriteFile(cacheFile, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write cache file: %v", err)
	}
	if !awsIPRangesCacheFresh(cacheFile, time.Hour) {
		t.Fatal("awsIPRangesCacheFresh() = false for fresh file")
	}
}

func TestBuildJSONSchemaForAWSIPRanges(t *testing.T) {
	schema := BuildJSONSchemaForConfig(Config{
		Enrichment: EnrichmentConfig{
			AWSIPRanges: AWSIPRangesEnrichmentConf{Enabled: true},
		},
	})

	if _, ok := schema.Properties["source_aws_service"]; !ok {
		t.Fatal("schema does not contain source_aws_service")
	}
	if _, ok := schema.Properties["destination_aws_region"]; !ok {
		t.Fatal("schema does not contain destination_aws_region")
	}
}

func mustParsePrefix(t *testing.T, rawPrefix string) netip.Prefix {
	t.Helper()
	prefix, err := netip.ParsePrefix(rawPrefix)
	if err != nil {
		t.Fatal(err)
	}
	return prefix.Masked()
}
