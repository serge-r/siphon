package collector

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"
)

func TestBuildKafkaTLSConfigWithInlineCAPEM(t *testing.T) {
	cfg, err := buildKafkaTLSConfig(KafkaSSLConfig{
		Enabled: true,
		CAPEM:   testCAPEM(t),
	})
	if err != nil {
		t.Fatalf("buildKafkaTLSConfig() error = %v", err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs is nil")
	}
}

func TestBuildKafkaTLSConfigRejectsInvalidCAPEM(t *testing.T) {
	_, err := buildKafkaTLSConfig(KafkaSSLConfig{
		Enabled: true,
		CAPEM:   "not a certificate",
	})
	if err == nil {
		t.Fatal("buildKafkaTLSConfig() error is nil, want error")
	}
}

func TestKafkaCompressionCodecRejectsUnknownValue(t *testing.T) {
	if _, err := kafkaCompressionCodec("brotli"); err == nil {
		t.Fatal("kafkaCompressionCodec() error is nil, want error")
	}
}

func TestKafkaCompressionCodecAcceptsSupportedValues(t *testing.T) {
	for _, name := range []string{"none", "snappy", "lz4", "zstd", "gzip"} {
		if _, err := kafkaCompressionCodec(name); err != nil {
			t.Fatalf("kafkaCompressionCodec(%q) error = %v", name, err)
		}
	}
}

// A nil client is enough to prove Ready does no I/O: it would panic if it still
// pinged the brokers inline.
func TestKafkaSinkReadyReadsTheCachedPingVerdict(t *testing.T) {
	sink := &KafkaSink{logger: discardLogger()}

	readiness := sink.Ready(t.Context())
	if len(readiness) != 1 || readiness[0].Ready {
		t.Fatalf("Ready() = %+v, want a single not-ready entry before the first ping", readiness)
	}

	sink.lastErr.Store(errHolder{err: errors.New("dial tcp: connection refused")})
	if got := sink.Ready(t.Context())[0].Error; got != "dial tcp: connection refused" {
		t.Fatalf("Ready() error = %q, want the last ping error", got)
	}

	sink.ready.Store(true)
	if !sink.Ready(t.Context())[0].Ready {
		t.Fatal("Ready() is not ready, want ready after a successful ping")
	}
}

func testCAPEM(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	}))
}
