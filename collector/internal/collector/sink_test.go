package collector

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
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
