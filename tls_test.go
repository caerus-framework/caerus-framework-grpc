package cf_grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestValidateClientConfigRejectsInsecureWithTLS(t *testing.T) {
	insecure := true
	err := validateClientConfig(&ClientConfig{
		Insecure:    &insecure,
		TLSCAFile:   "/tmp/ca.pem",
		TLSCertFile: "",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateServerConfigMTLS(t *testing.T) {
	if err := validateServerConfig(&ServerConfig{
		TLSCertFile:   "cert.pem",
		TLSKeyFile:    "key.pem",
		TLSCAFile:     "ca.pem",
		TLSClientAuth: string(TLSClientAuthRequireAndVerify),
	}); err != nil {
		t.Fatal(err)
	}
	if err := validateServerConfig(&ServerConfig{
		TLSClientAuth: string(TLSClientAuthRequireAndVerify),
	}); err == nil {
		t.Fatal("require_and_verify without CA should fail")
	}
}

func TestServerClientTLSRoundTrip(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := mustSelfSigned(t, "ca", true, nil, nil)
	srvCert, srvKey := mustSelfSigned(t, "localhost", false, caCert, caKey)

	caFile := writePEM(t, dir, "ca.pem", "CERTIFICATE", caCert.Raw)
	certFile := writePEM(t, dir, "server.pem", "CERTIFICATE", srvCert.Raw)
	keyFile := writeKey(t, dir, "server.key", srvKey)

	ctx := context.Background()
	fw := cf.New()
	srv := NewServer(
		WithBind("127.0.0.1:0"),
		WithServerTLS(certFile, keyFile),
		WithServerLogger(slog.Default()),
	)
	if err := srv.Init(ctx, fw); err != nil {
		t.Fatalf("server Init: %v", err)
	}
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	srv.Register(func(g *grpc.Server) {
		healthpb.RegisterHealthServer(g, hs)
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(runCtx) }()

	addr := waitAddr(t, srv)
	cli := NewClient(
		WithTarget(addr),
		WithInsecure(false),
		WithClientTLS(caFile, "", ""),
		WithTLSServerName("localhost"),
		WithConnectTimeout(2*time.Second),
		WithClientLogger(slog.Default()),
	)
	if err := cli.Init(ctx, fw); err != nil {
		t.Fatalf("client Init: %v", err)
	}
	t.Cleanup(func() { _ = cli.Shutdown(context.Background()) })

	hc := healthpb.NewHealthClient(cli.Conn())
	resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v", resp.GetStatus())
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
}

func waitAddr(t *testing.T, srv *Server) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr := srv.Addr()
		if addr != "" && addr != "127.0.0.1:0" && srv.listening.Load() {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not bind")
	return ""
}

func mustSelfSigned(t *testing.T, cn string, isCA bool, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	if isCA {
		tmpl.IsCA = true
		tmpl.KeyUsage |= x509.KeyUsageCertSign
		tmpl.BasicConstraintsValid = true
	}
	parentCert, parentPriv := tmpl, key
	if parent != nil {
		parentCert, parentPriv = parent, parentKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parentCert, &key.PublicKey, parentPriv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeKey(t *testing.T, dir, name string, key *ecdsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return writePEM(t, dir, name, "EC PRIVATE KEY", der)
}
