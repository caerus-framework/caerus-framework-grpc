package cf_grpc

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// TLSClientAuth names how the server treats client certificates.
type TLSClientAuth string

const (
	// TLSClientAuthNone does not request a client certificate (default when no CA).
	TLSClientAuthNone TLSClientAuth = "none"
	// TLSClientAuthRequireAndVerify requires a client cert signed by tls_ca_file.
	TLSClientAuthRequireAndVerify TLSClientAuth = "require_and_verify"
)

func normalizeTLSClientAuth(v string) (TLSClientAuth, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", string(TLSClientAuthNone):
		return TLSClientAuthNone, nil
	case string(TLSClientAuthRequireAndVerify):
		return TLSClientAuthRequireAndVerify, nil
	default:
		return "", fmt.Errorf("cf_grpc: tls_client_auth must be %q or %q, got %q",
			TLSClientAuthNone, TLSClientAuthRequireAndVerify, v)
	}
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("cf_grpc: read TLS CA file %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("cf_grpc: TLS CA file %s contains no valid certificates", caFile)
	}
	return pool, nil
}

func loadKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	if (certFile == "") != (keyFile == "") {
		return tls.Certificate{}, errors.New("cf_grpc: TLS cert and key must be set together")
	}
	if certFile == "" {
		return tls.Certificate{}, errors.New("cf_grpc: TLS cert and key are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("cf_grpc: load TLS keypair: %w", err)
	}
	return cert, nil
}

func buildClientTLSConfig(caFile, certFile, keyFile, serverName string, insecureSkipVerify bool) (*tls.Config, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("cf_grpc: TLS client cert and key must be set together")
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify, //nolint:gosec // explicit lab/break-glass setting
	}
	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if certFile != "" {
		cert, err := loadKeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func buildServerTLSConfig(caFile, certFile, keyFile string, clientAuth TLSClientAuth) (*tls.Config, error) {
	cert, err := loadKeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	switch clientAuth {
	case TLSClientAuthNone, "":
		cfg.ClientAuth = tls.NoClientCert
	case TLSClientAuthRequireAndVerify:
		if caFile == "" {
			return nil, errors.New("cf_grpc: tls_client_auth=require_and_verify needs tls_ca_file")
		}
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	default:
		return nil, fmt.Errorf("cf_grpc: unknown tls_client_auth %q", clientAuth)
	}
	if caFile != "" && clientAuth == TLSClientAuthNone {
		// CA present but auth none: still load pool unused — prefer explicit require.
		// Keep none as documented; ignore CA with a clear error so ops notice.
		return nil, errors.New("cf_grpc: tls_ca_file set but tls_client_auth is none; set require_and_verify or omit tls_ca_file")
	}
	return cfg, nil
}

func tlsFilesEqual(aCA, aCert, aKey, bCA, bCert, bKey string) bool {
	return aCA == bCA && aCert == bCert && aKey == bKey
}
