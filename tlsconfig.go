package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// buildTLSConfig builds a *tls.Config for talking to a Schema Registry
// over HTTPS. caFile adds a CA to trust alongside the system roots;
// certFile/keyFile enable mTLS; insecureSkipVerify disables server
// certificate verification entirely (test/dev use only - never for a
// real cluster). Applied to both -a and -b, since they typically share
// the same CA trust chain even when their credentials differ.
func buildTLSConfig(caFile, certFile, keyFile string, insecureSkipVerify bool) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: insecureSkipVerify}

	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read -tls-ca %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in -tls-ca %s", caFile)
		}
		cfg.RootCAs = pool
	}

	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("-tls-cert and -tls-key must both be set for mTLS")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load -tls-cert/-tls-key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
