package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"

	"github.com/spf13/afero"
)

func LoadTLSConfig(
	fs afero.Fs, truststoreFile, certFile, keyFile string,
) (*tls.Config, error) {
	var err error
	var caCertPool *x509.CertPool
	var certs []tls.Certificate
	if truststoreFile != "" {
		caCertPool, err = LoadRootCACert(fs, truststoreFile)
		if err != nil {
			return nil, err
		}
	}
	if certFile != "" && keyFile != "" {
		certs, err = LoadCert(fs, certFile, keyFile)
		if err != nil {
			return nil, err
		}
	}
	tlsConf := &tls.Config{RootCAs: caCertPool, Certificates: certs}

	tlsConf.BuildNameToCertificate()
	return tlsConf, nil
}

func LoadRootCACert(
	fs afero.Fs, truststoreFile string,
) (*x509.CertPool, error) {
	caCert, err := afero.ReadFile(fs, truststoreFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return caCertPool, nil
}

func LoadCert(
	fs afero.Fs, certFile, keyFile string,
) ([]tls.Certificate, error) {
	certPEMBlock, err := afero.ReadFile(fs, certFile)
	if err != nil {
		return nil, err
	}
	keyPEMBlock, err := afero.ReadFile(fs, keyFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return nil, err
	}
	return []tls.Certificate{cert}, nil
}

func BuildTLSConfig(
	fs afero.Fs, enableTLS bool, certFile, keyFile, truststoreFile string,
) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}

	switch {
	case certFile != "" && keyFile == "":
		return nil, errors.New(
			"if a TLS client certificate is set, then its key must be passed to" +
				" enable TLS authentication",
		)
	case keyFile != "" && certFile == "":
		return nil, errors.New(
			"if a TLS client certificate key is set, then its certificate must be" +
				" passed to enable TLS authentication",
		)
	case certFile == "" &&
		keyFile == "" &&
		truststoreFile == "":
		if enableTLS {
			return tlsConfig, nil
		}
		return nil, nil

	case certFile != "" &&
		keyFile != "" &&
		truststoreFile != "":

		tlsConfig, err = LoadTLSConfig(
			fs,
			truststoreFile,
			certFile,
			keyFile,
		)
		if err != nil {
			return nil, err
		}

	// Enable TLS (no auth) if only the CA cert file is set for rpk
	case truststoreFile != "":
		caCertPool, err := LoadRootCACert(fs, truststoreFile)
		if err != nil {
			return nil, err
		}
		tlsConfig = &tls.Config{RootCAs: caCertPool}
	}
	return tlsConfig, nil
}
