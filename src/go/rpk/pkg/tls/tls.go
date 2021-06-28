package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
)

func LoadTLSConfig(
	truststoreFile, certFile, keyFile string,
) (*tls.Config, error) {
	var err error
	var caCertPool *x509.CertPool
	var certs []tls.Certificate
	if truststoreFile != "" {
		caCertPool, err = LoadRootCACert(truststoreFile)
		if err != nil {
			return nil, err
		}
	}
	if certFile != "" && keyFile != "" {
		certs, err = LoadCert(certFile, keyFile)
		if err != nil {
			return nil, err
		}
	}
	tlsConf := &tls.Config{RootCAs: caCertPool, Certificates: certs}

	tlsConf.BuildNameToCertificate()
	return tlsConf, nil
}

func LoadRootCACert(truststoreFile string) (*x509.CertPool, error) {
	caCert, err := ioutil.ReadFile(truststoreFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return caCertPool, nil
}

func LoadCert(certFile, keyFile string) ([]tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return []tls.Certificate{cert}, nil
}

func BuildTLSConfig(
	enableTLS bool,
	certFile, keyFile, truststoreFile string,
) (*tls.Config, error) {
	tlsConfig := &tls.Config{}
	var err error

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
			truststoreFile,
			certFile,
			keyFile,
		)
		if err != nil {
			return nil, err
		}

	// Enable TLS (no auth) if only the CA cert file is set for rpk
	case truststoreFile != "":
		caCertPool, err := LoadRootCACert(truststoreFile)
		if err != nil {
			return nil, err
		}
		tlsConfig = &tls.Config{RootCAs: caCertPool}
	}
	return tlsConfig, nil
}
