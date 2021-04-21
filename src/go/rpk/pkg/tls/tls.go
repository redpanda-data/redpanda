package tls

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

func LoadTLSConfig(
	truststoreFile, certFile, keyFile string,
) (*tls.Config, error) {
	caCertPool, err := LoadRootCACert(truststoreFile)
	if err != nil {
		return nil, err
	}
	certs, err := LoadCert(certFile, keyFile)
	if err != nil {
		return nil, err
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
	certFile, keyFile, truststoreFile string,
) (*tls.Config, error) {
	var tlsConfig *tls.Config
	var err error

	switch {
	case certFile == "" &&
		keyFile == "" &&
		truststoreFile == "":
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
