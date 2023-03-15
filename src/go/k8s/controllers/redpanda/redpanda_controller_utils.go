package redpanda

import (
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/fluxcd/source-controller/controllers"
	"github.com/go-logr/logr"
	"helm.sh/helm/v3/pkg/registry"
	"k8s.io/apimachinery/pkg/util/errors"
)

func ClientGenerator(isLogin bool) (*registry.Client, string, error) {
	if isLogin {
		// create a temporary file to store the credentials
		// this is needed because otherwise the credentials are stored in ~/.docker/config.json.
		credentialsFile, err := os.CreateTemp("", "credentials")
		if err != nil {
			return nil, "", err
		}

		var errs []error
		rClient, err := registry.NewClient(registry.ClientOptWriter(io.Discard), registry.ClientOptCredentialsFile(credentialsFile.Name()))
		if err != nil {
			errs = append(errs, err)
			// attempt to delete the temporary file
			if credentialsFile != nil {
				err := os.Remove(credentialsFile.Name())
				if err != nil {
					errs = append(errs, err)
				}
			}
			return nil, "", errors.NewAggregate(errs)
		}
		return rClient, credentialsFile.Name(), nil
	}

	rClient, err := registry.NewClient(registry.ClientOptWriter(io.Discard))
	if err != nil {
		return nil, "", err
	}
	return rClient, "", nil
}

func MustInitStorage(path string, storageAdvAddr string, artifactRetentionTTL time.Duration, artifactRetentionRecords int, l logr.Logger) *controllers.Storage {
	if path == "" {
		p, _ := os.Getwd()
		path = filepath.Join(p, "bin")
		os.MkdirAll(path, 0o700)
	}

	storage, err := controllers.NewStorage(path, storageAdvAddr, artifactRetentionTTL, artifactRetentionRecords)
	if err != nil {
		l.Error(err, "unable to initialise storage")
		os.Exit(1)
	}

	return storage
}

func DetermineAdvStorageAddr(storageAddr string, l logr.Logger) string {
	host, port, err := net.SplitHostPort(storageAddr)
	if err != nil {
		l.Error(err, "unable to parse storage address")
		os.Exit(1)
	}
	switch host {
	case "":
		host = "localhost"
	case "0.0.0.0":
		host = os.Getenv("HOSTNAME")
		if host == "" {
			hn, err := os.Hostname()
			if err != nil {
				l.Error(err, "0.0.0.0 specified in storage addr but hostname is invalid")
				os.Exit(1)
			}
			host = hn
		}
	}
	return net.JoinHostPort(host, port)
}

func StartFileServer(path string, address string, l logr.Logger) {
	l.Info("starting file server")
	fs := http.FileServer(http.Dir(path))
	mux := http.NewServeMux()
	mux.Handle("/", fs)
	err := http.ListenAndServe(address, mux)
	if err != nil {
		l.Error(err, "file server error")
	}
}
