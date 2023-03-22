package redpanda

import (
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"k8s.io/utils/pointer"

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
		return tryCreateNewClient(credentialsFile)
	}

	rClient, err := registry.NewClient(registry.ClientOptWriter(io.Discard))
	if err != nil {
		return nil, "", err
	}
	return rClient, "", nil
}

func tryCreateNewClient(credentialsFile *os.File) (*registry.Client, string, error) {
	var errs []error
	rClient, err := registry.NewClient(registry.ClientOptWriter(io.Discard), registry.ClientOptCredentialsFile(credentialsFile.Name()))
	if err != nil {
		errs = append(errs, err)
		// attempt to delete the temporary file
		if credentialsFile != nil {
			if removeErr := os.Remove(credentialsFile.Name()); removeErr != nil {
				errs = append(errs, removeErr)
			}
		}
		return nil, "", errors.NewAggregate(errs)
	}
	return rClient, credentialsFile.Name(), nil
}

func MustInitStorage(path, storageAdvAddr string, artifactRetentionTTL time.Duration, artifactRetentionRecords int, l logr.Logger) *controllers.Storage {
	if path == "" {
		p, _ := os.Getwd()
		path = filepath.Join(p, "bin")
		err := os.MkdirAll(path, 0o700)
		if err != nil {
			l.Error(err, "unable make directory with right permissions")
		}
	}

	storage, err := controllers.NewStorage(path, storageAdvAddr, artifactRetentionTTL, artifactRetentionRecords)
	if err != nil {
		l.Error(err, "unable to initialize storage")
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

func StartFileServer(path, address string, l logr.Logger) {
	l.Info("starting file server")
	fs := http.FileServer(http.Dir(path))
	mux := http.NewServeMux()
	mux.Handle("/", fs)
	//nolint:gosec // we are aware there are no timeouts supported
	err := http.ListenAndServe(address, mux)
	if err != nil {
		l.Error(err, "file server error")
	}
}

func IsBoolPointerNILorEqual(a *bool, b bool) bool {
	return a == nil || pointer.BoolEqual(a, pointer.Bool(b))
}
