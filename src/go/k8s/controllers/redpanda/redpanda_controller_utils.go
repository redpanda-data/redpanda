package redpanda

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/fluxcd/pkg/runtime/logger"
	"github.com/fluxcd/source-controller/controllers"
	"github.com/go-logr/logr"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/registry"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	K8sInstanceLabelKey  = "app.kubernetes.io/instance"
	K8sNameLabelKey      = "app.kubernetes.io/name"
	K8sComponentLabelKey = "app.kubernetes.io/component"
	K8sManagedByLabelKey = "app.kubernetes.io/managed-by"

	EnvHelmReleaseNameKey = "REDPANDA_HELM_RELEASE_NAME"
)

var UpdateEventFilter = predicate.Funcs{
	CreateFunc:  func(e event.CreateEvent) bool { return false },
	UpdateFunc:  func(e event.UpdateEvent) bool { return true },
	DeleteFunc:  func(e event.DeleteEvent) bool { return false },
	GenericFunc: func(e event.GenericEvent) bool { return false },
}

var DeleteEventFilter = predicate.Funcs{
	CreateFunc:  func(e event.CreateEvent) bool { return false },
	UpdateFunc:  func(e event.UpdateEvent) bool { return false },
	DeleteFunc:  func(e event.DeleteEvent) bool { return true },
	GenericFunc: func(e event.GenericEvent) bool { return false },
}

// Check to see if the release name of a helm chart matches the name of a redpanda object
// this is by design for the operator
func isValidReleaseName(releaseName string, redpandaNameList []string) bool {
	for i := range redpandaNameList {
		if releaseName == redpandaNameList[i] {
			return true
		}
	}
	return false
}

func getHelmValues(log logr.Logger, releaseName, namespace string) (map[string]interface{}, error) {
	settings := cli.New()
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) { Debugf(log, format, v) }); err != nil {
		return nil, fmt.Errorf("could not create action-config for helm driver: %w", err)
	}

	gv := action.NewGetValues(actionConfig)
	gv.AllValues = true

	return gv.Run(releaseName)
}

func bestTrySetRetainPV(c client.Client, log logr.Logger, ctx context.Context, name, namespace string) {
	log.WithName("RedpandaNodePVCReconciler.bestTrySetRetainPV")
	pv := &corev1.PersistentVolume{}
	if getErr := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, pv); getErr != nil {
		Infof(log, "could not change retain policy of pv %s", pv.Name)
		return
	}
	// try to set reclaim policy, fail if we cannot set this to avoid data loss
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		if updateErr := c.Update(ctx, pv); updateErr != nil {
			// no need to place error here. we simply move on and not attempt to remove the pv
			Infof(log, "could not set reclaim policy for %s; continuing: %s", pv.Name, updateErr.Error())
		}
	}
}

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

func Infof(log logr.Logger, format string, a ...interface{}) {
	log.Info(fmt.Sprintf(format, a...))
}

func Debugf(log logr.Logger, format string, a ...interface{}) {
	log.V(logger.DebugLevel).Info(fmt.Sprintf(format, a...))
}

func Tracef(log logr.Logger, format string, a ...interface{}) {
	log.V(logger.TraceLevel).Info(fmt.Sprintf(format, a...))
}
