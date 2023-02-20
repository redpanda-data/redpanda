package resources

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
)

type license struct {
	client.Client
	scheme         *runtime.Scheme
	cluster        *redpandav1alpha1.Cluster
	adminAPIClient admin.AdminAPIClient
	hash           hash.Hash
	logger         logr.Logger
}

func NewLicense(cl client.Client, scheme *runtime.Scheme, cluster *redpandav1alpha1.Cluster, aa admin.AdminAPIClient, logger logr.Logger) *license {
	return &license{
		Client:         cl,
		scheme:         scheme,
		cluster:        cluster,
		adminAPIClient: aa,
		hash:           sha256.New(),
		logger:         logger,
	}
}

// Ensure manages license and makes sure it is loaded in the Cluster
func (l *license) Ensure(ctx context.Context) error {
	if l.cluster.Spec.LicenseRef == nil || l.adminAPIClient == nil {
		l.logger.V(debugLogLevel).Info(
			"Skip ensuring license, licenseRef or adminAPI internal listener not found",
			"licenseRef", l.cluster.Spec.LicenseRef,
			"adminAPIClientIsNil", l.adminAPIClient == nil,
		)
		return nil
	}

	cond := l.cluster.Status.GetCondition(redpandav1alpha1.ClusterConfiguredConditionType)
	if cond == nil || cond.Status != corev1.ConditionTrue {
		return &RequeueAfterError{RequeueAfter: time.Minute * 1, Msg: "Requeue ensuring license, Cluster not yet configured"}
	}

	// If user sets license and then removes it in the spec, the loaded license is not unset
	// Currently don't have admin API to unset license
	license, err := l.extract(ctx)
	if err != nil {
		return fmt.Errorf("extracting license from referenced secret: %w", err)
	}
	if err := l.load(ctx, license); err != nil {
		return fmt.Errorf("setting license: %w", err)
	}

	return nil
}

// Key returns namespaced name object to identify license Secret object
func (l *license) Key() types.NamespacedName {
	if ref := l.cluster.Spec.LicenseRef; ref != nil {
		return types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
	}
	return types.NamespacedName{}
}

// load sets the license in the Cluster
// Get loaded license first if any and don't set license if checksum is same
// SetLicense must be idempotent after PR https://github.com/redpanda-data/redpanda/pull/7130
// But this adds check on client (i.e. operator) of AdminAPI
func (l *license) load(ctx context.Context, license []byte) error {
	info, err := l.adminAPIClient.GetLicenseInfo(ctx)
	if err != nil {
		return fmt.Errorf("getting license info: %w", err)
	}

	prop := info.Properties
	if cs := checksum(l.hash, license); info.Loaded && prop.Checksum == cs {
		l.logger.V(debugLogLevel).Info(
			"Skip setting license, loaded license have same checksum",
			"loaded checksum", prop.Checksum,
			"checksum", cs,
		)
		return nil
	}

	l.logger.Info("Setting license", "licenseRef", l.cluster.Spec.LicenseRef)
	return l.adminAPIClient.SetLicense(ctx, bytes.NewReader(license))
}

func (l *license) extract(ctx context.Context) ([]byte, error) {
	ref := l.cluster.Spec.LicenseRef
	secret, err := ref.GetSecret(ctx, l.Client)
	if err != nil {
		return nil, err
	}
	return ref.GetValue(secret, redpandav1alpha1.DefaultLicenseSecretKey)
}

func checksum(h hash.Hash, license []byte) string {
	h.Write(license)
	return hex.EncodeToString(h.Sum(nil))
}
