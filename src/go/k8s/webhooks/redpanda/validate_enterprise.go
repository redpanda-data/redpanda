// Package redpanda defines Webhooks for redpanda API group
package redpanda

import (
	"context"
	"fmt"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	consolepkg "github.com/redpanda-data/redpanda/src/go/k8s/pkg/console"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrKeyNotFound is error when getting required key in ConfigMap/Secret
type ErrKeyNotFound struct {
	Message string
}

// Error implements error
func (e *ErrKeyNotFound) Error() string {
	return e.Message
}

// ValidateEnterpriseRBAC validates the referenced RBAC ConfigMap
func ValidateEnterpriseRBAC(
	ctx context.Context, cl client.Client, console *redpandav1alpha1.Console,
) error {
	if enterprise := console.Spec.Enterprise; enterprise != nil {
		configmap := &corev1.ConfigMap{}
		if err := cl.Get(ctx, client.ObjectKey{Namespace: console.GetNamespace(), Name: enterprise.RBAC.RoleBindingsRef.Name}, configmap); err != nil {
			return err
		}
		if _, ok := configmap.Data[consolepkg.EnterpriseRBACDataKey]; !ok {
			return &ErrKeyNotFound{fmt.Sprintf("must contain '%s' key", consolepkg.EnterpriseRBACDataKey)}
		}
	}
	return nil
}

// ValidateEnterpriseGoogleSA validates the referenced Google SA ConfigMap
func ValidateEnterpriseGoogleSA(
	ctx context.Context, cl client.Client, console *redpandav1alpha1.Console,
) error {
	if login := console.Spec.Login; console.IsGoogleLoginEnabled() && login.Google.Directory != nil {
		configmap := &corev1.ConfigMap{}
		if err := cl.Get(ctx, client.ObjectKey{Namespace: console.GetNamespace(), Name: login.Google.Directory.ServiceAccountRef.Name}, configmap); err != nil {
			return err
		}
		if _, ok := configmap.Data[consolepkg.EnterpriseGoogleSADataKey]; !ok {
			return &ErrKeyNotFound{fmt.Sprintf("must contain '%s' key", consolepkg.EnterpriseGoogleSADataKey)}
		}
	}
	return nil
}

// ValidateEnterpriseGoogleClientCredentials validates the referenced Google Client Credentials ConfigMap
func ValidateEnterpriseGoogleClientCredentials(
	ctx context.Context, cl client.Client, console *redpandav1alpha1.Console,
) error {
	if console.IsGoogleLoginEnabled() {
		cc := console.Spec.Login.Google.ClientCredentialsRef
		key := redpandav1alpha1.SecretKeyRef{Namespace: cc.Namespace, Name: cc.Name}
		ccSecret, err := key.GetSecret(ctx, cl)
		if err != nil {
			return err
		}
		if _, err := key.GetValue(ccSecret, consolepkg.EnterpriseGoogleClientIDSecretKey); err != nil {
			return &ErrKeyNotFound{fmt.Sprintf("must contain '%s' key", consolepkg.EnterpriseGoogleClientIDSecretKey)}
		}
		if _, err := key.GetValue(ccSecret, consolepkg.EnterpriseGoogleClientSecretKey); err != nil {
			return &ErrKeyNotFound{fmt.Sprintf("must contain '%s' key", consolepkg.EnterpriseGoogleClientSecretKey)}
		}
	}
	return nil
}
