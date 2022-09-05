// Package redpanda defines Webhooks for redpanda API group
package redpanda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	consolepkg "github.com/redpanda-data/redpanda/src/go/k8s/pkg/console"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// +kubebuilder:webhook:path=/validate-redpanda-vectorized-io-v1alpha1-console,mutating=false,failurePolicy=fail,sideEffects=None,groups="redpanda.vectorized.io",resources=consoles,verbs=create;update,versions=v1alpha1,name=vconsole.kb.io,admissionReviewVersions=v1

// ConsoleValidator validates Consoles
type ConsoleValidator struct {
	Client  client.Client
	decoder *admission.Decoder
}

// Handle processes admission for Console
func (v *ConsoleValidator) Handle(
	ctx context.Context, req admission.Request, // nolint:gocritic // interface not require pointer
) admission.Response {
	console := &redpandav1alpha1.Console{}

	err := v.decoder.Decode(req, console)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if !console.IsAllowedNamespace() {
		return admission.Denied(fmt.Sprintf("cluster %s/%s is in different namespace", console.Spec.ClusterKeyRef.Namespace, console.Spec.ClusterKeyRef.Name))
	}

	cluster := &redpandav1alpha1.Cluster{}
	if err := v.Client.Get(ctx, console.GetClusterRef(), cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return admission.Denied(fmt.Sprintf("cluster %s/%s not found", console.Spec.ClusterKeyRef.Namespace, console.Spec.ClusterKeyRef.Name))
		}
		return admission.Errored(http.StatusBadRequest, err)
	}

	if err := ValidateEnterpriseRBAC(ctx, v.Client, console); err != nil {
		if errors.Is(err, &ErrKeyNotFound{}) {
			return admission.Denied(err.Error())
		}
		return admission.Errored(http.StatusBadRequest, err)
	}

	if err := ValidateEnterpriseGoogleClientCredentials(ctx, v.Client, console); err != nil {
		if errors.Is(err, &ErrKeyNotFound{}) {
			return admission.Denied(err.Error())
		}
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := ValidateEnterpriseGoogleSA(ctx, v.Client, console); err != nil {
		if errors.Is(err, &ErrKeyNotFound{}) {
			return admission.Denied(err.Error())
		}
		return admission.Errored(http.StatusBadRequest, err)
	}

	return admission.Allowed("")
}

// ConsoleValidator implements admission.DecoderInjector.
// A decoder will be automatically injected.

// InjectDecoder injects the decoder.
func (v *ConsoleValidator) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}

// +kubebuilder:webhook:path=/mutate-redpanda-vectorized-io-v1alpha1-console,mutating=true,failurePolicy=fail,sideEffects=None,groups="redpanda.vectorized.io",resources=consoles,verbs=create;update,versions=v1alpha1,name=mconsole.kb.io,admissionReviewVersions=v1

// ConsoleDefaulter mutates Consoles
type ConsoleDefaulter struct {
	Client  client.Client
	decoder *admission.Decoder
}

// Handle processes admission for Console
func (m *ConsoleDefaulter) Handle(
	ctx context.Context, req admission.Request, // nolint:gocritic // interface not require pointer
) admission.Response {
	console := &redpandav1alpha1.Console{}

	err := m.decoder.Decode(req, console)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	response, err := m.Default(console)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	return *response
}

// Default implements admission defaulting
func (m *ConsoleDefaulter) Default(console *redpandav1alpha1.Console) (*admission.Response, error) {
	original, err := json.Marshal(console.DeepCopy())
	if err != nil {
		return nil, err
	}

	if login := console.Spec.Login; login != nil && login.JWTSecretRef.Key == "" {
		login.JWTSecretRef.Key = consolepkg.DefaultJWTSecretKey
	}
	if license := console.Spec.LicenseRef; license != nil && license.Key == "" {
		license.Key = consolepkg.DefaultLicenseSecretKey
	}

	current, err := json.Marshal(console)
	if err != nil {
		return nil, err
	}
	response := admission.PatchResponseFromRaw(original, current)
	return &response, nil
}

// ConsoleDefaulter implements admission.DecoderInjector.
// A decoder will be automatically injected.

// InjectDecoder injects the decoder.
func (m *ConsoleDefaulter) InjectDecoder(d *admission.Decoder) error {
	m.decoder = d
	return nil
}
