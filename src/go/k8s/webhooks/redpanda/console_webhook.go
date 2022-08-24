package redpanda

import (
	"context"
	"fmt"
	"net/http"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
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

// ConsoleValidator handles admission for Console
func (v *ConsoleValidator) Handle(
	ctx context.Context, req admission.Request,
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

	return admission.Allowed("")
}

// ConsoleValidator implements admission.DecoderInjector.
// A decoder will be automatically injected.

// InjectDecoder injects the decoder.
func (v *ConsoleValidator) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
