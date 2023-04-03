// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	nginx = "nginx"

	// SSLPassthroughAnnotation is the annotation for ingress nginx SSL passthrough
	SSLPassthroughAnnotation = "nginx.ingress.kubernetes.io/ssl-passthrough" //nolint:gosec // This value does not contain credentials.

	debugLogLevel = 4

	// LEClusterIssuer is the LetsEncrypt issuer
	LEClusterIssuer = "letsencrypt-dns-prod"
)

var _ Resource = &IngressResource{}

// IngressResource is part of the reconciliation of redpanda.vectorized.io CRD
// focusing on the internal connectivity management of redpanda cluster
type IngressResource struct {
	k8sclient.Client
	scheme          *runtime.Scheme
	object          metav1.Object
	subdomain       string
	svcName         string
	svcPortName     string
	annotations     map[string]string
	TLS             []netv1.IngressTLS
	userConfig      *redpandav1alpha1.IngressConfig
	defaultEndpoint string
	logger          logr.Logger
}

// NewIngress creates IngressResource
func NewIngress(
	client k8sclient.Client,
	object metav1.Object,
	scheme *runtime.Scheme,
	subdomain string,
	svcName string,
	svcPortName string,
	logger logr.Logger,
) *IngressResource {
	return &IngressResource{
		client,
		scheme,
		object,
		subdomain,
		svcName,
		svcPortName,
		map[string]string{},
		nil,
		nil,
		"",
		logger.WithValues(
			"Kind", ingressKind(),
		),
	}
}

// WithAnnotations sets annotations to the IngressResource
func (r *IngressResource) WithAnnotations(
	annot map[string]string,
) *IngressResource {
	for k, v := range annot {
		r.annotations[k] = v
	}
	return r
}

// WithTLS sets Ingress TLS with specified issuer
func (r *IngressResource) WithTLS(
	clusterIssuer, secretName string,
) *IngressResource {
	r.annotations["cert-manager.io/cluster-issuer"] = clusterIssuer
	r.annotations["nginx.ingress.kubernetes.io/force-ssl-redirect"] = trueString

	if r.TLS == nil {
		r.TLS = []netv1.IngressTLS{}
	}

	r.TLS = append(r.TLS, netv1.IngressTLS{
		Hosts: []string{r.subdomain, fmt.Sprintf("*.%s", r.subdomain)},
		// Use the Cluster wildcard certificate
		SecretName: secretName,
	})

	return r
}

// GetAnnotations returns the annotations for the Ingress resource
func (r *IngressResource) GetAnnotations() map[string]string {
	allAnnotations := make(map[string]string)
	for k, v := range r.annotations {
		allAnnotations[k] = v
	}
	if r.userConfig != nil {
		// user configured annotations take precedence over default ones
		for k, v := range r.userConfig.Annotations {
			allAnnotations[k] = v
		}
	}

	return allAnnotations
}

// WithUserConfig injects the end-user configuration for the ingress
func (r *IngressResource) WithUserConfig(
	userConfig *redpandav1alpha1.IngressConfig,
) *IngressResource {
	r.userConfig = userConfig
	return r
}

// WithDefaultEndpoint allows to configure the default endpoint for the ingress,
// that is used when user does not inject a different one.
func (r *IngressResource) WithDefaultEndpoint(
	defaultEndpoint string,
) *IngressResource {
	r.defaultEndpoint = defaultEndpoint
	return r
}

func (r *IngressResource) host() string {
	if r.subdomain == "" {
		return ""
	}

	endpoint := r.defaultEndpoint
	if r.userConfig != nil {
		endpoint = r.userConfig.Endpoint
	}
	if endpoint == "" {
		return r.subdomain
	}

	return fmt.Sprintf("%s.%s", endpoint, r.subdomain)
}

// Ensure will manage kubernetes Ingress for redpanda.vectorized.io custom resource
func (r *IngressResource) Ensure(ctx context.Context) error {
	ingressDisabled := r.userConfig != nil && r.userConfig.Enabled != nil && !*r.userConfig.Enabled
	emptyHost := r.host() == ""
	if ingressDisabled || emptyHost {
		r.logger.V(debugLogLevel).Info("ingress will not be created", "disabled", ingressDisabled, "empty_host", emptyHost)
		key := r.Key()
		ingress := netv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: key.Namespace,
				Name:      key.Name,
			},
		}
		return DeleteIfExists(ctx, &ingress, r.Client)
	}

	obj, err := r.obj()
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}
	created, err := CreateIfNotExists(ctx, r, obj, r.logger)
	if err != nil || created {
		return err
	}
	var ingress netv1.Ingress
	err = r.Get(ctx, r.Key(), &ingress)
	if err != nil {
		return fmt.Errorf("error while fetching Ingress resource: %w", err)
	}
	_, err = Update(ctx, &ingress, obj, r.Client, r.logger)
	return err
}

func (r *IngressResource) obj() (k8sclient.Object, error) {
	ingressClassName := nginx
	pathTypePrefix := netv1.PathTypePrefix

	objLabels, err := objectLabels(r.object)
	if err != nil {
		return nil, fmt.Errorf("cannot get object labels: %w", err)
	}

	ingress := &netv1.Ingress{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Ingress",
			APIVersion: "networking.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        r.Key().Name,
			Namespace:   r.Key().Namespace,
			Labels:      objLabels,
			Annotations: r.GetAnnotations(),
		},
		Spec: netv1.IngressSpec{
			IngressClassName: &ingressClassName,
			Rules: []netv1.IngressRule{
				{
					Host: r.host(),
					IngressRuleValue: netv1.IngressRuleValue{
						HTTP: &netv1.HTTPIngressRuleValue{
							Paths: []netv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathTypePrefix,
									Backend: netv1.IngressBackend{
										Service: &netv1.IngressServiceBackend{
											Name: r.svcName,
											Port: netv1.ServiceBackendPort{
												Name: r.svcPortName,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			TLS: r.TLS,
		},
	}

	err = controllerutil.SetControllerReference(r.object, ingress, r.scheme)
	if err != nil {
		return nil, err
	}

	return ingress, nil
}

// Key returns namespace/name object that is used to identify object.
func (r *IngressResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.object.GetName(), Namespace: r.object.GetNamespace()}
}

func ingressKind() string {
	var obj netv1.Ingress
	return obj.Kind
}

func objectLabels(obj metav1.Object) (labels.CommonLabels, error) {
	var objLabels labels.CommonLabels
	switch o := obj.(type) {
	case *redpandav1alpha1.Cluster:
		objLabels = labels.ForCluster(o)
	case *redpandav1alpha1.Console:
		objLabels = labels.ForConsole(o)
	default:
		return nil, fmt.Errorf("expected object to be Cluster or Console") //nolint:goerr113 // no need to declare new error type
	}
	return objLabels, nil
}
