// Copyright 2021 Vectorized, Inc.
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
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// ScramPandaproxyUsername is the username for Panda proxy
	ScramPandaproxyUsername = "pandaproxy_client"
	// ScramSchemaRegistryUsername is the username for schema registry
	ScramSchemaRegistryUsername = "schemaregistry_client"

	// PandaProxySuffix is the suffix for the kubernetes secret
	// where sasl credentials (username and password) for panda
	// proxy client is held
	PandaProxySuffix = "sasl"
	// SchemaRegistrySuffix is the suffix for the kubernetes secret
	// where sasl credentials (username and password) for schema
	// registry client is held
	SchemaRegistrySuffix = "schema-registry-sasl"
)

var _ Resource = &SuperUsersResource{}

// SuperUsersResource is part of the reconciliation of redpanda.vectorized.io CRD
// focusing on the super users for Schema Registry and Panda proxy
type SuperUsersResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	username     string
	suffix       string
	logger       logr.Logger
}

// NewSuperUsers creates SuperUsersResource that managed super users
// for Schema Registry and Panda proxy
func NewSuperUsers(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	username string,
	suffix string,
	logger logr.Logger,
) *SuperUsersResource {
	return &SuperUsersResource{
		client,
		scheme,
		pandaCluster,
		username,
		suffix,
		logger.WithValues(
			"Kind", ingressKind(),
		),
	}
}

// Ensure will manage Super users for redpanda.vectorized.io custom resource
func (r *SuperUsersResource) Ensure(ctx context.Context) error {
	if r == nil {
		return nil
	}

	obj, err := r.obj()
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}
	_, err = CreateIfNotExists(ctx, r, obj, r.logger)
	return err
}

func (r *SuperUsersResource) obj() (k8sclient.Object, error) {
	password, err := generatePassword(scramPasswordLength)
	if err != nil {
		return nil, fmt.Errorf("could not generate SASL password: %w", err)
	}

	obj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Key().Name,
			Namespace: r.Key().Namespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		Type: corev1.SecretTypeBasicAuth,
		Data: map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte(r.username),
			corev1.BasicAuthPasswordKey: []byte(password),
		},
	}

	err = controllerutil.SetControllerReference(r.pandaCluster, obj, r.scheme)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

// Key returns namespace/name object that is used to identify object.
func (r *SuperUsersResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: resourceNameTrim(r.pandaCluster.Name, r.suffix), Namespace: r.pandaCluster.Namespace}
}
