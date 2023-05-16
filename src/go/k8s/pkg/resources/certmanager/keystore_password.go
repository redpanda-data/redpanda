// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package certmanager

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

const (
	passwordKey = "password"

	keystoreSuffix = "keystore"
	separator      = "-"
)

// KeystoreSecretResource is part of the reconciliation of redpanda.vectorized.io CRD
// creating secrets for pkcs#12 and jks password
type KeystoreSecretResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *vectorizedv1alpha1.Cluster
	key          types.NamespacedName
	logger       logr.Logger
}

// NewKeystoreSecretResource creates KeystoreSecretResource instance
func NewKeystoreSecretResource(
	client k8sclient.Client,
	scheme *runtime.Scheme,
	pandaCluster *vectorizedv1alpha1.Cluster,
	key types.NamespacedName,
	logger logr.Logger,
) *KeystoreSecretResource {
	return &KeystoreSecretResource{
		client,
		scheme,
		pandaCluster,
		key,
		logger,
	}
}

// Ensure will manage pkcs#12 and jks password for KafkaAPI certificate
func (r *KeystoreSecretResource) Ensure(ctx context.Context) error {
	obj, err := r.obj()
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}

	_, err = resources.CreateIfNotExists(ctx, r, obj, r.logger)
	return err
}

// obj returns secret that consist password for jks and pkcs#12 keystores
func (r *KeystoreSecretResource) obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Key().Name,
			Namespace: r.Key().Namespace,
			Labels:    objLabels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		StringData: map[string]string{
			passwordKey: r.pandaCluster.Name,
		},
	}
	err := controllerutil.SetControllerReference(r.pandaCluster, secret, r.scheme)
	if err != nil {
		return nil, err
	}

	return secret, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *KeystoreSecretResource) Key() types.NamespacedName {
	return r.key
}

func keystoreName(clusterName string) string {
	suffixLength := len(keystoreSuffix)
	maxClusterNameLength := validation.DNS1123SubdomainMaxLength - suffixLength - len(separator)
	if len(clusterName) > maxClusterNameLength {
		clusterName = clusterName[:maxClusterNameLength]
	}
	return fmt.Sprintf("%s%s%s", clusterName, separator, keystoreSuffix)
}
