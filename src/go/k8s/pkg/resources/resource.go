// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package resources contains reconciliation logic for redpanda.vectorized.io CRD
package resources

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Resource decompose the reconciliation loop to specific kubernetes objects
type Resource interface {
	// Obj returns resource managed client.Object
	Obj() (client.Object, error)

	// Key returns namespace/name object that is used to identify object.
	// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
	Key() types.NamespacedName

	// Kind returns the canonical name of the kubernetes managed resource
	Kind() string

	// Ensure reconcile only one resource available in Kubernetes API server
	Ensure(ctx context.Context) error
}

// GetNodePort help to get first node port from the service
// The redpanda only exposes kafka api at the moment. Admin and RPC
// interface is hidden from the external user.
func GetNodePort(svc *corev1.Service) int32 {
	for _, port := range svc.Spec.Ports {
		if port.NodePort != 0 {
			return port.NodePort
		}
	}

	return -1
}

const (
	// As per https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md
	defaultLogLevel	= 2
	verboseLogLevel	= 3
)
