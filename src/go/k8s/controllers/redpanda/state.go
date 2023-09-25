// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda

import (
	"context"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
)

type state interface {
	Do(context.Context, *vectorizedv1alpha1.Console, *vectorizedv1alpha1.Cluster, logr.Logger) (ctrl.Result, error)
}

// ConsoleState implements state
type ConsoleState struct {
	*ConsoleReconciler
}
