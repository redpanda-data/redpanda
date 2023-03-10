package redpanda

import (
	"context"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

type state interface {
	Do(context.Context, *redpandav1alpha1.Console, *redpandav1alpha1.Cluster, logr.Logger) (ctrl.Result, error)
}

// ConsoleState implements state
type ConsoleState struct {
	*ConsoleReconciler
}
