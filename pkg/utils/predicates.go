package utils

import (
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DeletePredicate implements a predicate function that watches only delete events
type DeletePredicate struct {
	predicate.Funcs
}

func (DeletePredicate) Delete(e event.DeleteEvent) bool {
	return true
}
