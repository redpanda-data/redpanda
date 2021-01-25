package resources

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Resource interface {
	Obj() (client.Object, error)
	Key() types.NamespacedName
	Kind() string

	Ensure(ctx context.Context) error
}
