// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources_test

import (
	"context"
	"testing"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	res "github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/stretchr/testify/assert"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsure_PDB(t *testing.T) {
	cluster := pandaCluster()
	one := intstr.FromInt(1)
	two := intstr.FromInt(1)
	cluster.Spec.PodDisruptionBudget = &redpandav1alpha1.PDBConfig{
		Enabled:      true,
		MinAvailable: &one,
	}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      cluster.Name,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector:     labels.ForCluster(cluster).AsAPISelector(),
			MinAvailable: &one,
		},
	}
	clusterTwo := cluster.DeepCopy()
	clusterTwo.Spec.PodDisruptionBudget.MinAvailable = &two
	pdbTwo := pdb.DeepCopy()
	pdbTwo.Spec.MinAvailable = &two
	tests := []struct {
		name           string
		existingObject client.Object
		pandaCluster   *redpandav1alpha1.Cluster
		expectedObject *policyv1.PodDisruptionBudget
	}{
		{"none existing", nil, cluster, pdb},
		{"update to cluster with minAvailable 2", pdb, clusterTwo, pdbTwo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().Build()

			err := redpandav1alpha1.AddToScheme(scheme.Scheme)
			assert.NoError(t, err, tt.name)

			if tt.existingObject != nil {
				tt.existingObject.SetResourceVersion("")

				err = c.Create(context.Background(), tt.existingObject)
				assert.NoError(t, err, tt.name)
			}

			err = c.Create(context.Background(), tt.pandaCluster)
			assert.NoError(t, err)

			pdb := res.NewPDB(
				c,
				tt.pandaCluster,
				scheme.Scheme,
				ctrl.Log.WithName("test"))

			err = pdb.Ensure(context.Background())
			assert.NoError(t, err, tt.name)

			actual := &policyv1.PodDisruptionBudget{}
			err = c.Get(context.Background(), pdb.Key(), actual)
			assert.NoError(t, err, tt.name)
			assert.Equal(t, tt.expectedObject.Spec, actual.Spec)

			// retrieved object and new object should produce no diff and thus
			// not be updated
			updated, err := res.Update(context.TODO(), actual, actual, c, ctrl.Log.WithName("test"))
			assert.NoError(t, err)
			assert.False(t, updated)
		})
	}
}
