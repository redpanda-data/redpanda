// Copyright 2022 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// nolint:funlen // this is ok for a test
func TestRedpandaResourceRequirements(t *testing.T) {
	type test struct {
		name                string
		setRequestsCPU      resource.Quantity
		setRequestsMem      resource.Quantity
		setRedpandaCPU      resource.Quantity
		setRedpandaMem      resource.Quantity
		expectedRedpandaCPU resource.Quantity
		expectedRedpandaMem resource.Quantity
	}
	makeResources := func(t test) v1alpha1.RedpandaResourceRequirements {
		return v1alpha1.RedpandaResourceRequirements{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: t.setRequestsMem,
					corev1.ResourceCPU:    t.setRequestsCPU,
				}},
			Redpanda: corev1.ResourceList{
				corev1.ResourceMemory: t.setRedpandaMem,
				corev1.ResourceCPU:    t.setRedpandaCPU,
			},
		}
	}

	t.Run("Memory", func(t *testing.T) {
		tests := []test{
			{
				name:                "RedpandaMemory is set from requests.memory",
				setRequestsMem:      resource.MustParse("3000Mi"),
				expectedRedpandaMem: resource.MustParse("3000Mi"),
			},
			{
				name:                "RedpandaMemory is set from lower redpanda.memory",
				setRequestsMem:      resource.MustParse("4000Mi"),
				setRedpandaMem:      resource.MustParse("3000Mi"),
				expectedRedpandaMem: resource.MustParse("3000Mi"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rrr := makeResources(tt)
				assert.Equal(t, tt.expectedRedpandaMem.Value(), rrr.RedpandaMemory().Value())
			})
		}
	})

	t.Run("Cpu", func(t *testing.T) {
		tests := []test{
			{
				name:                "RedpandaCPU is set from integer requests.cpu",
				setRequestsCPU:      resource.MustParse("1"),
				setRequestsMem:      resource.MustParse("20Gi"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU is set from milli requests.cpu",
				setRequestsCPU:      resource.MustParse("1000m"),
				setRequestsMem:      resource.MustParse("20Gi"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU is rounded up from milli requests.cpu",
				setRequestsCPU:      resource.MustParse("1001m"),
				setRequestsMem:      resource.MustParse("20Gi"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU is set from lower redpanda.cpu",
				setRequestsCPU:      resource.MustParse("2"),
				setRequestsMem:      resource.MustParse("20Gi"),
				setRedpandaCPU:      resource.MustParse("1"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU is set from higher redpanda.cpu",
				setRequestsCPU:      resource.MustParse("1"),
				setRequestsMem:      resource.MustParse("20Gi"),
				setRedpandaCPU:      resource.MustParse("2"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU is rounded up from milli redpanda.cpu",
				setRequestsCPU:      resource.MustParse("1"),
				setRequestsMem:      resource.MustParse("20Gi"),
				setRedpandaCPU:      resource.MustParse("1001m"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU is limited by 2GiB/core",
				setRequestsCPU:      resource.MustParse("10"),
				setRequestsMem:      resource.MustParse("4Gi"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU has a minimum if requests >0",
				setRequestsCPU:      resource.MustParse("100m"),
				setRequestsMem:      resource.MustParse("100Mi"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU not set if no request",
				expectedRedpandaCPU: resource.MustParse("0"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rrr := makeResources(tt)
				assert.Equal(t, tt.expectedRedpandaCPU.Value(), rrr.RedpandaCPU().Value())
			})
		}
	})
}
