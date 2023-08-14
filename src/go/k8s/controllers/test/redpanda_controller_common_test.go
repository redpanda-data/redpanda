// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// nolint:testpackage // this name is ok
package test

import (
	"strings"

	"github.com/moby/moby/pkg/namesgenerator"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func getRandomizedNamespacedName(name string) (types.NamespacedName, *corev1.Namespace) {
	ns := strings.Replace(namesgenerator.GetRandomName(0), "_", "-", 1)
	key := types.NamespacedName{
		Name:      name,
		Namespace: ns,
	}
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}
	return key, namespace
}
