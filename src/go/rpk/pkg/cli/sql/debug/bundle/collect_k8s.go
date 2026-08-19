// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package bundle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// k8sResources dumps the namespace's core-v1 resources under a kubernetes/
// subtree, one JSON file per resource kind, mirroring what `rpk debug bundle`
// collects. Collection runs only when the process is inside a Kubernetes pod
// (in-cluster config resolves); anywhere else it is skipped silently, so
// workstation runs against a remote cluster stay k8s-free.
func (b *bundle) k8sResources(ctx context.Context) {
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		b.results = append(b.results, collectionResult{
			Node:   "k8s",
			RPC:    "resources",
			Status: "skipped",
			Error:  fmt.Sprintf("kubernetes unavailable: %v", err),
		})
		return
	}
	// One request per resource kind; raise the burst to avoid throttling.
	k8sCfg.Burst = 30
	clientset, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		b.errs = append(b.errs, fmt.Sprintf("[k8s] unable to create kubernetes client: %v", err))
		return
	}

	namespace := b.namespace()
	restInterface := clientset.CoreV1().RESTClient()
	for _, resource := range []string{
		"configmaps",
		"endpoints",
		"events",
		"limitranges",
		"persistentvolumeclaims",
		"pods",
		"replicationcontrollers",
		"resourcequotas",
		"serviceaccounts",
		"services",
	} {
		start := time.Now()
		raw, err := restInterface.Get().Namespace(namespace).Resource(resource).Do(ctx).Raw()
		if err != nil {
			// client-go can hide the server's Status message; prefer the body's.
			var status struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(raw, &status) == nil && status.Message != "" {
				err = errors.New(status.Message)
			}
		}
		b.record("k8s", resource, start, err)
		if err == nil {
			b.addJSON(fmt.Sprintf("%s/kubernetes/%s.json", bundleRoot, resource), raw)
		}
	}
}

// namespace resolves the collection namespace: --namespace, then $NAMESPACE,
// then the pod's service-account namespace, then "default".
func (b *bundle) namespace() string {
	if b.opts.Namespace != "" {
		return b.opts.Namespace
	}
	if ns := os.Getenv("NAMESPACE"); ns != "" {
		return ns
	}
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(data))
	}
	return "default"
}
