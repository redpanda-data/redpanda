// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package node watches all nodes in order satisfy resources.NodesLister
// interface.
package node

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ resources.NodesLister = &NodeWatcher{}
var _ resources.NodeExternalIPFetcher = &NodeWatcher{}

const (
	nodeIPsList = "node-list.txt"
)

// NodeWatcher watch for node changes and updates configmap that
// are tight to Redpanda clusters with external connectivity.
// The changes happen whenever new nodes is in ready state or
// Kubernetes cluster loses node.
type NodeWatcher struct {
	client.Client
	Log	logr.Logger
	Scheme	*runtime.Scheme

	nodeListIPs	sync.Map
	configMapNames	[]types.NamespacedName
	nodeExternalIP	sync.Map
}

// Reconcile is the function that watches for node changes and updates
// configmaps that has assigned to Redpanda cluster with external
// connectivity enabled.
func (n *NodeWatcher) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := n.Log.WithValues("node-watcher", req.NamespacedName)

	var node corev1.Node
	if err := n.Get(ctx, req.NamespacedName, &node); err != nil {
		log.Error(err, "Unable to fetch node")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var internalIP, externalIP string
	for _, addr := range node.Status.Addresses {
		switch addr.Type {
		case corev1.NodeInternalIP:
			internalIP = addr.Address
		case corev1.NodeExternalIP:
			externalIP = addr.Address
		case corev1.NodeExternalDNS, corev1.NodeHostName, corev1.NodeInternalDNS:
		}
	}

	n.nodeListIPs.Store(internalIP, externalIP)
	n.nodeExternalIP.Store(node.Name, externalIP)

	for _, name := range n.configMapNames {
		cm := corev1.ConfigMap{}
		if err := n.Get(context.Background(), name, &cm); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to retrieve config map: %w", err)
		}

		cm.Data[nodeIPsList] = n.getNodeList()

		if err := n.Update(context.Background(), &cm); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update config map: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (n *NodeWatcher) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		Complete(n)
}

// RegisterConfigMap register config map for updates in node list
func (n *NodeWatcher) RegisterConfigMap(name types.NamespacedName) error {
	cm := corev1.ConfigMap{}
	if err := n.Get(context.Background(), name, &cm); err != nil {
		return fmt.Errorf("failed to retrieve config map: %w", err)
	}

	cm.Data[nodeIPsList] = n.getNodeList()

	if err := n.Update(context.Background(), &cm); err != nil {
		return fmt.Errorf("failed to update config map: %w", err)
	}

	n.configMapNames = append(n.configMapNames, name)
	return nil
}

// getNodeList return list of pair that consist of internal and external IP
// of all nodes.
func (n *NodeWatcher) getNodeList() string {
	IPsList := strings.Builder{}
	n.nodeListIPs.Range(func(internalIP, externalIP interface{}) bool {
		intIP, ok := internalIP.(string)
		if !ok {
			return true
		}

		extIP, ok := externalIP.(string)
		if !ok {
			return true
		}

		IPsList.WriteString(fmt.Sprintf("%s %s\n", intIP, extIP))
		return true
	})

	return IPsList.String()
}

// GetExternalIP returns cached external IP of an requested node
func (n *NodeWatcher) GetExternalIP(name string) string {
	externalIP, exist := n.nodeExternalIP.Load(name)
	if !exist {
		return ""
	}

	extIP, ok := externalIP.(string)
	if !ok {
		return ""
	}

	return extIP
}
