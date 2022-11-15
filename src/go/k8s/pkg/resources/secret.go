// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
)

const (
	lifecycleSuffix = "lifecycle"
	postStartKey    = "postStart.sh"
	preStopKey      = "preStop.sh"
)

var _ Resource = &PreStartStopScriptResource{}

// PreStartStopScriptResource contains definition and reconciliation logic for operator's Secret
// The Secret contains the postStart and preStop scripts that are mounted in the redpanda
// Pods and are run during the postStart and preStop lifecycle phases.
type PreStartStopScriptResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster

	serviceFQDN            string
	pandaproxySASLUser     types.NamespacedName
	schemaRegistrySASLUser types.NamespacedName
	logger                 logr.Logger
}

// PreStartStopScriptSecret creates SecretResource
func PreStartStopScriptSecret(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	serviceFQDN string,
	pandaproxySASLUser types.NamespacedName,
	schemaRegistrySASLUser types.NamespacedName,
	logger logr.Logger,
) *PreStartStopScriptResource {
	return &PreStartStopScriptResource{
		client,
		scheme,
		pandaCluster,
		serviceFQDN,
		pandaproxySASLUser,
		schemaRegistrySASLUser,
		logger.WithValues("Kind", secretKind()),
	}
}

// Ensure will manage kubernetes v1.Secret for redpanda STS
func (r *PreStartStopScriptResource) Ensure(ctx context.Context) error {
	obj, err := r.obj()
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}
	created, err := CreateIfNotExists(ctx, r, obj, r.logger)
	if err != nil || created {
		return err
	}
	var secret corev1.Secret
	err = r.Get(ctx, r.Key(), &secret)
	if err != nil {
		return fmt.Errorf("error while fetching Secret resource: %w", err)
	}

	return r.update(ctx, &secret, obj.(*corev1.Secret), r.Client, r.logger)
}

func (r *PreStartStopScriptResource) update(
	ctx context.Context,
	current *corev1.Secret,
	modified *corev1.Secret,
	c k8sclient.Client,
	logger logr.Logger,
) error {
	// Do not touch existing last-applied-configuration (it's not reconciled in the main loop)
	if val, ok := current.Annotations[LastAppliedConfigurationAnnotationKey]; ok {
		if modified.Annotations == nil {
			modified.Annotations = make(map[string]string)
		}
		modified.Annotations[LastAppliedConfigurationAnnotationKey] = val
	}

	_, err := Update(ctx, current, modified, c, logger)
	return err
}

// obj returns resource managed client.Object
func (r *PreStartStopScriptResource) obj() (k8sclient.Object, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    labels.ForCluster(r.pandaCluster),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		Data: map[string][]byte{},
	}

	secret.Data[postStartKey] = []byte(r.getPostStartScript())
	secret.Data[preStopKey] = []byte(r.getPreStopScript())

	err := controllerutil.SetControllerReference(r.pandaCluster, secret, r.scheme)
	if err != nil {
		return nil, err
	}

	return secret, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *PreStartStopScriptResource) Key() types.NamespacedName {
	return SecretKey(r.pandaCluster)
}

// SecretKey provides config map name that derived from redpanda.vectorized.io CR
func SecretKey(pandaCluster *redpandav1alpha1.Cluster) types.NamespacedName {
	return types.NamespacedName{Name: resourceNameTrim(pandaCluster.Name, lifecycleSuffix), Namespace: pandaCluster.Namespace}
}

func secretKind() string {
	var cfg corev1.Secret
	return cfg.Kind
}

// getPostStartScript creates a script that removes maintenance mode after startup.
func (r *PreStartStopScriptResource) getPostStartScript() string {
	// TODO replace scripts with proper RPK calls
	curlNodeIDCommand := r.composeCURLGetNodeIDCommand("--silent --fail")
	curlCommand := r.composeCURLMaintenanceCommand(`-X DELETE --silent -o /dev/null -w "%{http_code}"`, nil)
	// HTTP code 400 is returned by v22 nodes during an upgrade from v21 until the new version reaches quorum and the maintenance mode feature is enabled
	// TODO(joe): The command builder shouldn't add auth/tls flags, they should be added to an environment variable
	// in the StatefulSet otherwise a race condition could occur where the mounted script could be updated before
	// the new setting has been configured on the pod's redpanda node.
	return fmt.Sprintf(`#!/usr/bin/env bash
set -e

until NODE_ID=$(%s | grep -o '\"node_id\":[^,}]*' | grep -o '[^: ]*$'); do
	sleep 0.5
done
echo "Clearing maintenance mode on node ${NODE_ID}"
until [ "${status:-}" = "200" ] || [ "${status:-}" = "400" ]; do
	status=$(%s)
	sleep 0.5
done`, curlNodeIDCommand, curlCommand)
}

// getPrestopScript creates a script that drains the node before shutting down.
func (r *PreStartStopScriptResource) getPreStopScript() string {
	// TODO replace scripts with proper RPK calls
	curlNodeIDCommand := r.composeCURLGetNodeIDCommand("--silent --fail")
	curlMaintenanceCommand := r.composeCURLMaintenanceCommand(`-X PUT --silent -o /dev/null -w "%{http_code}"`, nil)
	genericMaintenancePath := "/v1/maintenance"
	curlGetMaintenanceCommand := r.composeCURLMaintenanceCommand(`--silent`, &genericMaintenancePath)
	// TODO(joe): The command builder shouldn't add auth/tls flags, they should be added to an environment variable
	// in the StatefulSet otherwise a race condition could occur where the mounted script could be updated before
	// the new setting has been configured on the pod's redpanda node.
	return fmt.Sprintf(`#!/usr/bin/env bash
set -e

until NODE_ID=$(%s | grep -o '\"node_id\":[^,}]*' | grep -o '[^: ]*$'); do
	sleep 0.5
done
echo "Setting maintenance mode on node ${NODE_ID}"
until [ "${status:-}" = "200" ]; do
	status=$(%s)
	sleep 0.5
done
until [ "${finished:-}" = "true" ] || [ "${draining:-}" = "false" ]; do
	res=$(%s)
	finished=$(echo $res | grep -o '\"finished\":[^,}]*' | grep -o '[^: ]*$')
	draining=$(echo $res | grep -o '\"draining\":[^,}]*' | grep -o '[^: ]*$')
	sleep 0.5
done`, curlNodeIDCommand, curlMaintenanceCommand, curlGetMaintenanceCommand)
}

//nolint:goconst // no need
func (r *PreStartStopScriptResource) composeCURLGetNodeIDCommand(
	options string,
) string {
	adminAPI := r.pandaCluster.AdminAPIInternal()

	cmd := fmt.Sprintf(`curl %s `, options)

	tlsConfig := adminAPI.GetTLS()
	proto := "http"
	if tlsConfig != nil && tlsConfig.Enabled {
		proto = "https"
		if tlsConfig.RequireClientAuth {
			cmd += "--cacert /etc/tls/certs/admin/ca/ca.crt --cert /etc/tls/certs/admin/tls.crt --key /etc/tls/certs/admin/tls.key "
		} else {
			cmd += "--cacert /etc/tls/certs/admin/tls.crt "
		}
	}
	cmd += fmt.Sprintf("%s://${POD_NAME}.%s.%s.svc.cluster.local:%d", proto, r.pandaCluster.Name, r.pandaCluster.Namespace, adminAPI.Port)
	cmd += "/v1/node_config"
	return cmd
}

func (r *PreStartStopScriptResource) composeCURLMaintenanceCommand(
	options string, urlOverwrite *string,
) string {
	adminAPI := r.pandaCluster.AdminAPIInternal()

	cmd := fmt.Sprintf(`curl %s `, options)

	tlsConfig := adminAPI.GetTLS()
	proto := "http"
	if tlsConfig != nil && tlsConfig.Enabled {
		proto = "https"
		if tlsConfig.RequireClientAuth {
			cmd += "--cacert /etc/tls/certs/admin/ca/ca.crt --cert /etc/tls/certs/admin/tls.crt --key /etc/tls/certs/admin/tls.key "
		} else {
			cmd += "--cacert /etc/tls/certs/admin/tls.crt "
		}
	}
	cmd += fmt.Sprintf("%s://${POD_NAME}.%s.%s.svc.cluster.local:%d", proto, r.pandaCluster.Name, r.pandaCluster.Namespace, adminAPI.Port)
	if urlOverwrite == nil {
		cmd += "/v1/brokers/${NODE_ID}/maintenance"
	} else {
		cmd += *urlOverwrite
	}
	return cmd
}
