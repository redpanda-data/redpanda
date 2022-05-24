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
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	hooksSuffix = "hooks"

	readinessProbeEntryName         = "readiness.sh"
	postStartHookEntryName          = "post-start.sh"
	preStopHookEntryName            = "pre-stop.sh"
	disableMaintenanceModeEntryName = "disable-maintenance-mode.sh"
	enableMaintenanceModeEntryName  = "enable-maintenance-mode.sh"
	clusterHealthCheckEntryName     = "cluster-health-check.sh"

	delayBetweenHooksCheckSeconds = "0.5"
	// checking cluster up to a certain timeout, as the health of the cluster may depend also on other nodes
	defaultClusterHealthCheckTimeoutSeconds = 300
)

var hooksHashAnnotationKey = redpandav1alpha1.GroupVersion.Group + "/hooks-hash"

var _ Resource = &HooksConfigMapResource{}

// HooksConfigMapResource manages the configmap containing lifecycle hook scripts
type HooksConfigMapResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster

	logger logr.Logger
}

// HooksConfigMapKey returns the key associated with the HooksConfigMapResource
func HooksConfigMapKey(
	pandaCluster *redpandav1alpha1.Cluster,
) types.NamespacedName {
	return types.NamespacedName{Name: utils.ResourceNameTrim(pandaCluster.Name, hooksSuffix), Namespace: pandaCluster.Namespace}
}

// Key returns the key associated with the HooksConfigMapResource
func (r *HooksConfigMapResource) Key() types.NamespacedName {
	return HooksConfigMapKey(r.pandaCluster)
}

// NewHooksConfigMap creates a HooksConfigMapResource
func NewHooksConfigMap(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *HooksConfigMapResource {
	return &HooksConfigMapResource{
		Client:       client,
		pandaCluster: pandaCluster,
		scheme:       scheme,
		logger:       logger.WithValues("Kind", configMapKind()),
	}
}

// Ensure will manage kubernetes v1.ConfigMap for redpanda.vectorized.io CR
func (r *HooksConfigMapResource) Ensure(ctx context.Context) error {
	obj, err := r.obj(ctx)
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}
	created, err := CreateIfNotExists(ctx, r, obj, r.logger)
	if err != nil || created {
		return err
	}

	var cm corev1.ConfigMap
	err = r.Get(ctx, r.Key(), &cm)
	if err != nil {
		return fmt.Errorf("error while fetching ConfigMap resource: %w", err)
	}
	_, err = Update(ctx, &cm, obj, r.Client, r.logger)
	if err != nil {
		return err
	}
	// Also force the pods to reload the config when updating
	return r.forceConfigMapReload(ctx, obj.(*corev1.ConfigMap))
}

// obj returns resource managed client.Object
func (r *HooksConfigMapResource) obj(
	_ context.Context,
) (k8sclient.Object, error) {
	hooksConfigMap := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    labels.ForCluster(r.pandaCluster),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		Data: map[string]string{
			postStartHookEntryName:          r.getPostStartHook(),
			disableMaintenanceModeEntryName: r.getPostStartCheck(),
			preStopHookEntryName:            r.getPreStopHook(),
			enableMaintenanceModeEntryName:  r.getPreStopCheck(),
			readinessProbeEntryName:         r.getReadinessProbe(),
			clusterHealthCheckEntryName:     r.getClusterHealthCheck(),
		},
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, &hooksConfigMap, r.scheme)
	if err != nil {
		return nil, err
	}

	return &hooksConfigMap, nil
}

func (r *HooksConfigMapResource) getPostStartHook() string {
	return fmt.Sprintf(`#!/bin/bash

        location=$(dirname $0)
        cd $location

        %s
        %s
`,
		r.getTimedLoopFragment(clusterHealthCheckEntryName, "check_health", delayBetweenHooksCheckSeconds, r.getHealthCheckTimeout()),
		r.getLoopFragment(disableMaintenanceModeEntryName, delayBetweenHooksCheckSeconds),
	)
}

func (r *HooksConfigMapResource) getPostStartCheck() string {
	if !r.isMaintenanceModeEnabled("post-start") {
		return r.getNoopCheckFunction("check")
	}

	curlCommand := r.composeCURLMaintenanceCommand(`-X DELETE --max-time 10 --silent -o /dev/null -w "%{http_code}"`, nil)
	return fmt.Sprintf(`#!/bin/bash
		
        check() {
          status=$(%s)
          if [ "${status:-}" != "200" ]; then
            return 1
          fi
        }
`, curlCommand)
}

func (r *HooksConfigMapResource) getPreStopHook() string {
	return fmt.Sprintf(`#!/bin/bash

        location=$(dirname $0)
        cd $location

		%s
`,
		r.getLoopFragment(enableMaintenanceModeEntryName, delayBetweenHooksCheckSeconds),
	)
}

func (r *HooksConfigMapResource) getPreStopCheck() string {
	if !r.isMaintenanceModeEnabled("pre-stop") {
		return r.getNoopCheckFunction("check")
	}

	curlCommand := r.composeCURLMaintenanceCommand(`-X PUT --max-time 10 --silent -o /dev/null -w "%{http_code}"`, nil)
	genericMaintenancePath := "/v1/maintenance"
	curlGetCommand := r.composeCURLMaintenanceCommand(`--max-time 10 --silent`, &genericMaintenancePath)
	return fmt.Sprintf(`#!/bin/bash
		
        check() {
          status=$(%s)
          if [ "${status:-}" != "200" ]; then
            return 1
          fi

          finished=$(%s | grep -o '\"finished\":[^,}]*' | grep -o '[^: ]*$')
          if [ "${finished:-}" != "true" ]; then
            return 1
          fi
        }
`, curlCommand, curlGetCommand)
}

func (r *HooksConfigMapResource) getClusterHealthCheck() string {
	fallbackNoOp := r.getNoopCheckFunction("check_health")
	if !r.pandaCluster.IsUsingClusterHealthCheck() {
		r.logger.Info("Health overview endpoint explicitly disabled")
		return fallbackNoOp
	}

	// Current version needs to support the health overview endpoint
	if !featuregates.HealthOverview(r.pandaCluster.Status.Version) {
		r.logger.Info("Disabling health overview endpoint as not supported")
		return fallbackNoOp
	}

	// Enable health check only if the cluster can potentially reach quorum
	if r.pandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterStableConditionType) != corev1.ConditionTrue {
		r.logger.Info("Disabling health overview endpoint while cluster is unstable")
		return fallbackNoOp
	}

	r.logger.Info("Health overview endpoint enabled")
	healthOverviewPath := "/v1/cluster/health_overview"
	curlCommand := r.composeCURLMaintenanceCommand(`--max-time 10 --silent`, &healthOverviewPath)
	return fmt.Sprintf(`#!/bin/bash
		
        check_health() {
          is_healthy=$(%s | grep -o '\"is_healthy\":[^,}]*' | grep -o '[^: ]*$')
          if [ "${is_healthy:-}" != "true" ]; then
            return 1
          fi
        }
`, curlCommand)
}

func (r *HooksConfigMapResource) getReadinessProbe() string {
	if !r.pandaCluster.IsUsingReadinessProbe() {
		r.logger.Info("Readiness probe explicitly disabled")
		return r.getNoopScript()
	}

	r.logger.Info("Readiness probe enabled")
	readinessEndpoint := "/v1/status/ready"
	curlStatusCommand := r.composeCURLMaintenanceCommand(`--max-time 10 --silent -o /dev/null -w "%{http_code}"`, &readinessEndpoint)
	curlGetCommand := r.composeCURLMaintenanceCommand(`--max-time 10 --silent`, &readinessEndpoint)
	return fmt.Sprintf(`#!/bin/bash

        status_code=$(%s)
        if [ "${status_code:-}" != "200" ]; then
          exit 1
        fi
        status=$(%s | grep -o '\"status\":\s*\"[^\"]*\"' | grep -o '[^: ]*$' | tr -d '\"')
        if [ "${status:-}" != "ready" ]; then
          exit 1
        fi
`, curlStatusCommand, curlGetCommand)
}

func (r *HooksConfigMapResource) isMaintenanceModeEnabled(
	hookType string,
) bool {
	// Only multi-replica clusters should use maintenance mode. See: https://github.com/redpanda-data/redpanda/issues/4338
	if r.pandaCluster.Spec.Replicas == nil || *r.pandaCluster.Spec.Replicas <= 1 {
		r.logger.Info(fmt.Sprintf("Maintenance mode %s hook disabled because of insufficient nodes", hookType))
		return false
	}
	// When upgrading from pre-v22 to v22, an error is returned until the new version reaches quorum and the maintenance mode feature is enabled
	if !featuregates.MaintenanceMode(r.pandaCluster.Status.Version) {
		r.logger.Info(fmt.Sprintf("Maintenance mode %s hook disabled as not supported by current version", hookType))
		return false
	}
	if !r.pandaCluster.IsUsingMaintenanceModeHooks() {
		r.logger.Info(fmt.Sprintf("Maintenance mode %s hook explicitly disabled", hookType))
		return false
	}
	r.logger.Info(fmt.Sprintf("Maintenance mode %s hook enabled", hookType))
	return true
}

func (r *HooksConfigMapResource) composeCURLMaintenanceCommand(
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
		prefixLen := len(r.pandaCluster.Name) + 1
		cmd += fmt.Sprintf("/v1/brokers/${POD_NAME:%d}/maintenance", prefixLen)
	} else {
		cmd += *urlOverwrite
	}
	return cmd
}

func (r *HooksConfigMapResource) getNoopScript() string {
	return `#!/bin/bash

        exit 0
`
}

func (r *HooksConfigMapResource) getNoopCheckFunction(name string) string {
	return fmt.Sprintf(`#!/bin/bash
		
		%s() {
          return 0
        }
`, name)
}

func (r *HooksConfigMapResource) getLoopFragment(source, delay string) string {
	return fmt.Sprintf(`
        source ./%s
        until check; do
          sleep %s
          source ./%s
        done
`, source, delay, source)
}

func (r *HooksConfigMapResource) getTimedLoopFragment(
	source, funcName, delay string, timeout int,
) string {
	return fmt.Sprintf(`
         start_time=$(date +%%s)
         source ./%s
         until %s; do
           sleep %s
           time=$(date +%%s)
           if [ $(( time - start_time )) -gt %d ]; then
             break
           fi
           source ./%s
         done
`, source, funcName, delay, timeout, source)
}

func (r *HooksConfigMapResource) getHealthCheckTimeout() int {
	if r.pandaCluster.Spec.RestartConfig != nil && r.pandaCluster.Spec.RestartConfig.HealthCheckTimeoutSeconds != nil {
		timeout := int(*r.pandaCluster.Spec.RestartConfig.HealthCheckTimeoutSeconds)
		if timeout < 0 {
			timeout = 0
		}
		return timeout
	}
	return defaultClusterHealthCheckTimeoutSeconds
}

// forceConfigMapReload changes an annotation on the pod to force the Kubelet wake up and update the configmap mount
func (r *HooksConfigMapResource) forceConfigMapReload(
	ctx context.Context, cm *corev1.ConfigMap,
) error {
	digest, err := r.computeHash(cm)
	if err != nil {
		return fmt.Errorf("could not compute digest for ConfigMap %s: %w", cm.Name, err)
	}

	var podList corev1.PodList
	err = r.List(ctx, &podList, &k8sclient.ListOptions{
		Namespace:     r.pandaCluster.Namespace,
		LabelSelector: labels.ForCluster(r.pandaCluster).AsClientSelector(),
	})
	if err != nil {
		return fmt.Errorf("unable to list redpanda pods: %w", err)
	}

	for i := range podList.Items {
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pod := &podList.Items[i]
			err = r.Get(ctx, k8sclient.ObjectKeyFromObject(pod), pod)
			if err != nil {
				return err
			}
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string, 1)
			}
			pod.Annotations[hooksHashAnnotationKey] = digest

			return r.Update(ctx, pod)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *HooksConfigMapResource) computeHash(
	cm *corev1.ConfigMap,
) (string, error) {
	elements := make([]string, 0, len(cm.Data))
	for k := range cm.Data {
		elements = append(elements, k)
	}
	sort.Strings(elements)
	digester := sha256.New()
	for _, k := range elements {
		_, err := digester.Write([]byte(cm.Data[k]))
		if err != nil {
			return "", err
		}
	}
	digest := digester.Sum(nil)
	return fmt.Sprintf("%x", digest), nil
}
