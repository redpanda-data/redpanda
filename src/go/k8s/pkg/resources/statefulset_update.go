// Copyright 2021 Redpanda Data, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/cisco-open/k8s-objectmatcher/patch"
	"github.com/prometheus/common/expfmt"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// RequeueDuration is the time controller should
	// requeue resource reconciliation.
	RequeueDuration        = time.Second * 10
	defaultAdminAPITimeout = time.Second * 2
)

var (
	errRedpandaNotReady         = errors.New("redpanda not ready")
	errUnderReplicatedPartition = errors.New("partition under replicated")
)

// runUpdate handles image changes and additional storage in the redpanda cluster
// CR by removing statefulset with orphans Pods. The stateful set is then recreated
// and all Pods are restarted accordingly to the ordinal number.
//
// The process maintains an Restarting bool status that is set to true once the
// generated stateful differentiate from the actual state. It is set back to
// false when all pods are verified.
//
// The steps are as follows: 1) check the Restarting status or if the statefulset
// differentiate from the current stored statefulset definition 2) if true,
// set the Restarting status to true and remove statefulset with the orphan Pods
// 3) perform rolling update like removing Pods accordingly to theirs ordinal
// number 4) requeue until the pod is in ready state 5) prior to a pod update
// verify the previously updated pod and requeue as necessary. Currently, the
// verification checks the pod has started listening in its http Admin API port and may be
// extended.
func (r *StatefulSetResource) runUpdate(
	ctx context.Context, current, modified *appsv1.StatefulSet,
) error {
	// Keep existing central config hash annotation during standard reconciliation
	if ann, ok := current.Spec.Template.Annotations[CentralizedConfigurationHashAnnotationKey]; ok {
		if modified.Spec.Template.Annotations == nil {
			modified.Spec.Template.Annotations = make(map[string]string)
		}
		modified.Spec.Template.Annotations[CentralizedConfigurationHashAnnotationKey] = ann
	}

	update, err := r.shouldUpdate(r.pandaCluster.Status.IsRestarting(), current, modified)
	if err != nil {
		return fmt.Errorf("unable to determine the update procedure: %w", err)
	}

	if !update {
		return nil
	}

	if err = r.updateRestartingStatus(ctx, true); err != nil {
		return fmt.Errorf("unable to turn on restarting status in cluster custom resource: %w", err)
	}

	if err = r.updateStatefulSet(ctx, current, modified); err != nil {
		return err
	}

	if err = r.isClusterHealthy(ctx); err != nil {
		return err
	}

	if err = r.rollingUpdate(ctx, &modified.Spec.Template); err != nil {
		return err
	}

	// Update is complete for all pods (and all are ready). Set restarting status to false.
	if err = r.updateRestartingStatus(ctx, false); err != nil {
		return fmt.Errorf("unable to turn off restarting status in cluster custom resource: %w", err)
	}

	return nil
}

func (r *StatefulSetResource) isClusterHealthy(ctx context.Context) error {
	if !featuregates.ClusterHealth(r.pandaCluster.Status.Version) {
		r.logger.V(debugLogLevel).Info("Cluster health endpoint is not available", "version", r.pandaCluster.Spec.Version)
		return nil
	}

	adminAPIClient, err := r.getAdminAPIClient(ctx)
	if err != nil {
		return fmt.Errorf("creating admin API client: %w", err)
	}

	health, err := adminAPIClient.GetHealthOverview(ctx)
	if err != nil {
		return fmt.Errorf("getting cluster health overview: %w", err)
	}

	restarting := "not restarting"
	if r.pandaCluster.Status.IsRestarting() {
		restarting = "restarting"
	}

	if !health.IsHealthy {
		return &RequeueAfterError{
			RequeueAfter: RequeueDuration,
			Msg:          fmt.Sprintf("wait for cluster to become healthy (cluster %s)", restarting),
		}
	}

	return nil
}

func (r *StatefulSetResource) rollingUpdate(
	ctx context.Context, template *corev1.PodTemplateSpec,
) error {
	var podList corev1.PodList
	err := r.List(ctx, &podList, &k8sclient.ListOptions{
		Namespace:     r.pandaCluster.Namespace,
		LabelSelector: labels.ForCluster(r.pandaCluster).AsClientSelector(),
	})
	if err != nil {
		return fmt.Errorf("unable to list panda pods: %w", err)
	}

	sort.Slice(podList.Items, func(i, j int) bool {
		return podList.Items[i].Name < podList.Items[j].Name
	})

	var artificialPod corev1.Pod
	artificialPod.Annotations = template.Annotations
	artificialPod.Spec = template.Spec

	volumes := make(map[string]interface{})
	for i := range template.Spec.Volumes {
		vol := template.Spec.Volumes[i]
		volumes[vol.Name] = new(interface{})
	}

	for i := range podList.Items {
		pod := podList.Items[i]

		if err = r.podEviction(ctx, &pod, &artificialPod, volumes); err != nil {
			return err
		}

		if !utils.IsPodReady(&pod) {
			return &RequeueAfterError{
				RequeueAfter: RequeueDuration,
				Msg:          fmt.Sprintf("wait for %s pod to become ready", pod.Name),
			}
		}

		headlessServiceWithPort := fmt.Sprintf("%s:%d", r.serviceFQDN,
			r.pandaCluster.AdminAPIInternal().Port)

		adminURL := url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%s.%s", pod.Name, headlessServiceWithPort),
			Path:   "v1/status/ready",
		}

		r.logger.Info("Verify that Ready endpoint returns HTTP status OK", "pod-name", pod.Name)
		if err = r.queryRedpandaStatus(ctx, &adminURL); err != nil {
			return fmt.Errorf("unable to query Redpanda ready status: %w", err)
		}

		adminURL.Path = "metrics"

		params := url.Values{}
		if featuregates.MetricsQueryParamName(r.pandaCluster.Spec.Version) {
			params.Add("__name__", "cluster_partition_under_replicated_replicas*")
		} else {
			params.Add("name", "cluster_partition_under_replicated_replicas*")
		}
		adminURL.RawQuery = params.Encode()

		if err = r.evaluateUnderReplicatedPartitions(ctx, &adminURL); err != nil {
			return &RequeueAfterError{
				RequeueAfter: RequeueDuration,
				Msg:          fmt.Sprintf("broker reported under replicated partitions: %v", err),
			}
		}
	}

	return nil
}

func (r *StatefulSetResource) podEviction(ctx context.Context, pod, artificialPod *corev1.Pod, newVolumes map[string]interface{}) error {
	opts := []patch.CalculateOption{
		patch.IgnoreStatusFields(),
		ignoreKubernetesTokenVolumeMounts(),
		ignoreDefaultToleration(),
		ignoreExistingVolumes(newVolumes),
	}

	patchResult, err := patch.NewPatchMaker(patch.NewAnnotator(redpandaAnnotatorKey), &patch.K8sStrategicMergePatcher{}, &patch.BaseJSONMergePatcher{}).Calculate(pod, artificialPod, opts...)
	if err != nil {
		return err
	}

	var ordinal int64
	ordinal, err = utils.GetPodOrdinal(pod.Name, r.pandaCluster.Name)
	if err != nil {
		return fmt.Errorf("cluster %s: cannot convert pod name (%s) to ordinal: %w", r.pandaCluster.Name, pod.Name, err)
	}

	if patchResult.IsEmpty() {
		if err = r.checkMaintenanceMode(ctx, int32(ordinal)); err != nil {
			return &RequeueAfterError{
				RequeueAfter: RequeueDuration,
				Msg:          fmt.Sprintf("checking maintenance node (%s): %v", pod.Name, err),
			}
		}
		return nil
	}

	if *r.pandaCluster.Spec.Replicas > 1 {
		r.logger.Info("Put broker into maintenance mode",
			"pod-name", pod.Name,
			"patch", patchResult.Patch)
		if err = r.putInMaintenanceMode(ctx, int32(ordinal)); err != nil {
			// As maintenance mode can not be easily watched using controller runtime the requeue error
			// is always returned. That way a rolling update will not finish when operator waits for
			// maintenance mode finished.
			return &RequeueAfterError{
				RequeueAfter: RequeueDuration,
				Msg:          fmt.Sprintf("putting node (%s) into maintenance mode: %v", pod.Name, err),
			}
		}
	}

	r.logger.Info("Changes in Pod definition other than activeDeadlineSeconds, configurator and Redpanda container name. Deleting pod",
		"pod-name", pod.Name,
		"patch", patchResult.Patch)

	if err = r.Delete(ctx, pod); err != nil {
		return fmt.Errorf("unable to remove Redpanda pod: %w", err)
	}

	return &RequeueAfterError{RequeueAfter: RequeueDuration, Msg: "wait for pod restart"}
}

var (
	ErrMaintenanceNotFinished = errors.New("maintenance mode is not finished")
	ErrMaintenanceMissing     = errors.New("maintenance definition not returned")
)

func (r *StatefulSetResource) putInMaintenanceMode(ctx context.Context, ordinal int32) error {
	adminAPIClient, err := r.getAdminAPIClient(ctx, ordinal)
	if err != nil {
		return fmt.Errorf("creating admin API client: %w", err)
	}

	nodeConf, err := adminAPIClient.GetNodeConfig(ctx)
	if err != nil {
		return fmt.Errorf("getting node config: %w", err)
	}

	err = adminAPIClient.EnableMaintenanceMode(ctx, nodeConf.NodeID)
	if err != nil {
		return fmt.Errorf("enabling maintenance mode: %w", err)
	}

	br, err := adminAPIClient.Broker(ctx, nodeConf.NodeID)
	if err != nil {
		return fmt.Errorf("getting broker infromations: %w", err)
	}

	if br.Maintenance == nil {
		return ErrMaintenanceMissing
	}

	if !br.Maintenance.Finished {
		return fmt.Errorf("draining (%t), errors (%t), failed (%d), finished (%t): %w", br.Maintenance.Draining, br.Maintenance.Errors, br.Maintenance.Failed, br.Maintenance.Finished, ErrMaintenanceNotFinished)
	}

	return nil
}

func (r *StatefulSetResource) checkMaintenanceMode(ctx context.Context, ordinal int32) error {
	if *r.pandaCluster.Spec.Replicas <= 1 {
		return nil
	}

	adminAPIClient, err := r.getAdminAPIClient(ctx, ordinal)
	if err != nil {
		return fmt.Errorf("creating admin API client: %w", err)
	}

	nodeConf, err := adminAPIClient.GetNodeConfig(ctx)
	if err != nil {
		return fmt.Errorf("getting node config: %w", err)
	}

	br, err := adminAPIClient.Broker(ctx, nodeConf.NodeID)
	if err != nil {
		return fmt.Errorf("getting broker info: %w", err)
	}

	if br.Maintenance != nil && br.Maintenance.Draining {
		r.logger.Info("Disable broker maintenance mode as patch is empty",
			"pod-ordinal", ordinal)
		err = adminAPIClient.DisableMaintenanceMode(ctx, nodeConf.NodeID)
		if err != nil {
			return fmt.Errorf("disabling maintenance mode: %w", err)
		}
	}

	return nil
}

func (r *StatefulSetResource) updateStatefulSet(
	ctx context.Context,
	current *appsv1.StatefulSet,
	modified *appsv1.StatefulSet,
) error {
	_, err := Update(ctx, current, modified, r.Client, r.logger)
	if err != nil && strings.Contains(err.Error(), "spec: Forbidden: updates to statefulset spec for fields other than") {
		// REF: https://github.com/kubernetes/kubernetes/issues/69041#issuecomment-723757166
		// https://www.giffgaff.io/tech/resizing-statefulset-persistent-volumes-with-zero-downtime
		// in-place rolling update of a pod - https://github.com/kubernetes/kubernetes/issues/9043
		orphan := metav1.DeletePropagationOrphan
		err = r.Client.Delete(ctx, current, &k8sclient.DeleteOptions{
			PropagationPolicy: &orphan,
		})
		if err != nil {
			return fmt.Errorf("unable to delete statefulset using orphan propagation policy: %w", err)
		}
		return &RequeueAfterError{RequeueAfter: RequeueDuration, Msg: "wait for sts to be deleted"}
	}
	if err != nil {
		return fmt.Errorf("unable to update statefulset: %w", err)
	}
	return nil
}

// shouldUpdate returns true if changes on the CR require update
func (r *StatefulSetResource) shouldUpdate(
	isRestarting bool, current, modified *appsv1.StatefulSet,
) (bool, error) {
	prepareResourceForPatch(current, modified)
	opts := []patch.CalculateOption{
		patch.IgnoreStatusFields(),
		patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus(),
		utils.IgnoreAnnotation(redpandaAnnotatorKey),
		utils.IgnoreAnnotation(CentralizedConfigurationHashAnnotationKey),
	}
	patchResult, err := patch.NewPatchMaker(patch.NewAnnotator(redpandaAnnotatorKey), &patch.K8sStrategicMergePatcher{}, &patch.BaseJSONMergePatcher{}).Calculate(current, modified, opts...)
	if err != nil {
		return false, err
	}
	return !patchResult.IsEmpty() || isRestarting, nil
}

func (r *StatefulSetResource) updateRestartingStatus(
	ctx context.Context, restarting bool,
) error {
	if !reflect.DeepEqual(restarting, r.pandaCluster.Status.IsRestarting()) {
		r.pandaCluster.Status.SetRestarting(restarting)
		r.logger.Info("Status updated",
			"restarting", restarting,
			"resource name", r.pandaCluster.Name)
		if err := r.Status().Update(ctx, r.pandaCluster); err != nil {
			return err
		}
	}

	return nil
}

func ignoreExistingVolumes(
	volumes map[string]interface{},
) patch.CalculateOption {
	return func(current, modified []byte) ([]byte, []byte, error) {
		current, err := deleteExistingVolumes(current, volumes)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete configurator init container field from current byte sequence: %w", err)
		}

		modified, err = deleteExistingVolumes(modified, volumes)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete configurator init container field from modified byte sequence: %w", err)
		}

		return current, modified, nil
	}
}

func deleteExistingVolumes(
	obj []byte, volumes map[string]interface{},
) ([]byte, error) {
	var pod corev1.Pod
	err := json.Unmarshal(obj, &pod)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal byte sequence: %w", err)
	}

	var newVolumes []corev1.Volume
	for i := range pod.Spec.Volumes {
		_, ok := volumes[pod.Spec.Volumes[i].Name]
		if !ok {
			newVolumes = append(newVolumes, pod.Spec.Volumes[i])
		}
	}
	pod.Spec.Volumes = newVolumes

	obj, err = json.Marshal(pod)
	if err != nil {
		return nil, fmt.Errorf("could not marshal byte sequence: %w", err)
	}

	return obj, nil
}

func ignoreDefaultToleration() patch.CalculateOption {
	return func(current, modified []byte) ([]byte, []byte, error) {
		current, err := deleteDefaultToleration(current)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete configurator init container field from current byte sequence: %w", err)
		}

		modified, err = deleteDefaultToleration(modified)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete configurator init container field from modified byte sequence: %w", err)
		}

		return current, modified, nil
	}
}

func deleteDefaultToleration(obj []byte) ([]byte, error) {
	var pod corev1.Pod
	err := json.Unmarshal(obj, &pod)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal byte sequence: %w", err)
	}

	var newToleration []corev1.Toleration
	tolerations := pod.Spec.Tolerations
	for i := range tolerations {
		switch tolerations[i].Key {
		case "node.kubernetes.io/not-ready":
		case "node.kubernetes.io/unreachable":
			continue
		default:
			newToleration = append(newToleration, tolerations[i])
		}
	}
	pod.Spec.Tolerations = newToleration

	obj, err = json.Marshal(pod)
	if err != nil {
		return nil, fmt.Errorf("could not marshal byte sequence: %w", err)
	}

	return obj, nil
}

func ignoreKubernetesTokenVolumeMounts() patch.CalculateOption {
	return func(current, modified []byte) ([]byte, []byte, error) {
		current, err := deleteKubernetesTokenVolumeMounts(current)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete configurator init container field from current byte sequence: %w", err)
		}

		modified, err = deleteKubernetesTokenVolumeMounts(modified)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete configurator init container field from modified byte sequence: %w", err)
		}

		return current, modified, nil
	}
}

func deleteKubernetesTokenVolumeMounts(obj []byte) ([]byte, error) {
	var pod corev1.Pod
	err := json.Unmarshal(obj, &pod)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal byte sequence: %w", err)
	}

	tokenVolumeName := fmt.Sprintf("%s-token-", pod.Name)
	containers := pod.Spec.Containers
	for i := range containers {
		c := containers[i]
		for j := range c.VolumeMounts {
			vol := c.VolumeMounts[j]
			if strings.HasPrefix(vol.Name, tokenVolumeName) {
				c.VolumeMounts = append(c.VolumeMounts[:j], c.VolumeMounts[j+1:]...)
			}
		}
	}

	initContainers := pod.Spec.InitContainers
	for i := range initContainers {
		ic := initContainers[i]
		for j := range ic.VolumeMounts {
			vol := ic.VolumeMounts[j]
			if strings.HasPrefix(vol.Name, tokenVolumeName) {
				ic.VolumeMounts = append(ic.VolumeMounts[:j], ic.VolumeMounts[j+1:]...)
			}
		}
	}

	obj, err = json.Marshal(pod)
	if err != nil {
		return nil, fmt.Errorf("could not marshal byte sequence: %w", err)
	}

	return obj, nil
}

// Temporarily using the status/ready endpoint until we have a specific one for restarting.
func (r *StatefulSetResource) queryRedpandaStatus(
	ctx context.Context, adminURL *url.URL,
) error {
	client := &http.Client{Timeout: defaultAdminAPITimeout}

	// TODO right now we support TLS only on one listener so if external
	// connectivity is enabled, TLS is enabled only on external listener. This
	// will be fixed by https://github.com/redpanda-data/redpanda/issues/1084
	if r.pandaCluster.AdminAPITLS() != nil &&
		r.pandaCluster.AdminAPIExternal() == nil {
		tlsConfig, err := r.adminTLSConfigProvider.GetTLSConfig(ctx, r)
		if err != nil {
			return err
		}

		client.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		adminURL.Scheme = "https"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, adminURL.String(), http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errRedpandaNotReady
	}

	return nil
}

// Temporarily using the status/ready endpoint until we have a specific one for restarting.
func (r *StatefulSetResource) evaluateUnderReplicatedPartitions(
	ctx context.Context, adminURL *url.URL,
) error {
	client := &http.Client{Timeout: r.metricsTimeout}

	// TODO right now we support TLS only on one listener so if external
	// connectivity is enabled, TLS is enabled only on external listener. This
	// will be fixed by https://github.com/redpanda-data/redpanda/issues/1084
	if r.pandaCluster.AdminAPITLS() != nil &&
		r.pandaCluster.AdminAPIExternal() == nil {
		tlsConfig, err := r.adminTLSConfigProvider.GetTLSConfig(ctx, r)
		if err != nil {
			return err
		}

		client.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		adminURL.Scheme = "https"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, adminURL.String(), http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			r.logger.Error(err, "error closing connection to Redpanda admin API")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("getting broker metrics (%s): %w", adminURL.String(), errRedpandaNotReady)
	}

	var parser expfmt.TextParser
	metrics, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return err
	}

	for name, metricFamily := range metrics {
		if name != "vectorized_cluster_partition_under_replicated_replicas" {
			continue
		}

		for _, m := range metricFamily.Metric {
			if m == nil {
				continue
			}
			if m.Gauge == nil {
				r.logger.Info("cluster_partition_under_replicated_replicas metric does not have value", "labels", m.Label)
				continue
			}

			var namespace, partition, shard, topic string
			for _, l := range m.Label {
				switch *l.Name {
				case "namespace":
					namespace = *l.Value
				case "partition":
					partition = *l.Value
				case "shard":
					shard = *l.Value
				case "topic":
					topic = *l.Value
				}
			}

			if r.pandaCluster.Spec.RestartConfig != nil && *m.Gauge.Value > float64(r.pandaCluster.Spec.RestartConfig.UnderReplicatedPartitionThreshold) {
				return fmt.Errorf("in topic (%s), partition (%s), shard (%s), namespace (%s): %w", topic, partition, shard, namespace, errUnderReplicatedPartition)
			}
		}
	}

	return nil
}

// RequeueAfterError error carrying the time after which to requeue.
type RequeueAfterError struct {
	RequeueAfter time.Duration
	Msg          string
}

func (e *RequeueAfterError) Error() string {
	return fmt.Sprintf("RequeueAfterError %s", e.Msg)
}

func (e *RequeueAfterError) Is(target error) bool {
	return e.Error() == target.Error()
}

// RequeueError error to requeue using default retry backoff.
type RequeueError struct {
	Msg string
}

func (e *RequeueError) Error() string {
	return fmt.Sprintf("RequeueError %s", e.Msg)
}
