// Copyright 2021 Vectorized, Inc.
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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// RequeueDuration is the time controller should
	// requeue resource reconciliation.
	RequeueDuration = time.Second * 10
	adminAPITimeout = time.Millisecond * 100
)

var errRedpandaNotReady = errors.New("redpanda not ready")

// runUpdate handles image changes and additional storage in the redpanda cluster
// CR by removing statefulset with orphans Pods. The stateful set is then recreated
// and all Pods are restarted accordingly to the ordinal number.
//
// The process maintains an Upgrading bool status that is set to true once the
// generated stateful differentiate from the actual state. It is set back to
// false when all pods are verified.
//
// The steps are as follows: 1) check the Upgrading status or if the statefulset
// differentiate from the current stored statefulset definition 2) if true,
// set the Upgrading status to true and remove statefulset with the orphan Pods
// 3) perform rolling update like removing Pods accordingly to theirs ordinal
// number 4) requeue until the pod is in ready state 5) prior to a pod update
// verify the previously updated pod and requeue as necessary. Currently, the
// verification checks the pod has started listening in its http Admin API port and may be
// extended.
func (r *StatefulSetResource) runUpdate(
	ctx context.Context, current, modified *appsv1.StatefulSet,
) error {
	update, err := shouldUpdate(r.pandaCluster.Status.Upgrading, current, modified)
	if err != nil {
		return fmt.Errorf("unable to determine the update procedure: %w", err)
	}

	if !update {
		return nil
	}

	if err = r.updateUpgradingStatus(ctx, true); err != nil {
		return fmt.Errorf("unable to turn on upgrading status in cluster custom resource: %w", err)
	}
	if err = r.updateStatefulSet(ctx, current, modified); err != nil {
		return err
	}

	if err = r.rollingUpdate(ctx, &modified.Spec.Template); err != nil {
		return err
	}

	// Update is complete for all pods (and all are ready). Set upgrading status to false.
	if err = r.updateUpgradingStatus(ctx, false); err != nil {
		return fmt.Errorf("unable to turn off upgrading status in cluster custom resource: %w", err)
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

	opts := []patch.CalculateOption{
		patch.IgnoreStatusFields(),
		ignoreKubernetesTokenVolumeMounts(),
		ignoreDefaultToleration(),
		ignoreExistingVolumes(volumes),
	}

	for i := range podList.Items {
		pod := podList.Items[i]

		patchResult, err := patch.DefaultPatchMaker.Calculate(&pod, &artificialPod, opts...)
		if err != nil {
			return err
		}

		if !patchResult.IsEmpty() {
			r.logger.Info("Changes in Pod definition other than activeDeadlineSeconds, configurator and Redpanda container name. Deleting pod",
				"pod-name", pod.Name,
				"patch", patchResult.Patch)
			if err = r.Delete(ctx, &pod); err != nil {
				return fmt.Errorf("unable to remove Redpanda pod: %w", err)
			}
			return &RequeueAfterError{RequeueAfter: RequeueDuration, Msg: "wait for pod restart"}
		}

		if !podIsReady(&pod) {
			return &RequeueAfterError{RequeueAfter: RequeueDuration,
				Msg: fmt.Sprintf("wait for %s pod to become ready", pod.Name)}
		}

		headlessServiceWithPort := fmt.Sprintf("%s:%d", r.serviceFQDN,
			r.pandaCluster.AdminAPIInternal().Port)

		adminURL := url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%s.%s", pod.Name, headlessServiceWithPort),
			Path:   "v1/status/ready",
		}

		r.logger.Info("Verify that Ready endpoint returns HTTP status OK")
		if err = r.queryRedpandaStatus(ctx, &adminURL); err != nil {
			return fmt.Errorf("unable to query Redpanda ready status: %w", err)
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
func shouldUpdate(
	isUpgrading bool, current, modified *appsv1.StatefulSet,
) (bool, error) {
	prepareResourceForPatch(current, modified)
	opts := []patch.CalculateOption{
		patch.IgnoreStatusFields(),
		patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus(),
	}
	patchResult, err := patch.DefaultPatchMaker.Calculate(current, modified, opts...)
	if err != nil {
		return false, err
	}

	return !patchResult.IsEmpty() || isUpgrading, nil
}

func (r *StatefulSetResource) updateUpgradingStatus(
	ctx context.Context, upgrading bool,
) error {
	if !reflect.DeepEqual(upgrading, r.pandaCluster.Status.Upgrading) {
		r.pandaCluster.Status.Upgrading = upgrading
		r.logger.Info("Status updated",
			"status", upgrading,
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

// Temporarily using the status/ready endpoint until we have a specific one for upgrading.
func (r *StatefulSetResource) queryRedpandaStatus(
	ctx context.Context, adminURL *url.URL,
) error {
	client := &http.Client{Timeout: adminAPITimeout}

	// TODO right now we support TLS only on one listener so if external
	// connectivity is enabled, TLS is enabled only on external listener. This
	// will be fixed by https://github.com/vectorizedio/redpanda/issues/1084
	if r.pandaCluster.AdminAPITLS() != nil &&
		r.pandaCluster.AdminAPIExternal() == nil {
		tlsConfig := tls.Config{MinVersion: tls.VersionTLS12} // TLS12 is min version allowed by gosec.

		if err := r.populateTLSConfigCert(ctx, &tlsConfig); err != nil {
			return err
		}

		client.Transport = &http.Transport{
			TLSClientConfig: &tlsConfig,
		}
		adminURL.Scheme = "https"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, adminURL.String(), nil)
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

// Populates crypto/TLS configuration for certificate used by the operator
// during its client authentication.
func (r *StatefulSetResource) populateTLSConfigCert(
	ctx context.Context, tlsConfig *tls.Config,
) error {
	var nodeCertSecret corev1.Secret
	err := r.Get(ctx, r.adminAPINodeCertSecretKey, &nodeCertSecret)
	if err != nil {
		return err
	}

	// Add root CA
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(nodeCertSecret.Data[cmetav1.TLSCAKey])
	tlsConfig.RootCAs = caCertPool

	if r.pandaCluster.AdminAPITLS() != nil && r.pandaCluster.AdminAPITLS().TLS.RequireClientAuth {
		var clientCertSecret corev1.Secret
		err := r.Get(ctx, r.adminAPIClientCertSecretKey, &clientCertSecret)
		if err != nil {
			return err
		}
		cert, err := tls.X509KeyPair(clientCertSecret.Data[corev1.TLSCertKey], clientCertSecret.Data[corev1.TLSPrivateKeyKey])
		if err != nil {
			return err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return nil
}

func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady &&
			c.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

// RequeueAfterError error carrying the time after which to requeue.
type RequeueAfterError struct {
	RequeueAfter time.Duration
	Msg          string
}

func (e *RequeueAfterError) Error() string {
	return fmt.Sprintf("RequeueAfterError %s", e.Msg)
}
