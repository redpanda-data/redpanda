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
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const requeueDuration = time.Second * 10

// runPartitionedUpdate handles image changes in the redpanda cluster CR by triggering
// a rolling update (using partitions) against the statefulset underneath the CR.
// The partitioned rolling update allows us to verify the ith pod in a custom manner
// before proceeding to the next pod.
//
// The process maintains an Upgrading bool status that is set to true once the
// CR and statefulset images differ. It is set back to false when all pods are
// verified to be updated.
//
// The steps are as follows: 1) check the Upgrading status or if the statefulset image
// version differs from that of the cluster CR; 2) if true, set the Upgrading status
// to true and modify the image in the sts spec, in-memory; 3) perform rolling update
// starting from the last pod to the first; 4) in each iteration apply the update
// on the pod and requeue until the pod is in ready state; 5) prior to a pod update
// verify the previously updated pod and requeue as necessary. Currently, the
// verification checks the pod has started listening in its Kafka API port and may be
// extended.
func (r *StatefulSetResource) runPartitionedUpdate(
	ctx context.Context, sts *appsv1.StatefulSet,
) error {
	newImage := r.pandaCluster.FullImageName()

	if err := r.updateUpgradingStatus(ctx, true); err != nil {
		return err
	}

	if r.pandaCluster.Status.Upgrading {
		r.logger.Info("Continuing cluster partitioned update", "cluster image", newImage)
	}

	podSpec := &sts.Spec.Template.Spec
	if err := r.modifyPodImage(podSpec, newImage); err != nil {
		return err
	}

	if err := r.partitionUpdateImage(ctx, sts, newImage); err != nil {
		return err
	}

	// Update is complete for all pods (and all are ready). Set upgrading status to false.
	if err := r.updateUpgradingStatus(ctx, false); err != nil {
		return err
	}

	return nil
}

// shouldUsePartitionedUpdate returns true if changes on the CR require partitioned update
func (r *StatefulSetResource) shouldUsePartitionedUpdate(
	sts *appsv1.StatefulSet,
) (bool, error) {
	upgrading := r.pandaCluster.Status.Upgrading

	rpContainer, err := findContainer(sts.Spec.Template.Spec.Containers, redpandaContainerName)
	if err != nil {
		return false, err
	}

	newImage := r.pandaCluster.FullImageName()
	return rpContainer.Image != newImage || upgrading, nil
}

func (r *StatefulSetResource) updateUpgradingStatus(
	ctx context.Context, upgrading bool,
) error {
	if !reflect.DeepEqual(upgrading, r.pandaCluster.Status.Upgrading) {
		r.pandaCluster.Status.Upgrading = upgrading
		if err := r.Status().Update(ctx, r.pandaCluster); err != nil {
			return err
		}
	}

	return nil
}

func (r *StatefulSetResource) partitionUpdateImage(
	ctx context.Context, sts *appsv1.StatefulSet, newImage string,
) error {
	replicas := *sts.Spec.Replicas

	// When a StatefulSet's partition number is set to `i`, only Pods with ordinal
	// greater than or equal to `i` will be updated.
	// https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#partitions
	for ordinal := replicas - 1; ordinal >= 0; ordinal-- {
		// Update() on statefulset has not been called yet in this run, however,
		// this could be a retry call in which case we skip the current partition.
		poderr := r.podImageIdenticalToClusterImage(ctx, sts, newImage, ordinal)
		if poderr == nil {
			r.logger.Info("Pod already updated, skip", "ordinal", ordinal)
			continue
		}

		// Continue only if error is due to Pod not ready, or unchanged image
		// as an attempt to fix the Pod.
		if !errors.Is(poderr, errContainerHasWrongImage) && !errors.Is(poderr, errPodNotReady) {
			return poderr
		}

		// Before continuing to update the ith Pod, verify that the previously updated
		// Pod (if any) has rejoined its groups after restarting, i.e., is ready for I/O.
		if err := r.ensureRedpandaGroupsReady(ctx, sts, replicas, ordinal+1); err != nil {
			return &RequeueAfterError{RequeueAfter: requeueDuration,
				Msg: fmt.Sprintf("redpanda on pod (ordinal: %d) not ready", ordinal)}
		}

		if err := r.rollingUpdatePartition(ctx, ordinal, sts); err != nil {
			return err
		}

		// Restarting the Pod takes enough time to warrant a requeue.
		return &RequeueAfterError{RequeueAfter: requeueDuration,
			Msg: fmt.Sprintf("wait for pod (ordinal: %d) to restart", ordinal)}
	}

	// Ensure 0th Pod is ready for I/O before completing the upgrade.
	if err := r.ensureRedpandaGroupsReady(ctx, sts, replicas, 0); err != nil {
		return &RequeueAfterError{RequeueAfter: requeueDuration,
			Msg: fmt.Sprintf("redpanda on pod (ordinal: %d) not ready", 0)}
	}

	return nil
}

// Ensures the Redpanda pod has rejoined its groups after restarting,
// i.e., is ready for I/O.
func (r *StatefulSetResource) ensureRedpandaGroupsReady(
	ctx context.Context, sts *appsv1.StatefulSet, replicas, ordinal int32,
) error {
	if replicas == 0 || ordinal == replicas {
		return nil
	}

	if len(r.pandaCluster.Spec.Configuration.KafkaAPI) == 0 {
		return nil // TODO
	}
	port := strconv.Itoa(r.pandaCluster.Spec.Configuration.KafkaAPI[0].Port) // TODO pick any internal port (need connectivity field)
	headlessServiceWithPort := fmt.Sprintf("%s:%s", r.serviceFQDN, port)

	addresses := []string{fmt.Sprintf("%s-%d.%s", sts.Name, ordinal, headlessServiceWithPort)}

	return r.queryRedpandaForTopicMembers(ctx, addresses, r.logger)
}

// Used as a temporary indicator that Redpanda is ready until a health
// endpoint is introduced or logic is added here that goes through all topics
// metadata.
func (r *StatefulSetResource) queryRedpandaForTopicMembers(
	ctx context.Context, addresses []string, logger logr.Logger,
) error {
	logger.Info("Connect to Redpanda broker", "broker", addresses)

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_4_0_0
	conf.ClientID = "operator"
	conf.Admin.Timeout = time.Second

	// TODO right now we support TLS only on one listener so if external
	// connectivity is enabled, TLS is enabled only on external listener. This
	// will be fixed by https://github.com/vectorizedio/redpanda/issues/1084
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener != nil && !tlsListener.External.Enabled {
		tlsConfig := tls.Config{MinVersion: tls.VersionTLS12} // TLS12 is min version allowed by gosec.
		// For simplicity, we skip broker verification until per-listener
		// TLS is available in Redpanda. This client calls the internal listener.
		tlsConfig.InsecureSkipVerify = true

		if err := r.populateTLSConfigCert(ctx, &tlsConfig); err != nil {
			return err
		}

		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = &tlsConfig
	}

	consumer, err := sarama.NewConsumer(addresses, conf)
	if err != nil {
		logger.Error(err, "Error while creating consumer")
		return err
	}

	return consumer.Close()
}

// Populates crypto/TLS configuration for certificate used by the operator
// during its client authentication.
func (r *StatefulSetResource) populateTLSConfigCert(
	ctx context.Context, tlsConfig *tls.Config,
) error {
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener == nil || !tlsListener.TLS.RequireClientAuth {
		return nil
	}

	var certSecret corev1.Secret
	err := r.Get(ctx, r.internalClientCertSecretKey, &certSecret)
	if err != nil {
		return err
	}

	cert, err := tls.X509KeyPair(certSecret.Data[corev1.TLSCertKey], certSecret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return err
	}

	tlsConfig.Certificates = []tls.Certificate{cert}

	return nil
}

func (r *StatefulSetResource) podImageIdenticalToClusterImage(
	ctx context.Context, sts *appsv1.StatefulSet, newImage string, ordinal int32,
) error {
	podName := fmt.Sprintf("%s-%d", sts.Name, ordinal)

	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: sts.Namespace}, &pod); err != nil {
		return err
	}

	container, err := findContainer(pod.Spec.Containers, redpandaContainerName)
	if err != nil {
		return err
	}

	if container.Image != newImage {
		r.logger.Info("Container image not updated to cluster image", "pod", pod.Name,
			"container", container.Name, "container image", container.Image, "cluster image", newImage)
		return containerHasWrongImageError(podName, container.Name, container.Image, newImage)
	}

	if !podIsReady(&pod) {
		r.logger.Info("Pod not ready yet", "pod", pod.Name)
		return podNotReadyError(pod.Name)
	}

	r.logger.Info("Pod is ready", "pod", pod.Name, "container image", container.Image,
		"cluster image", newImage)

	return nil
}

func (r *StatefulSetResource) rollingUpdatePartition(
	ctx context.Context, ordinal int32, sts *appsv1.StatefulSet,
) error {
	r.logger.Info("Call update on statefulset", "ordinal", ordinal)

	modified, err := r.obj()
	if err != nil {
		return err
	}
	modifiedSts := modified.(*appsv1.StatefulSet)
	modifiedSts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{
		Partition: &ordinal,
	}
	if err := Update(ctx, sts, modifiedSts, r.Client, r.logger); err != nil {
		return fmt.Errorf("failed to update StatefulSet (ordinal %d): %w", ordinal, err)
	}

	return nil
}

func (r *StatefulSetResource) modifyPodImage(
	stsSpec *corev1.PodSpec, newImage string,
) error {
	return modifyContainerImage(stsSpec.Containers, redpandaContainerName, newImage)
}

func modifyContainerImage(
	containers []corev1.Container, containerName, newImage string,
) error {
	container, err := findContainer(containers, containerName)
	if err != nil {
		return err
	}

	container.Image = newImage

	return nil
}

func findContainer(
	containers []corev1.Container, container string,
) (*corev1.Container, error) {
	for i := range containers {
		if containers[i].Name == container {
			return &containers[i], nil
		}
	}

	return nil, containerNotFoundError(container)
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

var errContainerHasWrongImage = errors.New("container has wrong image")

func containerHasWrongImageError(
	podName, containerName, currentImage, expectedImage string,
) error {
	return fmt.Errorf("containerHasWrongImage %w : pod: %s; container: %s, image: %s; expected: %s",
		errContainerHasWrongImage, podName, containerName, currentImage, expectedImage)
}

var errContainerNotFound = errors.New("container not found")

func containerNotFoundError(container string) error {
	return fmt.Errorf("containerNotFound %w : container %s",
		errContainerNotFound, container)
}

var errPodNotReady = errors.New("pod not in ready state")

func podNotReadyError(pod string) error {
	return fmt.Errorf("podNotReady %w : pod: %s", errPodNotReady, pod)
}
