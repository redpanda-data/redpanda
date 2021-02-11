// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package resources contains reconciliation logic for redpanda.vectorized.io CRD
package resources

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const requeueDuration = time.Second * 10

// updateStsImage handles image changes in the redpanda cluster CR by triggering
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
func (r *StatefulSetResource) updateStsImage(
	ctx context.Context, sts *appsv1.StatefulSet,
) error {
	upgrading := r.pandaCluster.Status.Upgrading

	rpContainer, err := findContainer(sts.Spec.Template.Spec.Containers, redpandaContainerName)
	if err != nil {
		return err
	}

	newImage := fmt.Sprintf("%s:%s", r.pandaCluster.Spec.Image, r.pandaCluster.Spec.Version)
	if rpContainer.Image == newImage && !upgrading {
		return nil
	}

	if rpContainer.Image != newImage {
		r.logger.Info("Starting cluster image update", "cluster image", newImage, "container image", rpContainer.Image)

		// Mark cluster as being upgraded.
		if err := r.updateUpgradingStatus(ctx, true); err != nil {
			return err
		}
	}

	if upgrading {
		r.logger.Info("Continuing cluster image update", "cluster image", newImage)
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
		if err := r.ensureRedpandaGroupsReady(sts, replicas, ordinal+1); err != nil {
			return &NeedToReconcileError{RequeueAfter: requeueDuration,
				Msg:	fmt.Sprintf("redpanda on pod (ordinal: %d) not ready", ordinal)}
		}

		if err := r.rollingUpdatePartition(ctx, sts, ordinal); err != nil {
			return err
		}

		// Restarting the Pod takes enough time to warrant a requeue.
		return &NeedToReconcileError{RequeueAfter: requeueDuration,
			Msg:	fmt.Sprintf("wait for pod (ordinal: %d) to restart", ordinal)}
	}

	// Ensure 0th Pod is ready for I/O before completing the upgrade.
	if err := r.ensureRedpandaGroupsReady(sts, replicas, 0); err != nil {
		return &NeedToReconcileError{RequeueAfter: requeueDuration,
			Msg:	fmt.Sprintf("redpanda on pod (ordinal: %d) not ready", 0)}
	}

	return nil
}

// Ensures the Redpanda pod has rejoined its groups after restarting,
// i.e., is ready for I/O.
func (r *StatefulSetResource) ensureRedpandaGroupsReady(
	sts *appsv1.StatefulSet, replicas, ordinal int32,
) error {
	if replicas == 0 || ordinal == replicas {
		return nil
	}

	headlessServiceWithPort := fmt.Sprintf("%s:%d", r.svc.HeadlessServiceFQDN(),
		r.pandaCluster.Spec.Configuration.KafkaAPI.Port)

	addresses := []string{fmt.Sprintf("%s-%d.%s", sts.Name, ordinal, headlessServiceWithPort)}

	return queryRedpandaForTopicMembers(addresses, r.logger)
}

// Used as a temporary indicator that Redpanda is ready until a health
// endpoint is introduced or logic is added here that goes through all topics
// metadata.
func queryRedpandaForTopicMembers(
	addresses []string, logger logr.Logger,
) error {
	logger.Info("Connect to Redpanda broker", "broker", addresses)

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_4_0_0
	conf.ClientID = "operator"
	conf.Admin.Timeout = time.Second

	consumer, err := sarama.NewConsumer(addresses, conf)
	if err != nil {
		logger.Error(err, "Error while creating consumer")
		return err
	}

	return consumer.Close()
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

	container, err = findContainer(pod.Spec.InitContainers, configuratorContainerName)
	if err != nil {
		return err
	}

	if container.Image != newImage {
		r.logger.Info("Init container image not updated to cluster image", "pod", pod.Name,
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
	ctx context.Context, sts *appsv1.StatefulSet, ordinal int32,
) error {
	r.logger.Info("Call update on statefulset", "ordinal", ordinal)

	sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{
		Partition: &ordinal,
	}
	if err := r.Update(ctx, sts); err != nil {
		return fmt.Errorf("failed to update StatefulSet (ordinal %d): %w", ordinal, err)
	}

	return nil
}

func (r *StatefulSetResource) modifyPodImage(
	stsSpec *corev1.PodSpec, newImage string,
) error {
	if err := modifyContainerImage(stsSpec.InitContainers, configuratorContainerName, newImage); err != nil {
		return err
	}

	if err := modifyContainerImage(stsSpec.Containers, redpandaContainerName, newImage); err != nil {
		return err
	}

	return nil
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

// NeedToReconcileError error carrying the time after which to requeue.
type NeedToReconcileError struct {
	RequeueAfter	time.Duration
	Msg		string
}

func (e *NeedToReconcileError) Error() string {
	return fmt.Sprintf("NeedToReconcileError %s", e.Msg)
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
