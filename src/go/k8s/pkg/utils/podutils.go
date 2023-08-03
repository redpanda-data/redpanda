// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SetStatusPodCondition sets the corresponding condition in conditions to newCondition.
// conditions must be non-nil.
//  1. if the condition of the specified type already exists (all fields of the existing condition are updated to
//     newCondition, LastTransitionTime is set to now if the new status differs from the old status)
//  2. if a condition of the specified type does not exist (LastTransitionTime is set to now() if unset, and newCondition is appended)
func SetStatusPodCondition(conditions *[]corev1.PodCondition, newCondition *corev1.PodCondition) {
	if conditions == nil {
		return
	}
	existingCondition := FindStatusPodCondition(*conditions, newCondition.Type)
	if existingCondition == nil {
		if newCondition.LastTransitionTime.IsZero() {
			newCondition.LastTransitionTime = metav1.NewTime(time.Now())
		}
		*conditions = append(*conditions, *newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		if !newCondition.LastTransitionTime.IsZero() {
			existingCondition.LastTransitionTime = newCondition.LastTransitionTime
		} else {
			existingCondition.LastTransitionTime = metav1.NewTime(time.Now())
		}
	}

	existingCondition.Reason = newCondition.Reason
	existingCondition.Message = newCondition.Message
}

// RemoveStatusPodCondition removes the corresponding conditionType from conditions.
// conditions must be non-nil.
func RemoveStatusPodCondition(conditions *[]corev1.PodCondition, conditionType corev1.PodConditionType) {
	if conditions == nil || len(*conditions) == 0 {
		return
	}
	newConditions := make([]corev1.PodCondition, 0, len(*conditions)-1)
	for _, condition := range *conditions {
		if condition.Type != conditionType {
			newConditions = append(newConditions, condition)
		}
	}

	*conditions = newConditions
}

func FindStatusPodCondition(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}

	return nil
}

// IsStatusPodConditionTrue returns true when the conditionType is present and set to `metav1.ConditionTrue`
func IsStatusPodConditionTrue(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) bool {
	return IsStatusPodConditionPresentAndEqual(conditions, conditionType, corev1.ConditionTrue)
}

// IsStatusPodConditionFalse returns true when the conditionType is present and set to `metav1.ConditionFalse`
func IsStatusPodConditionFalse(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) bool {
	return IsStatusPodConditionPresentAndEqual(conditions, conditionType, corev1.ConditionFalse)
}

// IsStatusPodConditionPresentAndEqual returns true when conditionType is present and equal to status.
func IsStatusPodConditionPresentAndEqual(conditions []corev1.PodCondition, conditionType corev1.PodConditionType, status corev1.ConditionStatus) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}
	return false
}

func DeletePodPVCs(ctx context.Context, c client.Client, pod *corev1.Pod, l logr.Logger) error {
	log := l.WithName("DeletePodPVCs")
	//   delete the associated pvc
	pvc := corev1.PersistentVolumeClaim{}
	for i := range pod.Spec.Volumes {
		v := &pod.Spec.Volumes[i]
		if v.PersistentVolumeClaim != nil {
			key := types.NamespacedName{
				Name:      v.PersistentVolumeClaim.ClaimName,
				Namespace: pod.GetNamespace(),
			}
			err := c.Get(ctx, key, &pvc)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return fmt.Errorf(`unable to fetch PersistentVolumeClaim "%s/%s": %w`, key.Namespace, key.Name, err)
				}
				continue
			}
			log.WithValues("persistent-volume-claim", key).Info("deleting PersistentVolumeClaim")
			if err := c.Delete(ctx, &pvc, &client.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf(`unable to delete PersistentVolumeClaim "%s/%s": %w`, key.Name, key.Namespace, err)
			}
		}
	}
	return nil
}
