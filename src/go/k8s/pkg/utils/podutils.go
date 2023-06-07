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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
