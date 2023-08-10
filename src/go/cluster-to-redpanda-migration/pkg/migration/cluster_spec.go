// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package migration

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"
)

func MigrateClusterSpec(cluster *vectorizedv1alpha1.Cluster, rp *v1alpha1.Redpanda, repository string, imageVersion string) {
	// Cannot be nil
	oldSpec := cluster.Spec

	rpSpec := &v1alpha1.RedpandaClusterSpec{}
	if rp.Spec.ClusterSpec != nil {
		rpSpec = rp.Spec.ClusterSpec
	}

	rpStatefulset := &v1alpha1.Statefulset{}
	if rpSpec.Statefulset != nil {
		rpStatefulset = rpSpec.Statefulset
	}

	rpLicenseRef := &v1alpha1.LicenseSecretRef{}
	if rpSpec.LicenseSecretRef != nil {
		rpLicenseRef = rpSpec.LicenseSecretRef
	}

	rpAuth := &v1alpha1.Auth{}
	if rpSpec.Auth != nil {
		rpAuth = rpSpec.Auth
	}

	// --- Image information --
	rpImage := &v1alpha1.RedpandaImage{}
	if rpSpec.Image != nil {
		rpImage = rpSpec.Image
	}

	if oldSpec.Image != "" {
		rpImage.Repository = pointer.String(oldSpec.Image)
	} else {
		rpImage.Repository = pointer.String(repository)
	}

	if oldSpec.Version != "" {
		rpImage.Tag = pointer.String(oldSpec.Version)
	} else {
		rpImage.Tag = pointer.String(imageVersion)
	}

	if pointer.StringDeref(rpImage.Tag, "") == "" && pointer.StringDeref(rpImage.Repository, "") == "" {
		rpImage = nil
	}

	// -- NodeSelector ---
	if len(oldSpec.NodeSelector) > 0 {
		rpSpec.NodeSelector = oldSpec.NodeSelector
	}

	// -- Annotations ---
	if oldSpec.Annotations != nil {
		rpStatefulset.Annotations = oldSpec.Annotations
	}

	oldPBD := oldSpec.PodDisruptionBudget
	if oldPBD != nil && oldPBD.Enabled && oldPBD.MaxUnavailable != nil {
		rpStatefulset.Budget = &v1alpha1.Budget{
			MaxUnavailable: oldPBD.MaxUnavailable.IntValue(),
		}
	}

	rpTolerations := make([]corev1.Toleration, 0)
	if oldSpec.Tolerations != nil && len(oldSpec.Tolerations) > 0 {
		rpTolerations = append(rpTolerations, oldSpec.Tolerations...)
	}

	// --- Replicas ---
	rpStatefulset.Replicas = pointer.Int(int(pointer.Int32Deref(oldSpec.Replicas, 3)))

	// --- Resources ---
	addResources := false

	rpResources := &v1alpha1.Resources{}
	if rpSpec.Resources != nil {
		rpResources = rpSpec.Resources
	}

	OldResources := oldSpec.Resources
	// on old spec, the statefulset entries for these are:
	/*
			Resources: corev1.ResourceRequirements{
			Limits:   r.pandaCluster.Spec.Resources.Limits,
			Requests: r.pandaCluster.Spec.Resources.Requests,
		},
	*/

	// on helm chart these are a bit different as the logic is as follows:
	/*
		{{- if hasKey .Values.resources.memory "min" }}
		            requests:
		              cpu: {{ .Values.resources.cpu.cores }}
		              memory: {{ .Values.resources.memory.container.min }}
		{{- end }}
		            limits:
		              cpu: {{ .Values.resources.cpu.cores }}
		              memory: {{ .Values.resources.memory.container.max }}
	*/

	// so limits are always copied, but requests have a condition of "min"
	if OldResources.Limits != nil {
		addResources = true
		if rpResources.CPU == nil {
			rpResources.CPU = &v1alpha1.CPU{}
		}

		rpResources.CPU.Cores = OldResources.Limits.Cpu()

		if rpResources.Memory == nil {
			rpResources.Memory = &v1alpha1.Memory{}
		}

		if rpResources.Memory.Container == nil {
			rpResources.Memory.Container = &v1alpha1.Container{}
		}

		rpResources.Memory.Container.Max = OldResources.Limits.Memory()

		// TODO: multiplying by 80% fo fix issue we are seeing
		// 1000 * 1/.8, check if this is the correct approach
		// scaling to mega converts from Gi to MegaBytes with is multiplying by 1073.74
		q := float64(OldResources.Limits.Memory().ScaledValue(resource.Mega)*1250) / float64(1073.74)
		quant := resource.MustParse(fmt.Sprintf("%.0fMi", q))

		rpResources.Memory.Container.Max = &quant

	}

	if OldResources.Requests != nil {
		addResources = true
		if rpResources.CPU == nil {
			rpResources.CPU = &v1alpha1.CPU{}
		}

		rpResources.CPU.Cores = OldResources.Requests.Cpu()

		if rpResources.Memory == nil {
			rpResources.Memory = &v1alpha1.Memory{}
		}

		if rpResources.Memory.Container == nil {
			rpResources.Memory.Container = &v1alpha1.Container{}
		}

		// TODO: multiplying by 80% fo fix issue we are seeing
		// 1000 * 1/.8, check if this is the correct approach
		// scaling to mega converts from Gi to MegaBytes with is multiplying by 1073.74
		q := float64(OldResources.Limits.Memory().ScaledValue(resource.Mega)*1250) / float64(1073.74)
		quant := resource.MustParse(fmt.Sprintf("%.0fMi", q))

		rpResources.Memory.Container.Min = &quant
	}

	if OldResources.Redpanda != nil {
		addResources = true
		if rpResources.Memory == nil {
			rpResources.Memory = &v1alpha1.Memory{}
		}

		if rpResources.Memory.Redpanda == nil {
			rpResources.Memory.Redpanda = &v1alpha1.RedpandaMemory{}
		}

		rpResources.Memory.Redpanda.Memory = OldResources.RedpandaMemory()

		// TODO: doesnt seem like this concepts exists in cluster spec so adding default here
		rpResources.Memory.Redpanda.ReserveMemory = resource.NewQuantity(1, resource.BinarySI)
	}

	if addResources {
		rpSpec.Resources = rpResources
	}

	// migrate auth
	if pointer.BoolDeref(oldSpec.KafkaEnableAuthorization, false) || oldSpec.EnableSASL {
		rpAuth.SASL = &v1alpha1.SASL{
			Enabled: true,
		}

		// TODO: check to see what secret is being created with user information
		if len(oldSpec.Superusers) > 0 {
			users := make([]v1alpha1.UsersItems, 0)
			for i := range oldSpec.Superusers {
				user := oldSpec.Superusers[i]
				userItem := v1alpha1.UsersItems{
					Name:      pointer.String(user.Username),
					Password:  pointer.String(""),
					Mechanism: pointer.String("SCRAM-SHA-512"),
				}

				users = append(users, userItem) //nolint:staticcheck // placeholder for now
			}
		} else {
			rpAuth.SASL.Users = nil
		}
	} else {
		rpAuth = nil
	}

	// --- license details ---
	if oldSpec.LicenseRef != nil {
		if oldSpec.LicenseRef.Key != "" {
			rpLicenseRef.SecretKey = pointer.String(oldSpec.LicenseRef.Key)
		}
		rpLicenseRef.SecretName = pointer.String(oldSpec.LicenseRef.Name)
	} else {
		rpLicenseRef = nil
	}

	// --- sidecars ---
	migrateSideCars(cluster, rp)

	// --- storage ---
	rpStorage := migrateStorage(&oldSpec.Storage)

	// --- cloud storage ---
	migrateCloudStorage(&oldSpec.CloudStorage, rp)

	// --- finalize ---

	rpSpec.Image = rpImage
	rpSpec.Statefulset = rpStatefulset
	rpSpec.Tolerations = rpTolerations
	rpSpec.Auth = rpAuth
	rpSpec.LicenseSecretRef = rpLicenseRef
	rpSpec.Storage = rpStorage

	// cluster spec now updated, we should probably only recreate if needed
	rp.Spec.ClusterSpec = rpSpec
}

func migrateSideCars(cluster *vectorizedv1alpha1.Cluster, rp *v1alpha1.Redpanda) {
	if cluster == nil {
		return
	}

	// currently only rpk status exists
	// TODO: may require for us to add this sidecar to helm chart
	if cluster.Spec.Sidecars.RpkStatus == nil {
		return
	}

	rpkStatusSideCar := cluster.Spec.Sidecars.RpkStatus

	sts := &v1alpha1.Statefulset{}
	if rp.Spec.ClusterSpec.Statefulset != nil {
		sts = rp.Spec.ClusterSpec.Statefulset
	}

	sideCars := &v1alpha1.SideCars{}
	if sts.SideCars != nil {
		sideCars = sts.SideCars
	}

	sideCars.RpkStatus = &v1alpha1.SideCarObj{
		Enabled:   rpkStatusSideCar.Enabled,
		Resources: rpkStatusSideCar.Resources,
	}
}
