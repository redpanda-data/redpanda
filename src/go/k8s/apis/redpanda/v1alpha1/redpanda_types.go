// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1

import (
	"encoding/json"
	"fmt"

	helmv2beta1 "github.com/fluxcd/helm-controller/api/v2beta1"

	"github.com/fluxcd/pkg/apis/meta"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var RedpandaChartRepository = "https://charts.redpanda.com/"

type ChartRef struct {
	// ChartName is the chart to use
	ChartName string `json:"chartName,omitempty"`
	// ChartVersion defines the helm chart version to use
	ChartVersion string `json:"chartVersion,omitempty"`
	// HelmRepositoryName defines the repository to use, defaults to redpanda if not defined
	HelmRepositoryName string `json:"helmRepositoryName,omitempty"`
	// Timeout is the time to wait for any individual Kubernetes operation (like Jobs
	// for hooks) during the performance of a Helm action. Defaults to '15m0s'.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern="^([0-9]+(\\.[0-9]+)?(ms|s|m|h))+$"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
	// Upgrade contains the details for handling upgrades including failures
	Upgrade *HelmUpgrade `json:"upgrade,omitempty"`
}

// RedpandaSpec defines the desired state of Redpanda
type RedpandaSpec struct {
	// ChartRef defines chart details including repository
	ChartRef ChartRef `json:"chartRef"`
	// HelmRepositoryName defines the repository to use, defaults to redpanda if not defined
	HelmRepositoryName string `json:"helmRepositoryName,omitempty"`
	// ClusterSpec defines the values to use in the cluster
	ClusterSpec *RedpandaClusterSpec `json:"clusterSpec,omitempty"`
}

// RedpandaStatus defines the observed state of Redpanda
type RedpandaStatus struct {
	// ObservedGeneration is the last observed generation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	meta.ReconcileRequestStatus `json:",inline"`

	// Conditions holds the conditions for the Redpanda.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastAppliedRevision is the revision of the last successfully applied source.
	// +optional
	LastAppliedRevision string `json:"lastAppliedRevision,omitempty"`

	// LastAttemptedRevision is the revision of the last reconciliation attempt.
	// +optional
	LastAttemptedRevision string `json:"lastAttemptedRevision,omitempty"`

	// +optional
	HelmRelease string `json:"helmRelease,omitempty"`

	// +optional
	HelmReleaseReady *bool `json:"helmReleaseReady,omitempty"`

	// +optional
	HelmRepository string `json:"helmRepository,omitempty"`

	// +optional
	HelmRepositoryReady *bool `json:"helmRepositoryReady,omitempty"`

	// +optional
	UpgradeFailures int64 `json:"upgradeFailures,omitempty"`

	// Failures is the reconciliation failure count against the latest desired
	// state. It is reset after a successful reconciliation.
	// +optional
	Failures int64 `json:"failures,omitempty"`

	// +optional
	InstallFailures int64 `json:"installFailures,omitempty"`
}

type RemediationStrategy string

// HelmUpgrade represents the configurations upgrading helm releases
type HelmUpgrade struct {
	Remediation    *helmv2beta1.UpgradeRemediation `json:"remediation,omitempty"`
	Force          *bool                           `json:"force,omitempty"`
	PreserveValues *bool                           `json:"preserveValues,omitempty"`
	CleanupOnFail  *bool                           `json:"cleanupOnFail,omitempty"`
}

// Redpanda is the Schema for the redpanda API
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rp
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status",description=""
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].message",description=""
type Redpanda struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedpandaSpec   `json:"spec,omitempty"`
	Status RedpandaStatus `json:"status,omitempty"`
}

// RedpandaList contains a list of Redpanda
// +kubebuilder:object:root=true
type RedpandaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Redpanda `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Redpanda{}, &RedpandaList{})
}

// GetHelmRelease returns the namespace and name of the HelmRelease.
func (in *RedpandaStatus) GetHelmRelease() string {
	return in.HelmRelease
}

func (in *Redpanda) GetHelmReleaseName() string {
	return in.Name
}

func (in *Redpanda) GetHelmRepositoryName() string {
	helmRepository := in.Spec.HelmRepositoryName
	if helmRepository == "" {
		helmRepository = "redpanda-repository"
	}
	return helmRepository
}

func (in *Redpanda) ValuesJSON() (*apiextensionsv1.JSON, error) {
	vyaml, err := json.Marshal(in.Spec.ClusterSpec)
	if err != nil {
		return nil, fmt.Errorf("could not convert spec to yaml: %w", err)
	}
	values := &apiextensionsv1.JSON{Raw: vyaml}

	return values, nil
}

// RedpandaReady registers a successful reconciliation of the given HelmRelease.
func RedpandaReady(rp *Redpanda) *Redpanda {
	newCondition := metav1.Condition{
		Type:    meta.ReadyCondition,
		Status:  metav1.ConditionTrue,
		Reason:  "RedpandaClusterDeployed",
		Message: "Redpanda reconciliation succeeded",
	}
	apimeta.SetStatusCondition(rp.GetConditions(), newCondition)
	rp.Status.LastAppliedRevision = rp.Status.LastAttemptedRevision
	return rp
}

// RedpandaNotReady registers a failed reconciliation of the given Redpanda.
func RedpandaNotReady(rp *Redpanda, reason, message string) *Redpanda {
	newCondition := metav1.Condition{
		Type:    meta.ReadyCondition,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	}
	apimeta.SetStatusCondition(rp.GetConditions(), newCondition)
	return rp
}

// RedpandaProgressing resets any failures and registers progress toward
// reconciling the given Redpanda by setting the meta.ReadyCondition to
// 'Unknown' for meta.ProgressingReason.
func RedpandaProgressing(rp *Redpanda) *Redpanda {
	rp.Status.Conditions = []metav1.Condition{}
	newCondition := metav1.Condition{
		Type:    meta.ReadyCondition,
		Status:  metav1.ConditionUnknown,
		Reason:  meta.ProgressingReason,
		Message: "Reconciliation in progress",
	}
	apimeta.SetStatusCondition(rp.GetConditions(), newCondition)
	return rp
}

// GetConditions returns the status conditions of the object.
func (in *Redpanda) GetConditions() *[]metav1.Condition {
	return &in.Status.Conditions
}

func (in *Redpanda) OwnerShipRefObj() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: in.APIVersion,
		Kind:       in.Kind,
		Name:       in.Name,
		UID:        in.UID,
	}
}
