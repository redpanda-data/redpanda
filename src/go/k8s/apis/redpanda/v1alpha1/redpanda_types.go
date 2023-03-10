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
}

// RedpandaSpec defines the desired state of Redpanda
type RedpandaSpec struct {
	// ChartRef defines chart details including repository
	ChartRef ChartRef `json:"chartRef"`
	// ChartVersion defines the helm chart version to use
	ChartVersion string `json:"chartVersion,omitempty"`
	// HelmRepositoryName defines the repository to use, defaults to redpanda if not defined
	HelmRepositoryName string `json:"helmRepositoryName,omitempty"`
	// ClusterSpec defines the values to use in the cluster
	ClusterSpec RedpandaClusterSpec `json:"clusterSpec,omitempty"`
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
	HelmRepository string `json:"helmRepository,omitempty"`

	// +optional
	UpgradeFailures int64 `json:"upgradeFailures,omitempty"`

	// Failures is the reconciliation failure count against the latest desired
	// state. It is reset after a successful reconciliation.
	// +optional
	Failures int64 `json:"failures,omitempty"`

	// +optional
	InstallFailures int64 `json:"installFailures,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Redpanda is the Schema for the redpanda API
type Redpanda struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedpandaSpec   `json:"spec,omitempty"`
	Status RedpandaStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RedpandaList contains a list of Redpanda
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

func (in *Redpanda) GetValuesJson() (*apiextensionsv1.JSON, error) {
	vyaml, err := json.Marshal(in.Spec.ClusterSpec)
	if err != nil {
		return nil, fmt.Errorf("could not convert spec to yaml: %s", err)
	}
	values := &apiextensionsv1.JSON{Raw: vyaml}

	return values, nil
}

// RedpandaReady registers a successful reconciliation of the given HelmRelease.
func RedpandaReady(rp Redpanda) Redpanda {
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
func RedpandaNotReady(rp Redpanda, reason, message string) Redpanda {
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
func RedpandaProgressing(rp Redpanda) Redpanda {
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
