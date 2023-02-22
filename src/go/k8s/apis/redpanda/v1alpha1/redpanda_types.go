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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// RedpandaSpec defines the desired state of Redpanda
type RedpandaSpec struct {

	// NameOverride is the override to give your redpanda release
	NameOverride string `json:"nameOverride,omitempty"`
	// NameOverride is the override to give your redpanda release
	FullnameOverride string `json:"fullnameOverride,omitempty"`
	// NameOverride is the override to give your redpanda release
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// NameOverride is the override to give your redpanda release
	CommonLabels []string `json:"commonLabels,omitempty"`
	// NameOverride is the override to give your redpanda release
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// NameOverride is the override to give your redpanda release
	Tolerations []string `json:"tolerations,omitempty"`

	Image RedpandaImage `json:"image,omitempty"`
}

// RedpandaStatus defines the observed state of Redpanda
type RedpandaStatus struct {
	// +optional
	HelmRelease string `json:"helmRelease,omitempty"`

	// +optional
	UpgradeFailures int64 `json:"upgradeFailures,omitempty"`

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

type RedpandaImage struct {
	Repository string `json:"repository,omitempty"`
	Tag        string `json:"tag,omitempty"`
	PullPolicy string `json:"pullPolicy,omitempty"`
}

func init() {
	SchemeBuilder.Register(&Redpanda{}, &RedpandaList{})
}

// GetHelmRelease returns the namespace and name of the HelmRelease.
func (in RedpandaStatus) GetHelmRelease() (string, string) {
	if in.HelmRelease == "" {
		return "", ""
	}
	if split := strings.Split(in.HelmRelease, string(types.Separator)); len(split) > 1 {
		return split[0], split[1]
	}
	return "", ""
}
func (in Redpanda) GetHelmReleaseName() string {
	return strings.Join([]string{in.Namespace, in.Name}, "-")
}
