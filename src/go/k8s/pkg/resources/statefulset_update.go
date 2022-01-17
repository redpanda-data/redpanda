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
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

	if err = r.rollingUpdate(ctx); err != nil {
		return err
	}

	// Update is complete for all pods (and all are ready). Set upgrading status to false.
	if err = r.updateUpgradingStatus(ctx, false); err != nil {
		return fmt.Errorf("unable to turn off upgrading status in cluster custom resource: %w", err)
	}

	return nil
}

// rollingUpdate implements custom rolling update strategy that is inspired by
// the sts rolling update that takes into consideration pod and statefulset
// revision. For more information see
// https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/statefulset/stateful_set_control.go
func (r *StatefulSetResource) rollingUpdate(ctx context.Context) error {
	podList, err := getOrderedPods(ctx, r.pandaCluster, r)
	if err != nil {
		return fmt.Errorf("error getting pods %w", err)
	}
	sts, err := getSts(ctx, r.Key(), r)
	if err != nil {
		return fmt.Errorf("error getting sts %w", err)
	}
	// we cannot use CurrentRevision as this is never updated for OnDelete
	// update strategy https://github.com/kubernetes/kubernetes/pull/106059
	updateStsRevision := sts.Status.UpdateRevision

	for i := range podList.Items {
		pod := podList.Items[i]

		// check pod revision if differ delete pod
		podRevision := getPodRevision(&pod)
		if podRevision != updateStsRevision {
			r.logger.Info("Pod revision and sts revision does not match. Deleting pod to allow recreating it",
				"pod-name", pod.Name,
				"pod-revision", podRevision,
				"sts-revision", updateStsRevision)
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

func getOrderedPods(
	ctx context.Context,
	pandaCluster *redpandav1alpha1.Cluster,
	client k8sclient.Client,
) (*corev1.PodList, error) {
	var podList corev1.PodList
	err := client.List(ctx, &podList, &k8sclient.ListOptions{
		Namespace:     pandaCluster.Namespace,
		LabelSelector: labels.ForCluster(pandaCluster).AsClientSelector(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list panda pods: %w", err)
	}

	sort.Slice(podList.Items, func(i, j int) bool {
		return podList.Items[i].Name < podList.Items[j].Name
	})
	return &podList, nil
}

func getSts(
	ctx context.Context, key types.NamespacedName, client k8sclient.Client,
) (*appsv1.StatefulSet, error) {
	var sts appsv1.StatefulSet
	err := client.Get(ctx, key, &sts)
	return &sts, err
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

// getPodRevision gets the revision of Pod by inspecting the StatefulSetRevisionLabel. If pod has no revision the empty
// string is returned.
// see https://github.com/kubernetes/kubernetes/blob/b8af116327cd5d8e5411cbac04e7d4d11d22485d/pkg/controller/statefulset/stateful_set_utils.go#L420 for reference
func getPodRevision(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	return pod.Labels[appsv1.StatefulSetRevisionLabel]
}
