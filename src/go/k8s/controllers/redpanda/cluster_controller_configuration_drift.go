// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/networking"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	defaultDriftCheckPeriod = 1 * time.Minute

	debugLogLevel = 4
)

// ClusterConfigurationDriftReconciler detects drifts in the cluster configuration and triggers a reconciliation.
type ClusterConfigurationDriftReconciler struct {
	client.Client
	Log                       logr.Logger
	clusterDomain             string
	Scheme                    *runtime.Scheme
	DriftCheckPeriod          *time.Duration
	AdminAPIClientFactory     adminutils.AdminAPIClientFactory
	RestrictToRedpandaVersion string
}

// Reconcile detects drift in configuration for clusters and schedules a patch.
//
//nolint:funlen // May be broken down
func (r *ClusterConfigurationDriftReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := r.Log.WithValues("redpandacluster", req.NamespacedName)

	log.V(debugLogLevel).Info(fmt.Sprintf("Starting configuration drift reconcile loop for %v", req.NamespacedName))
	defer log.V(debugLogLevel).Info(fmt.Sprintf("Finished configuration drift reconcile loop for %v", req.NamespacedName))

	var redpandaCluster redpandav1alpha1.Cluster
	if err := r.Get(ctx, req.NamespacedName, &redpandaCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unable to retrieve Cluster resource: %w", err)
	}
	if redpandaCluster.GetDeletionTimestamp() != nil {
		log.Info("not reconciling deleted Cluster")
		return ctrl.Result{}, nil
	}

	if !featuregates.CentralizedConfiguration(redpandaCluster.Spec.Version) {
		return ctrl.Result{RequeueAfter: r.getDriftCheckPeriod()}, nil
	}

	if !isRedpandaClusterManaged(log, &redpandaCluster) {
		return ctrl.Result{RequeueAfter: r.getDriftCheckPeriod()}, nil
	}
	if !isRedpandaClusterVersionManaged(log, &redpandaCluster, r.RestrictToRedpandaVersion) {
		return ctrl.Result{}, nil
	}

	condition := redpandaCluster.Status.GetCondition(redpandav1alpha1.ClusterConfiguredConditionType)
	if condition == nil || condition.Status != corev1.ConditionTrue {
		// configuration drift already signaled
		return ctrl.Result{RequeueAfter: r.getDriftCheckPeriod()}, nil
	}

	// wait at least a driftCheckPeriod before checking drifts
	now := time.Now()
	driftCheckPeriod := r.getDriftCheckPeriod()
	if condition.LastTransitionTime.Time.Add(driftCheckPeriod).After(now) {
		period := condition.LastTransitionTime.Time.Add(driftCheckPeriod).Sub(now)
		return ctrl.Result{RequeueAfter: period}, nil
	}

	// Pre-check before contacting the admin API to exclude errors
	if available, err := adminutils.IsAvailableInPreFlight(ctx, r, &redpandaCluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not perform pre-flight check for admin API availability: %w", err)
	} else if !available {
		return ctrl.Result{RequeueAfter: r.getDriftCheckPeriod()}, nil
	}

	redpandaPorts := networking.NewRedpandaPorts(&redpandaCluster)
	headlessPorts := collectHeadlessPorts(redpandaPorts)
	clusterPorts := collectClusterPorts(redpandaPorts, &redpandaCluster)

	headlessSvc := resources.NewHeadlessService(r.Client, &redpandaCluster, r.Scheme, headlessPorts, log)
	clusterSvc := resources.NewClusterService(r.Client, &redpandaCluster, r.Scheme, clusterPorts, log)

	var proxySu *resources.SuperUsersResource
	var proxySuKey types.NamespacedName
	if redpandaCluster.IsSASLOnInternalEnabled() && redpandaCluster.PandaproxyAPIInternal() != nil {
		proxySu = resources.NewSuperUsers(r.Client, &redpandaCluster, r.Scheme, resources.ScramPandaproxyUsername, resources.PandaProxySuffix, log)
		proxySuKey = proxySu.Key()
	}
	var schemaRegistrySu *resources.SuperUsersResource
	var schemaRegistrySuKey types.NamespacedName
	if redpandaCluster.IsSASLOnInternalEnabled() && redpandaCluster.Spec.Configuration.SchemaRegistry != nil {
		schemaRegistrySu = resources.NewSuperUsers(r.Client, &redpandaCluster, r.Scheme, resources.ScramSchemaRegistryUsername, resources.SchemaRegistrySuffix, log)
		schemaRegistrySuKey = schemaRegistrySu.Key()
	}
	pki, err := certmanager.NewPki(ctx, r.Client, &redpandaCluster, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), clusterSvc.ServiceFQDN(r.clusterDomain), r.Scheme, log)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating pki: %w", err)
	}
	configMapResource := resources.NewConfigMap(r.Client, &redpandaCluster, r.Scheme, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), proxySuKey, schemaRegistrySuKey, pki.BrokerTLSConfigProvider(), log)

	lastAppliedConfig, cmExists, err := configMapResource.GetLastAppliedConfigurationFromCluster(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not get last applied configuration to check drifts: %w", err)
	} else if !cmExists {
		return ctrl.Result{RequeueAfter: r.getDriftCheckPeriod()}, nil
	}

	adminAPI, err := r.AdminAPIClientFactory(ctx, r, &redpandaCluster, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), pki.AdminAPIConfigProvider())
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not get admin API to check drifts on the cluster: %w", err)
	}

	schema, err := adminAPI.ClusterConfigSchema(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not get cluster schema to check drifts: %w", err)
	}
	clusterConfig, err := adminAPI.Config(ctx, true)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not get cluster configuration to check drifts: %w", err)
	}

	// Since config is in sync, we assume that the current desired configuration is equal to the lastAppliedConfig and there are no invalid properties
	patch := configuration.ThreeWayMerge(log, lastAppliedConfig, clusterConfig, lastAppliedConfig, nil, schema)
	if patch.Empty() {
		// Nothing to do, everything in sync
		return ctrl.Result{RequeueAfter: r.getDriftCheckPeriod()}, nil
	}

	log.Info("Detected configuration drift in the cluster")

	// Signal drift by setting the condition to False
	redpandaCluster.Status.SetCondition(
		redpandav1alpha1.ClusterConfiguredConditionType,
		corev1.ConditionFalse,
		redpandav1alpha1.ClusterConfiguredReasonDrift,
		"Drift detected by periodic check",
	)
	if err := r.Status().Update(ctx, &redpandaCluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not patch cluster to signal a configuration drift: %w", err)
	}

	return ctrl.Result{RequeueAfter: r.getDriftCheckPeriod()}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterConfigurationDriftReconciler) SetupWithManager(
	mgr ctrl.Manager,
) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Cluster{}).
		WithEventFilter(createOrDeleteEventFilter{}).
		Complete(r)
}

// WithClusterDomain set the clusterDomain
func (r *ClusterConfigurationDriftReconciler) WithClusterDomain(
	clusterDomain string,
) *ClusterConfigurationDriftReconciler {
	r.clusterDomain = clusterDomain
	return r
}

func (r *ClusterConfigurationDriftReconciler) getDriftCheckPeriod() time.Duration {
	if r.DriftCheckPeriod != nil {
		return *r.DriftCheckPeriod
	}
	return defaultDriftCheckPeriod
}

// createOrDeleteEventFilter selects only the events of creation and deletion of a cluster,
// to make the controller independent of changes to the resources.
// Note: a "create" event is also fired for existing resources when the controller starts up.
type createOrDeleteEventFilter struct{}

// Create is implemented for compatibility with predicate.Predicate
func (filter createOrDeleteEventFilter) Create(event.CreateEvent) bool {
	return true
}

// Delete is implemented for compatibility with predicate.Predicate
func (filter createOrDeleteEventFilter) Delete(event.DeleteEvent) bool {
	return true
}

// Update is implemented for compatibility with predicate.Predicate
func (filter createOrDeleteEventFilter) Update(event.UpdateEvent) bool {
	return false
}

// Generic is implemented for compatibility with predicate.Predicate
func (filter createOrDeleteEventFilter) Generic(event.GenericEvent) bool {
	return false
}
