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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	corev1 "k8s.io/api/core/v1"
)

// reconcileConfiguration ensures that the cluster configuration is synchronized with expected data
//
//nolint:funlen // splitting makes it difficult to follow
func (r *ClusterReconciler) reconcileConfiguration(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	configMapResource *resources.ConfigMapResource,
	statefulSetResource *resources.StatefulSetResource,
	pki *certmanager.PkiReconciler,
	fqdn string,
	log logr.Logger,
) error {
	errorWithContext := newErrorWithContext(redpandaCluster.Namespace, redpandaCluster.Name)
	if !featuregates.CentralizedConfiguration(redpandaCluster.Spec.Version) {
		log.Info("Cluster is not using centralized configuration, skipping...")
		return nil
	}

	if added, err := r.ensureConditionPresent(ctx, redpandaCluster, log); err != nil || added {
		// If condition is added or error returned, we wait for another reconcile loop
		return err
	}

	if redpandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterConfiguredConditionType) == corev1.ConditionTrue {
		log.Info("Cluster configuration is synchronized")
		return nil
	}

	config, err := configMapResource.CreateConfiguration(ctx)
	if err != nil {
		return errorWithContext(err, "error while creating the configuration")
	}

	lastAppliedConfiguration, err := r.getOrInitLastAppliedConfiguration(ctx, configMapResource, config)
	if err != nil {
		return errorWithContext(err, "could not load the last applied configuration")
	}

	// Before trying to contact the admin API we should do a pre-check, to prevent errors and exponential backoff
	var available bool
	available, err = adminutils.IsAvailableInPreFlight(ctx, r, redpandaCluster)
	if err != nil {
		return errorWithContext(err, "could not perform pre-flight check for admin API availability")
	} else if !available {
		log.Info("Waiting for admin API to be available before syncing the configuration")
		return &resources.RequeueAfterError{
			RequeueAfter: resources.RequeueDuration,
			Msg:          "admin API is not available yet",
		}
	}

	adminAPI, err := r.AdminAPIClientFactory(ctx, r, redpandaCluster, fqdn, pki.AdminAPIConfigProvider())
	if err != nil {
		return errorWithContext(err, "error creating the admin API client")
	}

	// Checking if the feature is active because in the initial stages of cluster creation, it takes time for the feature to be activated
	// and the API returns the same error (400) that is returned in case of malformed input, which causes a stop of the reconciliation
	var centralConfigActive bool
	if centralConfigActive, err = adminutils.IsFeatureActive(ctx, adminAPI, adminutils.CentralConfigFeatureName); err != nil {
		return errorWithContext(err, "could not determine if central config is active in the cluster")
	} else if !centralConfigActive {
		log.Info("Waiting for the centralized configuration feature to be active in the cluster")
		return &resources.RequeueAfterError{
			RequeueAfter: resources.RequeueDuration,
			Msg:          "centralized configuration feature not active",
		}
	}

	schema, clusterConfig, status, err := r.retrieveClusterState(ctx, redpandaCluster, adminAPI)
	if err != nil {
		return err
	}

	patchSuccess, err := r.applyPatchIfNeeded(ctx, redpandaCluster, adminAPI, config, schema, clusterConfig, status, lastAppliedConfiguration, log)
	if err != nil || !patchSuccess {
		// patchSuccess=false indicates an error set on the condition that should not be propagated (but we terminate reconciliation anyway)
		return err
	}

	// TODO a failure and restart here (after successful patch, before setting the last applied configuration) may lead to inconsistency if the user
	// changes the CR in the meantime (e.g. removing a field), since we applied a config to the cluster but did not store the information anywhere else.
	// A possible fix is doing a two-phase commit (first stage commit on configmap, then apply it to the cluster, with possibility to recover on failure),
	// but it seems overkill given that the case is rare and requires cooperation from the user.

	hash, hashChanged, err := r.checkCentralizedConfigurationHashChange(ctx, redpandaCluster, config, schema, lastAppliedConfiguration, statefulSetResource)
	if err != nil {
		return err
	} else if hashChanged {
		// Definitely needs restart
		log.Info("Centralized configuration hash has changed")
		if err = statefulSetResource.SetCentralizedConfigurationHashInCluster(ctx, hash); err != nil {
			return errorWithContext(err, "could not update config hash on statefulset")
		}
	}

	// Now we can mark the new lastAppliedConfiguration for next update
	if err = configMapResource.SetLastAppliedConfigurationInCluster(ctx, config.ClusterConfiguration); err != nil {
		return errorWithContext(err, "could not store last applied configuration in the cluster")
	}

	// Synchronized status with cluster, including triggering a restart if needed
	conditionData, err := r.synchronizeStatusWithCluster(ctx, redpandaCluster, adminAPI, log)
	if err != nil {
		return err
	}

	// If condition is not met, we need to reschedule, waiting for the cluster to heal.
	if conditionData.Status != corev1.ConditionTrue {
		return &resources.RequeueAfterError{
			RequeueAfter: resources.RequeueDuration,
			Msg:          fmt.Sprintf("cluster configuration is not in sync (%s): %s", conditionData.Reason, conditionData.Message),
		}
	}

	return nil
}

// getOrInitLastAppliedConfiguration gets the last applied configuration to the cluster or creates it when missing.
//
// This is needed because the controller will later use that annotation to determine which centralized properties are managed by the operator, since configuration
// can be changed by other means in a cluster. A missing annotation indicates a cluster where centralized configuration has just been primed using the
// contents of the .bootstrap.yaml file, so we freeze its current content (early in the reconciliation cycle) so that subsequent patches are computed correctly.
func (r *ClusterReconciler) getOrInitLastAppliedConfiguration(
	ctx context.Context,
	configMapResource *resources.ConfigMapResource,
	config *configuration.GlobalConfiguration,
) (map[string]interface{}, error) {
	lastApplied, cmPresent, err := configMapResource.GetLastAppliedConfigurationFromCluster(ctx)
	if err != nil {
		return nil, err
	}
	if !cmPresent || lastApplied != nil {
		return lastApplied, nil
	}

	if err := configMapResource.SetLastAppliedConfigurationInCluster(ctx, config.ClusterConfiguration); err != nil {
		return nil, err
	}
	return config.ClusterConfiguration, nil
}

func (r *ClusterReconciler) applyPatchIfNeeded(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	adminAPI adminutils.AdminAPIClient,
	cfg *configuration.GlobalConfiguration,
	schema admin.ConfigSchema,
	clusterConfig admin.Config,
	status admin.ConfigStatusResponse,
	lastAppliedConfiguration map[string]interface{},
	log logr.Logger,
) (success bool, err error) {
	errorWithContext := newErrorWithContext(redpandaCluster.Namespace, redpandaCluster.Name)

	var invalidProperties []string
	for i := range status {
		invalidProperties = append(invalidProperties, status[i].Invalid...)
		invalidProperties = append(invalidProperties, status[i].Unknown...)
	}

	patch := configuration.ThreeWayMerge(log, cfg.ClusterConfiguration, clusterConfig, lastAppliedConfiguration, invalidProperties, schema)
	if patch.Empty() {
		return true, nil
	}

	log.Info("Applying patch to the cluster configuration", "patch", patch.String())
	wr, err := adminAPI.PatchClusterConfig(ctx, patch.Upsert, patch.Remove)
	if err != nil {
		var conditionData *redpandav1alpha1.ClusterCondition
		conditionData, err = tryMapErrorToCondition(err)
		if err != nil {
			return false, errorWithContext(err, "could not patch centralized configuration")
		}
		log.Info("Failure when patching the configuration using the admin API")
		conditionChanged := redpandaCluster.Status.SetCondition(
			conditionData.Type,
			conditionData.Status,
			conditionData.Reason,
			conditionData.Message,
		)
		if conditionChanged {
			log.Info("Updating the condition with failure information",
				"status", conditionData.Status,
				"reason", conditionData.Reason,
				"message", conditionData.Message,
			)
			if err := r.Status().Update(ctx, redpandaCluster); err != nil {
				return false, errorWithContext(err, "could not update condition on cluster")
			}
		}
		// Patch issue is due to user error, so it's unrecoverable
		return false, nil
	}
	log.Info("Patch written to the cluster", "config_version", wr.ConfigVersion)
	return true, nil
}

func (r *ClusterReconciler) retrieveClusterState(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	adminAPI adminutils.AdminAPIClient,
) (admin.ConfigSchema, admin.Config, admin.ConfigStatusResponse, error) {
	errorWithContext := newErrorWithContext(redpandaCluster.Namespace, redpandaCluster.Name)

	schema, err := adminAPI.ClusterConfigSchema(ctx)
	if err != nil {
		return nil, nil, nil, errorWithContext(err, "could not get centralized configuration schema")
	}
	clusterConfig, err := adminAPI.Config(ctx, true)
	if err != nil {
		return nil, nil, nil, errorWithContext(err, "could not get current centralized configuration from cluster")
	}

	// We always send requests for config status to the leader to avoid inconsistencies due to config propagation delays.
	status, err := adminAPI.ClusterConfigStatus(ctx, true)
	if err != nil {
		return nil, nil, nil, errorWithContext(err, "could not get current centralized configuration status from cluster")
	}

	return schema, clusterConfig, status, nil
}

func (r *ClusterReconciler) ensureConditionPresent(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	log logr.Logger,
) (bool, error) {
	if condition := redpandaCluster.Status.GetCondition(redpandav1alpha1.ClusterConfiguredConditionType); condition == nil {
		// nil condition means that no change has been detected earlier, but we can't assume that configuration is in sync
		// because of multiple reasons, for example:
		// - .bootstrap.yaml may contain invalid/unknown properties
		// - The PVC may have been recycled from a previously running cluster with a different configuration
		log.Info("Setting the condition to false until check against admin API")
		redpandaCluster.Status.SetCondition(
			redpandav1alpha1.ClusterConfiguredConditionType,
			corev1.ConditionFalse,
			redpandav1alpha1.ClusterConfiguredReasonUpdating,
			"Verifying configuration using cluster admin API",
		)
		if err := r.Status().Update(ctx, redpandaCluster); err != nil {
			return false, newErrorWithContext(redpandaCluster.Namespace, redpandaCluster.Name)(err, "could not update condition on cluster")
		}
		return true, nil
	}
	return false, nil
}

func (r *ClusterReconciler) checkCentralizedConfigurationHashChange(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	config *configuration.GlobalConfiguration,
	schema admin.ConfigSchema,
	lastAppliedConfiguration map[string]interface{},
	statefulSetResource *resources.StatefulSetResource,
) (hash string, changed bool, err error) {
	hash, err = config.GetCentralizedConfigurationHash(schema)
	if err != nil {
		return "", false, newErrorWithContext(redpandaCluster.Namespace, redpandaCluster.Name)(err, "could not compute hash of the new configuration")
	}

	oldHash, err := statefulSetResource.GetCentralizedConfigurationHashFromCluster(ctx)
	if err != nil {
		return "", false, err
	}

	if oldHash == "" {
		// Annotation not yet set on the statefulset (e.g. first time we change config).
		// We check a diff against last applied configuration to avoid triggering a restart when not needed.
		prevConfig := *config
		prevConfig.ClusterConfiguration = lastAppliedConfiguration
		oldHash, err = prevConfig.GetCentralizedConfigurationHash(schema)
		if err != nil {
			return "", false, err
		}
	}

	return hash, hash != oldHash, nil
}

func (r *ClusterReconciler) synchronizeStatusWithCluster(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	adminAPI adminutils.AdminAPIClient,
	log logr.Logger,
) (*redpandav1alpha1.ClusterCondition, error) {
	errorWithContext := newErrorWithContext(redpandaCluster.Namespace, redpandaCluster.Name)
	// Check status again on the leader using admin API
	status, err := adminAPI.ClusterConfigStatus(ctx, true)
	if err != nil {
		return nil, errorWithContext(err, "could not get config status from admin API")
	}
	conditionData := mapStatusToCondition(status)
	conditionChanged := redpandaCluster.Status.SetCondition(conditionData.Type, conditionData.Status, conditionData.Reason, conditionData.Message)
	clusterNeedsRestart := needsRestart(status, log)
	clusterSafeToRestart := isSafeToRestart(status, log)
	restartingCluster := clusterNeedsRestart && clusterSafeToRestart

	log.Info("Synchronizing configuration state for cluster",
		"status", conditionData.Status,
		"reason", conditionData.Reason,
		"message", conditionData.Message,
		"needs_restart", clusterNeedsRestart,
		"restarting", restartingCluster,
	)
	if conditionChanged || (restartingCluster && !redpandaCluster.Status.IsRestarting()) {
		log.Info("Updating configuration state for cluster")
		// Trigger restart here if needed and safe to do it
		if restartingCluster {
			redpandaCluster.Status.SetRestarting(true)
		}

		if err := r.Status().Update(ctx, redpandaCluster); err != nil {
			return nil, errorWithContext(err, "could not update condition on cluster")
		}
	}
	return redpandaCluster.Status.GetCondition(conditionData.Type), nil
}

//nolint:gocritic // I like this if else chain
func mapStatusToCondition(
	clusterStatus admin.ConfigStatusResponse,
) redpandav1alpha1.ClusterCondition {
	var condition *redpandav1alpha1.ClusterCondition
	var configVersion int64 = -1
	for _, nodeStatus := range clusterStatus {
		if len(nodeStatus.Invalid) > 0 {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonError,
				Message: fmt.Sprintf("Invalid value provided for properties: %s", strings.Join(nodeStatus.Invalid, ", ")),
			}
		} else if len(nodeStatus.Unknown) > 0 {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonError,
				Message: fmt.Sprintf("Unknown properties: %s", strings.Join(nodeStatus.Unknown, ", ")),
			}
		} else if nodeStatus.Restart {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonUpdating,
				Message: fmt.Sprintf("Node %d needs restart", nodeStatus.NodeID),
			}
		} else if configVersion >= 0 && nodeStatus.ConfigVersion != configVersion {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonUpdating,
				Message: fmt.Sprintf("Not all nodes share the same configuration version: %d / %d", nodeStatus.ConfigVersion, configVersion),
			}
		}

		configVersion = nodeStatus.ConfigVersion
	}

	if condition == nil {
		// Everything is ok
		condition = &redpandav1alpha1.ClusterCondition{
			Type:   redpandav1alpha1.ClusterConfiguredConditionType,
			Status: corev1.ConditionTrue,
		}
	}
	return *condition
}

func needsRestart(
	clusterStatus admin.ConfigStatusResponse, log logr.Logger,
) bool {
	nodeNeedsRestart := false
	for i := range clusterStatus {
		log.Info(fmt.Sprintf("Node %d restart status is %v", clusterStatus[i].NodeID, clusterStatus[i].Restart))
		if clusterStatus[i].Restart {
			nodeNeedsRestart = true
		}
	}
	return nodeNeedsRestart
}

func isSafeToRestart(
	clusterStatus admin.ConfigStatusResponse, log logr.Logger,
) bool {
	configVersions := make(map[int64]bool)
	for i := range clusterStatus {
		log.Info(fmt.Sprintf("Node %d is using config version %d", clusterStatus[i].NodeID, clusterStatus[i].ConfigVersion))
		configVersions[clusterStatus[i].ConfigVersion] = true
	}
	return len(configVersions) == 1
}

// tryMapErrorToCondition tries to map validation errors received from the cluster to a condition
// or returns the same error if not possible.
func tryMapErrorToCondition(
	err error,
) (*redpandav1alpha1.ClusterCondition, error) {
	var httpErr *admin.HTTPResponseError
	if errors.As(err, &httpErr) {
		if httpErr.Response != nil && httpErr.Response.StatusCode == http.StatusBadRequest {
			return &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonError,
				Message: string(httpErr.Body),
			}, nil
		}
	}
	return nil, err
}

func newErrorWithContext(namespace, name string) func(error, string) error {
	return func(err error, msg string) error {
		return fmt.Errorf("%s (cluster %s/%s): %w", msg, namespace, name, err)
	}
}
