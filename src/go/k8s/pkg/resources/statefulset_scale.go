// Copyright 2022 Redpanda Data, Inc.
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
	"errors"
	"fmt"

	"github.com/fluxcd/pkg/runtime/logger"
	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
)

const (
	decommissionWaitJitterFactor = 0.2
)

// handleScaling is responsible for managing the current number of replicas running for a cluster.
//
// Replicas are controlled via the field `status.currentReplicas` that is set in the current method and should be
// respected by all other external reconcile functions, including the one that sets the actual amount of replicas in the StatefulSet.
// External functions should use `cluster.getCurrentReplicas()` to get the number of expected replicas for a cluster,
// since it deals with cases where the status is not initialized yet.
//
// When users change the value of `spec.replicas` for a cluster, this function is responsible for progressively changing the `status.currentReplicas` to match that value.
// In the case of a Cluster downscaled by 1 replica (i.e. `spec.replicas` lower than current value of `status.currentReplicas` by `1`), before lowering the
// value of `status.currentReplicas`, this handler will first decommission the last node, using the admin API of the cluster.
//
// There are cases where the cluster will not decommission a node (e.g. user reduces `spec.replicas` to `2`, but there are topics with `3` partition replicas),
// in which case the draining phase will hang indefinitely in Redpanda. When this happens, the controller will not downscale the cluster and
// users will find in `status.decommissioningNode` that a node is constantly being decommissioned.
//
// In cases where the decommissioning process hangs, users can increase again the value of `spec.replicas` and the handler will contact the admin API
// to recommission the last node, ensuring that the number of replicas matches the expected value and that the node joins the cluster again.
//
// Users can change the value of `spec.replicas` freely (except it cannot be 0). In case of downscaling by multiple replicas, the handler will
// decommission one node at time, until the desired number of replicas is met.
//
// When a new cluster is created, the value of `status.currentReplicas` will be initially set to `1`, no matter what the user sets in `spec.replicas`.
// This handler will first ensure that the initial node forms a cluster, by retrieving the list of brokers using the admin API.
// After the cluster is formed, the handler will increase the `status.currentReplicas` as requested.
//
// This is due to the fact that Redpanda is currently unable to initialize a cluster if each node is given the full list of seed servers: https://github.com/redpanda-data/redpanda/issues/333.
// Previous versions of the operator use to hack the list of seeds server (in the configurator pod) of node with ordinal 0, to set it always to an empty set,
// allowing it to create an initial cluster. That strategy worked because the list of seed servers is not read again from the `redpanda.yaml` file once the
// cluster is initialized.
// But the drawback was that, on an existing running cluster, when node 0 loses its data directory (e.g. because it's using local storage, and it undergoes a k8s node upgrade),
// then node 0 (having no data and an empty seed server list in `redpanda.yaml`) creates a brand new cluster ignoring the other nodes (split-brain).
// The strategy implemented here (to initialize the cluster at 1 replica, then upscaling to the desired number, without hacks on the seed server list),
// should fix this problem, since the list of seeds servers will be the same in all nodes once the cluster is created.
//
//nolint:nestif // for clarity
func (r *StatefulSetResource) handleScaling(ctx context.Context) error {
	log := r.logger.WithName("handleScaling")

	// if a decommission is already in progress, handle it first. If it's not finished, it will return an error
	// which will requeue the reconciliation. We can't do any further scaling until it's finished.
	if err := r.handleDecommissionInProgress(ctx, log); err != nil {
		return err
	}

	if r.pandaCluster.Status.CurrentReplicas == 0 {
		// Initialize the currentReplicas field, so that it can be later controlled
		r.pandaCluster.Status.CurrentReplicas = r.pandaCluster.ComputeInitialCurrentReplicasField()
		return r.Status().Update(ctx, r.pandaCluster)
	}

	if *r.pandaCluster.Spec.Replicas == r.pandaCluster.Status.CurrentReplicas {
		r.logger.V(logger.DebugLevel).Info("No scaling changes required", "replicas", *r.pandaCluster.Spec.Replicas)
		// No changes to replicas, we do nothing here
		return nil
	}

	if *r.pandaCluster.Spec.Replicas > r.pandaCluster.Status.CurrentReplicas {
		r.logger.Info("Upscaling cluster", "replicas", *r.pandaCluster.Spec.Replicas)

		// We care about upscaling only when the cluster is moving off 1 replica, which happen e.g. at cluster startup
		if r.pandaCluster.Status.CurrentReplicas == 1 {
			r.logger.Info("Waiting for first node to form a cluster before upscaling")
			formed, err := r.isClusterFormed(ctx)
			if err != nil {
				return err
			}
			if !formed {
				return &RequeueAfterError{
					RequeueAfter: wait.Jitter(r.decommissionWaitInterval, decommissionWaitJitterFactor),
					Msg:          fmt.Sprintf("Waiting for cluster to be formed before upscaling to %d replicas", *r.pandaCluster.Spec.Replicas),
				}
			}
			r.logger.Info("Initial cluster has been formed")
		}

		// Upscaling request: this is already handled by Redpanda, so we just increase status currentReplicas
		return setCurrentReplicas(ctx, r, r.pandaCluster, *r.pandaCluster.Spec.Replicas, r.logger)
	}

	// User required replicas is lower than current replicas (currentReplicas): start the decommissioning process
	r.logger.Info("Downscaling cluster", "replicas", *r.pandaCluster.Spec.Replicas)

	targetOrdinal := r.pandaCluster.Status.CurrentReplicas - 1 // Always decommission last node
	targetBroker, err := r.getBrokerIDForPod(ctx, targetOrdinal)
	if err != nil {
		return fmt.Errorf("error getting broker ID for pod with ordinal %d when downscaling cluster: %w", targetOrdinal, err)
	}
	nonExistantBroker := int32(-1)
	if targetBroker == nil {
		// The target pod isn't in the broker list. Just select a non-existing broker for decommission so the next
		// reconcile loop will succeed.
		targetBroker = &nonExistantBroker
	}
	log.WithValues("ordinal", targetOrdinal, "node_id", targetBroker).Info("start decommission broker")
	r.pandaCluster.SetDecommissionBrokerID(targetBroker)
	err = r.Status().Update(ctx, r.pandaCluster)
	if err != nil {
		return err
	}
	if *targetBroker == nonExistantBroker {
		return &RequeueAfterError{
			RequeueAfter: RequeueDuration,
			Msg:          fmt.Sprintf("the broker for pod with ordinal %d is not registered with the cluster. Requeuing.", targetOrdinal),
		}
	}
	return nil
}

func (r *StatefulSetResource) handleDecommissionInProgress(ctx context.Context, l logr.Logger) error {
	log := l.WithName("handleDecommissionInProgress")
	if r.pandaCluster.GetDecommissionBrokerID() == nil {
		return nil
	}

	if *r.pandaCluster.Spec.Replicas >= r.pandaCluster.GetCurrentReplicas() {
		// Decommissioning can also be canceled and we need to recommission
		err := r.handleRecommission(ctx)
		if !errors.Is(err, &RecommissionFatalError{}) {
			return err
		}
		// if it's impossible to recommission, fall through and let the decommission complete
		log.WithValues("node_id", r.pandaCluster.GetDecommissionBrokerID()).Info("cannot recommission broker", "error", err)
	}
	// handleDecommission will return an error until the decommission is completed
	if err := r.handleDecommission(ctx, log); err != nil {
		return err
	}

	// Broker is now removed
	targetReplicas := r.pandaCluster.GetCurrentReplicas() - 1
	log.WithValues("targetReplicas", targetReplicas).Info("broker decommission complete: scaling down StatefulSet")

	// We set status.currentReplicas accordingly to trigger scaling down of the statefulset
	if err := setCurrentReplicas(ctx, r, r.pandaCluster, targetReplicas, r.logger); err != nil {
		return err
	}

	scaledDown, err := r.verifyRunningCount(ctx, targetReplicas)
	if err != nil {
		return err
	}
	if !scaledDown {
		return &RequeueAfterError{
			RequeueAfter: wait.Jitter(r.decommissionWaitInterval, decommissionWaitJitterFactor),
			Msg:          fmt.Sprintf("Waiting for statefulset to downscale to %d replicas", targetReplicas),
		}
	}
	return nil
}

// handleDecommission manages the case of decommissioning of the last node of a cluster.
//
// When this handler is called, the `status.decommissioningNode` is populated with the
// pod ordinal of the node that needs to be decommissioned.
//
// The handler verifies that the node is not present in the list of brokers registered in the cluster, via admin API,
// then downscales the StatefulSet via decreasing the `status.currentReplicas`.
//
// Before completing the process, it double-checks if the node is still not registered, for handling cases where the node was
// about to start when the decommissioning process started. If the broker is found, the process is restarted.
func (r *StatefulSetResource) handleDecommission(ctx context.Context, l logr.Logger) error {
	brokerID := r.pandaCluster.GetDecommissionBrokerID()
	if brokerID == nil {
		return nil
	}
	log := l.WithName("handleDecommission").WithValues("node_id", *brokerID)
	log.Info("handling broker decommissioning")

	adminAPI, err := r.getAdminAPIClient(ctx)
	if err != nil {
		return err
	}

	broker, err := getBrokerByBrokerID(ctx, *brokerID, adminAPI)
	if err != nil {
		return err
	}

	if broker == nil {
		log.Info("broker does not exist in the cluster")
		r.pandaCluster.SetDecommissionBrokerID(nil)
		return r.Status().Update(ctx, r.pandaCluster)
	}

	if broker.MembershipStatus == admin.MembershipStatusDraining {
		log.Info("broker is still draining")
		return &RequeueAfterError{
			RequeueAfter: wait.Jitter(r.decommissionWaitInterval, decommissionWaitJitterFactor),
			Msg:          fmt.Sprintf("broker %d is in the process of draining", *brokerID),
		}
	}

	// start decommissioning
	err = adminAPI.DecommissionBroker(ctx, broker.NodeID)
	if err != nil {
		return fmt.Errorf("error while trying to decommission node %d: %w", broker.NodeID, err)
	}
	log.Info("broker marked for decommissioning")
	return &RequeueAfterError{
		RequeueAfter: wait.Jitter(r.decommissionWaitInterval, decommissionWaitJitterFactor),
		Msg:          fmt.Sprintf("waiting for broker %d to be decommissioned", *brokerID),
	}
}

type RecommissionFatalError struct {
	Err string
}

func (e *RecommissionFatalError) Error() string {
	return fmt.Sprintf("recommission error: %v", e.Err)
}

// handleRecommission manages the case of a broker being recommissioned after a failed/wrong decommission.
//
// Recommission can only work for brokers that are still in the "draining" phase according to Redpanda.
//
// When this handler is triggered, `status.decommissioningNode` is populated with the broker id that was being decommissioned and
// `spec.replicas` reports a value that include a pod for that broker, indicating the intention from the user to recommission it.
//
// The handler ensures that the broker is running and also calls the admin API to recommission it.
// The process finishes when the broker is registered with redpanda and the StatefulSet is correctly scaled.
func (r *StatefulSetResource) handleRecommission(ctx context.Context) error {
	brokerID := r.pandaCluster.GetDecommissionBrokerID()
	if brokerID == nil {
		return nil
	}
	log := r.logger.WithName("handleRecommission").WithValues("node_id", *brokerID)
	log.Info("handling broker recommissioning")

	adminAPI, err := r.getAdminAPIClient(ctx)
	if err != nil {
		return err
	}

	broker, err := getBrokerByBrokerID(ctx, *r.pandaCluster.GetDecommissionBrokerID(), adminAPI)
	if err != nil {
		return err
	}

	// ensure the pod still exists
	if pod, shadowErr := r.getPodByBrokerID(ctx, brokerID); shadowErr != nil || pod == nil {
		return &RecommissionFatalError{Err: fmt.Sprintf("the pod for broker %d does not exist", *brokerID)}
	}

	if broker == nil {
		return &RecommissionFatalError{Err: fmt.Sprintf("cannot recommission broker %d: already fully decommissioned", *brokerID)}
	}

	if broker.MembershipStatus == admin.MembershipStatusActive {
		log.Info("Recommissioning process successfully completed")
		r.pandaCluster.SetDecommissionBrokerID(nil)
		return r.Status().Update(ctx, r.pandaCluster)
	}

	err = adminAPI.RecommissionBroker(ctx, int(*brokerID))
	if err != nil {
		return fmt.Errorf("error while trying to recommission broker %d: %w", *brokerID, err)
	}
	log.Info("broker being recommissioned")

	return &RequeueAfterError{
		RequeueAfter: wait.Jitter(r.decommissionWaitInterval, decommissionWaitJitterFactor),
		Msg:          fmt.Sprintf("waiting for broker %d to be recommissioned", *brokerID),
	}
}

func (r *StatefulSetResource) getAdminAPIClient(
	ctx context.Context, ordinals ...int32,
) (adminutils.AdminAPIClient, error) {
	return r.adminAPIClientFactory(ctx, r, r.pandaCluster, r.serviceFQDN, r.adminTLSConfigProvider, ordinals...)
}

func (r *StatefulSetResource) isClusterFormed(
	ctx context.Context,
) (bool, error) {
	rootNodeAdminAPI, err := r.getAdminAPIClient(ctx, 0)
	if err != nil {
		return false, err
	}
	brokers, err := rootNodeAdminAPI.Brokers(ctx)
	if err != nil {
		// Eat the error and return that the cluster is not formed
		return false, nil
	}
	return len(brokers) > 0, nil
}

// disableMaintenanceModeOnDecommissionedNodes can be used to put a cluster in a consistent state, disabling maintenance mode on
// nodes that have been decommissioned.
//
// A decommissioned node may activate maintenance mode via shutdown hooks and the cluster may enter an inconsistent state,
// preventing other pods clean shutdown.
//
// See: https://github.com/redpanda-data/redpanda/issues/4999
func (r *StatefulSetResource) disableMaintenanceModeOnDecommissionedNodes(
	ctx context.Context,
) error {
	brokerID := r.pandaCluster.GetDecommissionBrokerID()
	if brokerID == nil {
		// Only if actually in a decommissioning phase
		return nil
	}
	log := r.logger.WithName("disableMaintenanceModeOnDecommissionedNodes").WithValues("node_id", *brokerID)

	if !featuregates.MaintenanceMode(r.pandaCluster.Status.Version) {
		return nil
	}

	pod, err := r.getPodByBrokerID(ctx, brokerID)
	if err != nil {
		return err
	}

	// if there is a pod for this broker, it's not finished decommissioning
	if pod != nil {
		return nil
	}

	adminAPI, err := r.getAdminAPIClient(ctx)
	if err != nil {
		return err
	}

	log.Info("Forcing deletion of maintenance mode for the decommissioned node")
	err = adminAPI.DisableMaintenanceMode(ctx, int(*brokerID), false)
	if err != nil {
		var httpErr *admin.HTTPResponseError
		if errors.As(err, &httpErr) {
			if httpErr.Response != nil && httpErr.Response.StatusCode/100 == 4 {
				// Cluster says we don't need to do it
				log.Info("No need to disable maintenance mode on the decommissioned node", "status_code", httpErr.Response.StatusCode)
				return nil
			}
		}
		return fmt.Errorf("could not disable maintenance mode on decommissioning node %d: %w", *brokerID, err)
	}
	log.Info("Maintenance mode disabled for the decommissioned node")
	return nil
}

// verifyRunningCount checks if the statefulset is configured to run the given amount of replicas and that also pods match the expectations
func (r *StatefulSetResource) verifyRunningCount(
	ctx context.Context, replicas int32,
) (bool, error) {
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, r.Key(), &sts); err != nil {
		return false, fmt.Errorf("could not get statefulset for checking replicas: %w", err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != replicas || sts.Status.Replicas != replicas {
		return false, nil
	}

	var podList corev1.PodList
	err := r.List(ctx, &podList, &k8sclient.ListOptions{
		Namespace:     r.pandaCluster.Namespace,
		LabelSelector: labels.ForCluster(r.pandaCluster).AsClientSelector(),
	})
	if err != nil {
		return false, fmt.Errorf("could not list pods for checking replicas: %w", err)
	}

	return len(podList.Items) == int(replicas), nil
}

// getBrokerByBrokerID allows to get broker information using the admin API
func getBrokerByBrokerID(
	ctx context.Context, brokerID int32, adminAPI adminutils.AdminAPIClient,
) (*admin.Broker, error) {
	brokers, err := adminAPI.Brokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get the list of brokers for checking decommission: %w", err)
	}
	for i := range brokers {
		if brokers[i].NodeID == int(brokerID) {
			return &brokers[i], nil
		}
	}
	return nil, nil
}

// setCurrentReplicas allows to set the number of status.currentReplicas in the CR, which in turns controls the replicas
// assigned to the StatefulSet
func setCurrentReplicas(
	ctx context.Context,
	c k8sclient.Client,
	pandaCluster *vectorizedv1alpha1.Cluster,
	replicas int32,
	l logr.Logger,
) error {
	log := l.WithName("setCurrentReplicas")
	if pandaCluster.Status.CurrentReplicas == replicas {
		// Skip if already done
		return nil
	}

	log.Info("Scaling StatefulSet", "replicas", replicas)
	pandaCluster.Status.CurrentReplicas = replicas
	if err := c.Status().Update(ctx, pandaCluster); err != nil {
		return fmt.Errorf("could not scale cluster %s to %d replicas: %w", pandaCluster.Name, replicas, err)
	}
	log.Info("StatefulSet scaled", "replicas", replicas)
	return nil
}

// NodeReappearingError indicates that a node has appeared in the cluster before completion of the a direct downscale
type NodeReappearingError struct {
	NodeID int
}

// Error makes the NodeReappearingError a proper error
func (e *NodeReappearingError) Error() string {
	return fmt.Sprintf("node has appeared in the cluster with id=%d", e.NodeID)
}
