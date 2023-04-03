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

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
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
	if r.pandaCluster.Status.DecommissioningNode != nil {
		decommissionTargetReplicas := *r.pandaCluster.Status.DecommissioningNode
		if *r.pandaCluster.Spec.Replicas > decommissionTargetReplicas {
			// Decommissioning can also be canceled and we need to recommission
			return r.handleRecommission(ctx)
		}
		return r.handleDecommission(ctx)
	}

	if r.pandaCluster.Status.CurrentReplicas == 0 {
		// Initialize the currentReplicas field, so that it can be later controlled
		r.pandaCluster.Status.CurrentReplicas = r.pandaCluster.ComputeInitialCurrentReplicasField()
		return r.Status().Update(ctx, r.pandaCluster)
	}

	if *r.pandaCluster.Spec.Replicas == r.pandaCluster.Status.CurrentReplicas {
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
	targetOrdinal := r.pandaCluster.Status.CurrentReplicas - 1 // Always decommission last node
	r.logger.Info("Start decommission of last broker node", "ordinal", targetOrdinal)
	r.pandaCluster.Status.DecommissioningNode = &targetOrdinal
	return r.Status().Update(ctx, r.pandaCluster)
}

// handleDecommission manages the case of decommissioning of the last node of a cluster.
//
// When this handler is called, the `status.decommissioningNode` is populated with the pod ordinal (== nodeID) of the
// node that needs to be decommissioned.
//
// The handler verifies that the node is not present in the list of brokers registered in the cluster, via admin API,
// then downscales the StatefulSet via decreasing the `status.currentReplicas`.
//
// Before completing the process, it double-checks if the node is still not registered, for handling cases where the node was
// about to start when the decommissioning process started. If the broker is found, the process is restarted.
func (r *StatefulSetResource) handleDecommission(ctx context.Context) error {
	targetReplicas := *r.pandaCluster.Status.DecommissioningNode
	r.logger.Info("Handling cluster in decommissioning phase", "target replicas", targetReplicas)

	adminAPI, err := r.getAdminAPIClient(ctx)
	if err != nil {
		return err
	}

	broker, err := getNodeInfoFromCluster(ctx, *r.pandaCluster.Status.DecommissioningNode, adminAPI)
	if err != nil {
		return err
	}

	if broker != nil {
		r.logger.Info("Broker still exists in the cluster", "node_id", broker.NodeID, "status", broker.MembershipStatus)
		if broker.MembershipStatus != admin.MembershipStatusDraining {
			// We ask to decommission since it does not seem done
			err = adminAPI.DecommissionBroker(ctx, broker.NodeID)
			if err != nil {
				return fmt.Errorf("error while trying to decommission node %d in cluster %s: %w", broker.NodeID, r.pandaCluster.Name, err)
			}
			r.logger.Info("Node marked for decommissioning in cluster", "node_id", broker.NodeID)
		}

		// The draining phase must always be completed with all nodes running, to let single-replica partitions be transferred.
		// The value may diverge in case we restarted the process after a complete scale down.
		drainingReplicas := targetReplicas + 1
		if r.pandaCluster.Status.CurrentReplicas != drainingReplicas {
			return setCurrentReplicas(ctx, r, r.pandaCluster, drainingReplicas, r.logger)
		}

		// Wait until the node is fully drained (or wait forever if the cluster does not allow decommissioning of that specific node)
		return &RequeueAfterError{
			RequeueAfter: wait.Jitter(r.decommissionWaitInterval, decommissionWaitJitterFactor),
			Msg:          fmt.Sprintf("Waiting for node %d to be decommissioned from cluster", broker.NodeID),
		}
	}

	// Broker is now missing from cluster API
	r.logger.Info("Node is not registered in the cluster: initializing downscale", "node_id", *r.pandaCluster.Status.DecommissioningNode)

	// We set status.currentReplicas accordingly to trigger scaling down of the statefulset
	if err = setCurrentReplicas(ctx, r, r.pandaCluster, targetReplicas, r.logger); err != nil {
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

	// There's a chance that the node was initially not present in the broker list, but appeared after we started to scale down.
	// Since the node may hold data that need to be propagated to other nodes, we need to restart it to let the decommission process finish.
	broker, err = getNodeInfoFromCluster(ctx, *r.pandaCluster.Status.DecommissioningNode, adminAPI)
	if err != nil {
		return err
	}
	if broker != nil {
		// Node reappeared in the cluster, we restart the process to handle it
		return &NodeReappearingError{NodeID: broker.NodeID}
	}

	r.logger.Info("Decommissioning process successfully completed", "node_id", *r.pandaCluster.Status.DecommissioningNode)
	r.pandaCluster.Status.DecommissioningNode = nil
	return r.Status().Update(ctx, r.pandaCluster)
}

// handleRecommission manages the case of a node being recommissioned after a failed/wrong decommission.
//
// Recommission can only work for nodes that are still in the "draining" phase according to Redpanda.
//
// When this handler is triggered, `status.decommissioningNode` is populated with the node that was being decommissioned and
// `spec.replicas` reports a value that include that node, indicating the intention from the user to recommission it.
//
// The handler ensures that the node is running and also calls the admin API to recommission it.
// The process finishes when the node is registered among brokers and the StatefulSet is correctly scaled.
func (r *StatefulSetResource) handleRecommission(ctx context.Context) error {
	r.logger.Info("Handling cluster in recommissioning phase")

	// First we ensure we've enough replicas to let the recommissioning node run
	targetReplicas := *r.pandaCluster.Status.DecommissioningNode + 1
	err := setCurrentReplicas(ctx, r, r.pandaCluster, targetReplicas, r.logger)
	if err != nil {
		return err
	}

	adminAPI, err := r.getAdminAPIClient(ctx)
	if err != nil {
		return err
	}

	broker, err := getNodeInfoFromCluster(ctx, *r.pandaCluster.Status.DecommissioningNode, adminAPI)
	if err != nil {
		return err
	}

	if broker == nil || broker.MembershipStatus != admin.MembershipStatusActive {
		err = adminAPI.RecommissionBroker(ctx, int(*r.pandaCluster.Status.DecommissioningNode))
		if err != nil {
			return fmt.Errorf("error while trying to recommission node %d in cluster %s: %w", *r.pandaCluster.Status.DecommissioningNode, r.pandaCluster.Name, err)
		}
		r.logger.Info("Node marked for being recommissioned in cluster", "node_id", *r.pandaCluster.Status.DecommissioningNode)

		return &RequeueAfterError{
			RequeueAfter: wait.Jitter(r.decommissionWaitInterval, decommissionWaitJitterFactor),
			Msg:          fmt.Sprintf("Waiting for node %d to be recommissioned into cluster %s", *r.pandaCluster.Status.DecommissioningNode, r.pandaCluster.Name),
		}
	}

	r.logger.Info("Recommissioning process successfully completed", "node_id", *r.pandaCluster.Status.DecommissioningNode)
	r.pandaCluster.Status.DecommissioningNode = nil
	return r.Status().Update(ctx, r.pandaCluster)
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
	if !featuregates.MaintenanceMode(r.pandaCluster.Status.Version) {
		return nil
	}

	if r.pandaCluster.Status.DecommissioningNode == nil || r.pandaCluster.Status.CurrentReplicas > *r.pandaCluster.Status.DecommissioningNode {
		// Only if actually in a decommissioning phase
		return nil
	}

	ordinal := *r.pandaCluster.Status.DecommissioningNode
	targetReplicas := ordinal

	scaledDown, err := r.verifyRunningCount(ctx, targetReplicas)
	if err != nil || !scaledDown {
		// This should be done only when the pod disappears from the cluster
		return err
	}

	adminAPI, err := r.getAdminAPIClient(ctx)
	if err != nil {
		return err
	}

	r.logger.Info("Forcing deletion of maintenance mode for the decommissioned node", "node_id", ordinal)
	err = adminAPI.DisableMaintenanceMode(ctx, int(ordinal))
	if err != nil {
		var httpErr *admin.HTTPResponseError
		if errors.As(err, &httpErr) {
			if httpErr.Response != nil && httpErr.Response.StatusCode/100 == 4 {
				// Cluster says we don't need to do it
				r.logger.Info("No need to disable maintenance mode on the decommissioned node", "node_id", ordinal, "status_code", httpErr.Response.StatusCode)
				return nil
			}
		}
		return fmt.Errorf("could not disable maintenance mode on decommissioning node %d: %w", ordinal, err)
	}
	r.logger.Info("Maintenance mode disabled for the decommissioned node", "node_id", ordinal)
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

// getNodeInfoFromCluster allows to get broker information using the admin API
func getNodeInfoFromCluster(
	ctx context.Context, ordinal int32, adminAPI adminutils.AdminAPIClient,
) (*admin.Broker, error) {
	brokers, err := adminAPI.Brokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get the list of brokers for checking decommission: %w", err)
	}
	for i := range brokers {
		if brokers[i].NodeID == int(ordinal) {
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
	pandaCluster *redpandav1alpha1.Cluster,
	replicas int32,
	logger logr.Logger,
) error {
	if pandaCluster.Status.CurrentReplicas == replicas {
		// Skip if already done
		return nil
	}

	logger.Info("Scaling StatefulSet", "replicas", replicas)
	pandaCluster.Status.CurrentReplicas = replicas
	if err := c.Status().Update(ctx, pandaCluster); err != nil {
		return fmt.Errorf("could not scale cluster %s to %d replicas: %w", pandaCluster.Name, replicas, err)
	}
	logger.Info("StatefulSet scaled", "replicas", replicas)
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
