// Copyright 2023 Redpanda Data, Inc.
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
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// +kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=redpandas,verbs=get;list;watch;
// +kubebuilder:rbac:groups=core,namespace=default,resources=pods,verbs=get;list;watch;
// +kubebuilder:rbac:groups=core,namespace=default,resources=persistentvolumeclaims,verbs=get;list;update;patch;delete;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;update;patch;watch
// +kubebuilder:rbac:groups=apps,namespace=default,resources=statefulsets/status,verbs=update;patch

const (
	DecommissionCondition = "DecommissionPhase"

	DecomConditionFalseReasonMsg   = "Decommission process is in waiting phase."
	DecomConditionTrueReasonMsg    = "Decommission process is actively running."
	DecomConditionUnknownReasonMsg = "Decommission process has completed or in an unknown state."
)

var ConditionUnknown = appsv1.StatefulSetCondition{
	Type:    DecommissionCondition,
	Status:  corev1.ConditionUnknown,
	Message: DecomConditionUnknownReasonMsg,
}

type DecommissionReconciler struct {
	client.Client
	OperatorMode bool
}

// SetupWithManager sets up the controller with the Manager.
func (r *DecommissionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).WithEventFilter(UpdateEventFilter).Complete(r)
}

func (r *DecommissionReconciler) Reconcile(c context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, done := context.WithCancel(c)
	defer done()

	start := time.Now()
	log := ctrl.LoggerFrom(ctx).WithName("DecommissionReconciler.Reconcile")

	sts := &appsv1.StatefulSet{}
	if err := r.Client.Get(ctx, req.NamespacedName, sts); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not retrieve the statefulset: %w", err)
	}

	// Examine if the object is under deletion
	if !sts.ObjectMeta.DeletionTimestamp.IsZero() {
		log.Info(fmt.Sprintf("the statefulset %q is being deleted", req.NamespacedName))
		return ctrl.Result{}, nil
	}

	decomCondition, _ := getConditionOfTypeAndListWithout(DecommissionCondition, sts.Status.Conditions)
	if decomCondition == nil {
		decomCondition = &ConditionUnknown
	}

	var err error
	var result ctrl.Result

	switch decomCondition.Status {
	case corev1.ConditionUnknown:
		// we have been notified, check to see if we need to decommission
		result, err = r.verifyIfNeedDecommission(ctx, sts)
	case corev1.ConditionFalse:
		// we have verified we need to decommission, so we need to start, update the condition to do so
		patch := client.MergeFrom(sts.DeepCopy())

		// create condition here
		newCondition := appsv1.StatefulSetCondition{
			Type:               DecommissionCondition,
			Status:             corev1.ConditionTrue,
			Message:            DecomConditionTrueReasonMsg,
			Reason:             "Waiting period over, ready to start Decommission process.",
			LastTransitionTime: metav1.Now(),
		}

		if updatedConditions, ok := updateStatefulSetDecommissionConditions(&newCondition, sts.Status.Conditions); ok {
			sts.Status.Conditions = updatedConditions
			if errPatch := r.Client.Status().Patch(ctx, sts, patch); errPatch != nil {
				return ctrl.Result{}, fmt.Errorf("unable to update sts status %q with condition: %w", sts.Name, errPatch)
			}
			log.Info("Updating true condition successfully")
		}
		result = ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}
	case corev1.ConditionTrue:
		// condition updated to true, so we proceed to decommission
		log.Info("decommission started")
		result, err = r.reconcileDecommission(ctx, sts)
	}

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("succesfull reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	if err != nil {
		durationMsg = fmt.Sprintf("found error, we will requeue, reconciliation attempt finished in %s", time.Since(start).String())
	}

	log.Info(durationMsg)

	return result, err
}

// verifyIfNeedDecommission checks if decommission is necessary or not.
// The checks happen in a few steps:
// 1. We first check whether statefulset is a redpanda statefulset or not
// 2. If we are, we check if another processes is already in chart of decommission
// 3. We then check to see if we have contact with the adminAPI and verify health
// 4. We ensure we are in a good state by seeing if requestedReplicas and health.AllNodes are neither one empty
// 5. Check that the AdminAPI is reporting that we have more brokers than the number of requestedReplicas of the sts resource
// 6. If we have this situation, we are most likely decommission since we have a signal of scaling, set condition and requeue
// The requeue process at the end of the above process allows time the node to get enter maintenance mode.
// nolint:funlen // the length is ok
func (r *DecommissionReconciler) verifyIfNeedDecommission(ctx context.Context, sts *appsv1.StatefulSet) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithName("DecommissionReconciler.verifyIfNeedDecommission")
	Infof(log, "verify if we need to decommission: %s/%s", sts.Namespace, sts.Name)

	namespace := sts.Namespace

	if len(sts.Labels) == 0 {
		return ctrl.Result{}, nil
	}

	// if helm is not managing it, move on.
	if managedBy, ok := sts.Labels[K8sManagedByLabelKey]; managedBy != "Helm" || !ok {
		Infof(log, "not managed by helm, moving on: managed-by: %s, ok: %t", managedBy, ok)
		return ctrl.Result{}, nil
	}

	redpandaNameList := make([]string, 0)
	if r.OperatorMode {
		opts := &client.ListOptions{Namespace: namespace}
		redpandaList := &v1alpha1.RedpandaList{}
		if errGET := r.Client.List(ctx, redpandaList, opts); errGET != nil {
			return ctrl.Result{}, fmt.Errorf("could not GET list of Redpandas in namespace: %w", errGET)
		}

		for i := range redpandaList.Items {
			item := redpandaList.Items[i]
			redpandaNameList = append(redpandaNameList, item.Name)
		}
	} else {
		releaseName, ok := os.LookupEnv(EnvHelmReleaseNameKey)
		if !ok {
			log.Info(fmt.Sprintf("Skipping reconciliation, expected to find release-name env var: %q", EnvHelmReleaseNameKey))
			return ctrl.Result{}, nil
		}

		redpandaNameList = append(redpandaNameList, releaseName)
	}

	var releaseName string
	if val, ok := sts.Labels[K8sInstanceLabelKey]; !ok || !isValidReleaseName(val, redpandaNameList) {
		Infof(log, "could not find instance label or unable retrieve valid releaseName: %s", val)
		return ctrl.Result{}, nil
	} else {
		releaseName = val
	}

	requestedReplicas := pointer.Int32Deref(sts.Spec.Replicas, 0)

	valuesMap, err := getHelmValues(log, releaseName, namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not retrieve values, probably not a valid managed helm release: %w", err)
	}

	// we are in operator mode, check if another controller has ownership here
	if r.OperatorMode {
		enabledControllerSideCar, ok, errGetBool := unstructured.NestedBool(valuesMap, "statefulset", "sideCars", "controllers", "enabled")
		if errGetBool != nil {
			return ctrl.Result{}, fmt.Errorf("could not retrieve sideCar state: %w", errGetBool)
		}
		if ok && enabledControllerSideCar {
			log.Info("another controller has ownership, moving on ")
			return ctrl.Result{}, nil
		}
	}

	adminAPI, err := buildAdminAPI(releaseName, namespace, requestedReplicas, valuesMap)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile, error creating adminapi: %w", err)
	}

	health, err := watchClusterHealth(ctx, adminAPI)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not make request to admin-api: %w", err)
	}

	// strange error case here
	if requestedReplicas == 0 || len(health.AllNodes) == 0 {
		Infof(log, "requested replicas %q, or number of nodes registered %q are invalid, stopping reconciliation", requestedReplicas, health.AllNodes)
		return ctrl.Result{}, nil
	}

	Debugf(log, "health is found to be %+v", health)

	Infof(log, "all-nodes/requestedReps: %d/%d", len(health.AllNodes), int(requestedReplicas))
	if len(health.AllNodes) > int(requestedReplicas) {
		log.Info("we are downscaling, attempt to add condition with status false")
		// we are in decommission mode, we should probably wait here some time to verify
		// that we are committed and after x amount of time perform the decommission task after.

		patch := client.MergeFrom(sts.DeepCopy())
		// create condition here
		newCondition := appsv1.StatefulSetCondition{
			Type:               DecommissionCondition,
			Status:             corev1.ConditionFalse,
			Message:            DecomConditionFalseReasonMsg,
			Reason:             "Need for Decommission detected",
			LastTransitionTime: metav1.Now(),
		}

		updatedConditions, ok := updateStatefulSetDecommissionConditions(&newCondition, sts.Status.Conditions)
		if ok {
			sts.Status.Conditions = updatedConditions
			if errPatch := r.Client.Status().Patch(ctx, sts, patch); errPatch != nil {
				return ctrl.Result{}, fmt.Errorf("unable to update sts status %q with condition: %w", sts.Name, errPatch)
			}
			log.Info("Updating false condition successfully")
		}

		log.Info("we are entering decommission and updated conditions, waiting to begin")
		// we exit but requeue to allow actual decommission later
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// reconcileDecommission performs decommission task after verifying that we should decommission the sts given
// 1. After requeue from decommission due to condition we have set, now we verify and perform tasks.
// 2. Retrieve sts information again, this time focusing on replicas and state
// 3. If we observe that we have not completed deletion, we requeue
// 4. We wait until the cluster is not ready, else requeue, this means that we have reached a steady state that we can proceed from
// 5. As in previous function, we build adminAPI client and get values files
// 6. Check if we have more nodes registered than requested, proceed since this is the first clue we need to decommission
// 7. We are in steady state, proceed if we have more or the same number of downed nodes then are in allNodes registered minus requested
// 8. For all the downed nodes, we get decommission-status, since we have waited for steady state we should be OK to do so
// 9. Any failures captured will force us to requeue and try again.
// 10. Attempt to delete the pvc and retain volumes if possible.
// 11. Finally, reset condition state to unknown if we have been successful so far.
//
//nolint:funlen // length looks good
func (r *DecommissionReconciler) reconcileDecommission(ctx context.Context, sts *appsv1.StatefulSet) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithName("DecommissionReconciler.reconcileDecommission")
	Infof(log, "reconciling: %s/%s", sts.Namespace, sts.Name)

	namespace := sts.Namespace

	releaseName, ok := sts.Labels[K8sInstanceLabelKey]
	if !ok {
		log.Info("could not find instance label to retrieve releaseName")
		return ctrl.Result{}, nil
	}

	requestedReplicas := pointer.Int32Deref(sts.Spec.Replicas, 0)
	statusReplicas := sts.Status.Replicas
	availableReplicas := sts.Status.AvailableReplicas

	// we have started decommission, but we want to requeue if we have not transitioned here. This should
	// avoid decommissioning the wrong node (broker) id
	if statusReplicas != requestedReplicas && sts.Status.UpdatedReplicas == 0 {
		log.Info("have not finished terminating and restarted largest ordinal, requeue here")
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
	}

	// This helps avoid decommissioning nodes that are starting up where, say, a node has been removed
	// and you need to move it and start a new one
	if availableReplicas != 0 {
		log.Info("have not reached steady state yet, requeue here")
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
	}

	valuesMap, err := getHelmValues(log, releaseName, namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not retrieve values, probably not a valid managed helm release: %w", err)
	}

	adminAPI, err := buildAdminAPI(releaseName, namespace, requestedReplicas, valuesMap)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile, error creating adminAPI: %w", err)
	}

	health, err := watchClusterHealth(ctx, adminAPI)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not make request to admin-api: %w", err)
	}

	// decommission looks like this:
	// 1) replicas = 2, and health: AllNodes:[0 1 2] NodesDown:[2]
	// will not heal on its own, we need to remove these downed nodes
	// 2) Downed node was replaced due to node being removed,

	if requestedReplicas == 0 || len(health.AllNodes) == 0 {
		return ctrl.Result{}, nil
	}

	var errList error
	// nolint:nestif // this is ok
	if len(health.AllNodes) > int(requestedReplicas) {
		// we are in decommission mode

		// first check if we have a controllerID before we perform the decommission, else requeue immediately
		// this happens when the controllerID node is being terminated, may show more than one node down at this point
		if health.ControllerID < 0 {
			log.Info("controllerID is not defined yet, we will requeue")
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
		}

		// perform decommission on down down-nodes but only if down nodes match count of all-nodes-replicas
		// the greater case takes care of the situation where we may also have additional ids here.
		if len(health.NodesDown) >= (len(health.AllNodes) - int(requestedReplicas)) {
			// TODO guard against intermittent situations where a node is coming up after it being brought down
			// how do we get a signal of this, it would be easy if we can compare previous situation
			for i := range health.NodesDown {
				item := health.NodesDown[i]

				// Now we check the decommission status before continuing
				doDecommission := false
				status, decommStatusError := adminAPI.DecommissionBrokerStatus(ctx, item)
				if decommStatusError != nil {
					Infof(log, "error found for decommission status: %s", decommStatusError.Error())
					// nolint:gocritic // no need for a switch, this is ok
					if strings.Contains(decommStatusError.Error(), "is not decommissioning") {
						doDecommission = true
					} else if strings.Contains(decommStatusError.Error(), "does not exists") {
						Infof(log, "nodeID %d does not exist, skipping: %s", item, decommStatusError.Error())
					} else {
						errList = errors.Join(errList, fmt.Errorf("could get decommission status of broker: %w", decommStatusError))
					}
				}
				Debugf(log, "decommission status: %v", status)

				if doDecommission {
					Infof(log, "all checks pass, attempting to decommission: %d", item)
					// we want a clear signal to avoid 400s here, the suspicion here is an invalid transitional state
					decomErr := adminAPI.DecommissionBroker(ctx, item)
					if decomErr != nil && !strings.Contains(decomErr.Error(), "failed: Not Found") && !strings.Contains(decomErr.Error(), "failed: Bad Request") {
						errList = errors.Join(errList, fmt.Errorf("could not decommission broker: %w", decomErr))
					}
				}
			}
		}
	}

	// now we check pvcs
	if err = r.reconcilePVCs(log.WithName("DecommissionReconciler.reconcilePVCs"), ctx, sts, valuesMap); err != nil {
		errList = errors.Join(errList, fmt.Errorf("could not reconcile pvcs: %w", err))
	}

	if errList != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, fmt.Errorf("found errors %w", errList)
	}

	// now we need to
	patch := client.MergeFrom(sts.DeepCopy())
	// create condition here
	newCondition := appsv1.StatefulSetCondition{
		Type:               DecommissionCondition,
		Status:             corev1.ConditionUnknown,
		Message:            DecomConditionUnknownReasonMsg,
		Reason:             "Decommission completed",
		LastTransitionTime: metav1.Now(),
	}

	if updatedConditions, isUpdated := updateStatefulSetDecommissionConditions(&newCondition, sts.Status.Conditions); isUpdated {
		sts.Status.Conditions = updatedConditions
		if errPatch := r.Client.Status().Patch(ctx, sts, patch); errPatch != nil {
			return ctrl.Result{}, fmt.Errorf("unable to update sts status %q condition: %w", sts.Name, errPatch)
		}
		log.Info("Updating unknown condition successfully, was able to decommission")
	}

	return ctrl.Result{}, nil
}

func (r *DecommissionReconciler) reconcilePVCs(log logr.Logger, ctx context.Context, sts *appsv1.StatefulSet, valuesMap map[string]interface{}) error {
	Infof(log, "reconciling: %s/%s", sts.Namespace, sts.Name)

	Infof(log, "checking storage")
	persistentStorageEnabled, ok, err := unstructured.NestedBool(valuesMap, "storage", "persistentVolume", "enabled")
	if !ok || err != nil {
		return fmt.Errorf("could not retrieve persistent storage settings: %w", err)
	}

	if !persistentStorageEnabled {
		return nil
	}

	log.Info("persistent storage enabled, checking if we need to remove something")
	podLabels := client.MatchingLabels{}

	for k, v := range sts.Spec.Template.Labels {
		podLabels[k] = v
	}

	podOpts := []client.ListOption{
		client.InNamespace(sts.Namespace),
		podLabels,
	}

	podList := &corev1.PodList{}
	if listPodErr := r.Client.List(ctx, podList, podOpts...); listPodErr != nil {
		return fmt.Errorf("could not list pods: %w", listPodErr)
	}

	templates := sts.Spec.VolumeClaimTemplates
	var vctLabels map[string]string
	for i := range templates {
		template := templates[i]
		if template.Name == "datadir" {
			vctLabels = template.Labels
			break
		}
	}

	vctMatchingLabels := client.MatchingLabels{}

	for k, v := range vctLabels {
		// TODO is this expected
		vctMatchingLabels[k] = v
		if k == K8sComponentLabelKey {
			vctMatchingLabels[k] = fmt.Sprintf("%s-statefulset", v)
		}
	}

	vctOpts := []client.ListOption{
		client.InNamespace(sts.Namespace),
		vctMatchingLabels,
	}

	// find the dataDir template
	// now cycle through pvcs, retain volumes for future but delete claims
	pvcList := &corev1.PersistentVolumeClaimList{}
	if listErr := r.Client.List(ctx, pvcList, vctOpts...); listErr != nil {
		return fmt.Errorf("could not get pvc list: %w", listErr)
	}

	pvcsBound := make(map[string]bool, 0)
	for i := range pvcList.Items {
		item := pvcList.Items[i]
		pvcsBound[item.Name] = false
	}

	for j := range podList.Items {
		pod := podList.Items[j]
		// skip pods that are being terminated
		if pod.GetDeletionTimestamp() != nil {
			continue
		}
		volumes := pod.Spec.Volumes
		for i := range volumes {
			volume := volumes[i]
			if volume.VolumeSource.PersistentVolumeClaim != nil {
				pvcsBound[volume.VolumeSource.PersistentVolumeClaim.ClaimName] = true
			}
		}
	}

	Infof(log, "pvc name list, binding processed: %+v", pvcsBound)

	if pvcErrorList := r.tryToDeletePVC(log, ctx, pointer.Int32Deref(sts.Spec.Replicas, 0), pvcsBound, pvcList); pvcErrorList != nil {
		return fmt.Errorf("errors found: %w", pvcErrorList)
	}

	return nil
}

func (r *DecommissionReconciler) tryToDeletePVC(log logr.Logger, ctx context.Context, replicas int32, isBoundList map[string]bool, pvcList *corev1.PersistentVolumeClaimList) error {
	var pvcErrorList error

	// here we sort the list of items, should be ordered by ordinal, and we remove the last first so we sort first then remove
	// only the first n matching the number of replicas
	keys := make([]string, 0)
	for k := range isBoundList {
		keys = append(keys, k)
	}

	// sort the pvc strings
	sort.Strings(keys)

	// remove first
	keys = keys[int(replicas):]

	Debugf(log, "pvcs to delete: %+v", keys)

	// TODO: may want to not processes cases where we have more than 1 pvcs, so we force the 1 node at a time policy
	// this will avoid dead locking the cluster since you cannot add new nodes, or decomm

	for i := range pvcList.Items {
		item := pvcList.Items[i]

		if isBoundList[item.Name] || !isNameInList(item.Name, keys) {
			continue
		}

		// we are being deleted, before moving forward, try to update PV to avoid data loss
		bestTrySetRetainPV(r.Client, log, ctx, item.Spec.VolumeName, item.Namespace)

		Debugf(log, "getting ready to remove %s", item.Name)

		// now we are ready to delete PVC
		if errDelete := r.Client.Delete(ctx, &item); errDelete != nil {
			pvcErrorList = errors.Join(pvcErrorList, fmt.Errorf("could not delete PVC %q: %w", item.Name, errDelete))
		}
	}

	return pvcErrorList
}

func isNameInList(name string, keys []string) bool {
	for i := range keys {
		if name == keys[i] {
			return true
		}
	}
	return false
}

func buildAdminAPI(releaseName, namespace string, replicas int32, values map[string]interface{}) (*admin.AdminAPI, error) {
	tlsEnabled, ok, err := unstructured.NestedBool(values, "tls", "enabled")
	if !ok || err != nil {
		// probably not a correct helm release, bail
		return nil, fmt.Errorf("tlsEnabled found not to be ok %t, err: %w", tlsEnabled, err)
	}

	// need some additional checks to see if this is a redpanda

	// Now try to either use the URL if it is not empty or build the service name to get the sts information
	// proper command, note that we are using curl and ignoring tls, we will continue
	// curl -k https://redpanda-2.redpanda.redpanda.svc.cluster.local.:9644/v1/cluster/health_overview
	// sample response:

	// this can be <scheme>://<release-name>.<namespace>.svc.cluster.local.:<ipAddress>
	// http scheme will be determined by tls option below:

	var tlsConfig *tls.Config = nil
	if tlsEnabled {
		//nolint:gosec // will not pull secrets unless working in chart
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	urls, err := createBrokerURLs(releaseName, namespace, replicas, values)
	if err != nil {
		return nil, fmt.Errorf("could not create broker url: %w", err)
	}

	// TODO we do not tls, but we may need sasl items here.
	return admin.NewAdminAPI(urls, admin.BasicCredentials{}, tlsConfig)
}

func createBrokerURLs(release, namespace string, replicas int32, values map[string]interface{}) ([]string, error) {
	brokerList := make([]string, 0)

	fullnameOverride, ok, err := unstructured.NestedString(values, "fullnameOverride")
	if !ok || err != nil {
		return brokerList, fmt.Errorf("could not retrieve fullnameOverride: %s; error %w", fullnameOverride, err)
	}

	serviceName := release
	if fullnameOverride != "" {
		serviceName = fullnameOverride
	}

	// unstructured things that ports are flot64 types this is by design for json conversion
	portFloat, ok, err := unstructured.NestedFloat64(values, "listeners", "admin", "port")
	if !ok || err != nil {
		return brokerList, fmt.Errorf("could not retrieve admin port %f, error: %w", portFloat, err)
	}

	port := int(portFloat)

	domain, ok, err := unstructured.NestedString(values, "clusterDomain")
	if !ok || err != nil {
		return brokerList, fmt.Errorf("could not retrieve clusterDomain: %s; error: %w", domain, err)
	}

	for i := 0; i < int(replicas); i++ {
		brokerList = append(brokerList, fmt.Sprintf("%s-%d.%s.%s.svc.%s:%d", release, i, serviceName, namespace, domain, port))
	}

	return brokerList, nil
}

func watchClusterHealth(ctx context.Context, adminAPI *admin.AdminAPI) (*admin.ClusterHealthOverview, error) {
	start := time.Now()
	stop := start.Add(60 * time.Second)

	var health admin.ClusterHealthOverview
	var err error
	for time.Now().Before(stop) {
		health, err = adminAPI.GetHealthOverview(ctx)

		if err == nil && len(health.NodesDown) > 0 {
			return &health, err
		}
		time.Sleep(2) // nolint:staticcheck // this is ok, we do not want hit the api too hard
	}

	return &health, err
}

func updateStatefulSetDecommissionConditions(toAdd *appsv1.StatefulSetCondition, conditions []appsv1.StatefulSetCondition) ([]appsv1.StatefulSetCondition, bool) {
	if len(conditions) == 0 {
		conditions = make([]appsv1.StatefulSetCondition, 0)
		conditions = append(conditions, *toAdd)
		return conditions, true
	}

	oldCondition, newConditions := getConditionOfTypeAndListWithout(toAdd.Type, conditions)
	if oldCondition == nil {
		newConditions = append(newConditions, *toAdd)
		return newConditions, true
	}

	switch oldCondition.Status {
	case corev1.ConditionUnknown:
		// we can only transition to a "false" state, leave old condition alone
		if toAdd.Status != corev1.ConditionFalse {
			return conditions, false
		}
	case corev1.ConditionFalse:
		if toAdd.Status != corev1.ConditionTrue {
			// we can only transition to a "true" state, leave old alone otherwise
			return conditions, false
		}
	case corev1.ConditionTrue:
		if toAdd.Status != corev1.ConditionUnknown {
			// we can only transition to a "unknown" state, leave old alone otherwise
			return conditions, false
		}
	}

	newConditions = append(newConditions, *toAdd)
	return newConditions, true
}

func getConditionOfTypeAndListWithout(conditionType appsv1.StatefulSetConditionType, conditions []appsv1.StatefulSetCondition) (oldCondition *appsv1.StatefulSetCondition, newConditions []appsv1.StatefulSetCondition) {
	if len(conditions) == 0 {
		return nil, conditions
	}

	newConditions = make([]appsv1.StatefulSetCondition, 0)
	for i := range conditions {
		c := conditions[i]
		// we expect only to have one decommission condition
		if c.Type == conditionType {
			oldCondition = &c
			continue
		}
		newConditions = append(newConditions, c)
	}

	return oldCondition, newConditions
}
