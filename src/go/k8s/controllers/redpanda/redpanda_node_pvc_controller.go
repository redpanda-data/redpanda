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
	"errors"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/component-helpers/storage/volume"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// +kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=redpandas,verbs=get;list;watch;
// +kubebuilder:rbac:groups=core,namespace=default,resources=persistentvolumeclaims,verbs=get;list;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch

// RedpandaNodePVCReconciler watches node objects, and sets annotation to PVC to mark them for deletion
type RedpandaNodePVCReconciler struct {
	client.Client
	OperatorMode bool
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedpandaNodePVCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).WithEventFilter(DeleteEventFilter).Complete(r)
}

func (r *RedpandaNodePVCReconciler) Reconcile(c context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, done := context.WithCancel(c)
	defer done()

	start := time.Now()
	log := ctrl.LoggerFrom(ctx).WithName("RedpandaNodePVCReconciler.Reconcile")

	Infof(log, "Node %q was found to be deleted, checking for existing PVCs", req.Name)

	result, err := r.reconcile(ctx, req)

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	log.Info(durationMsg)

	return result, err
}

// nolint:funlen,unparam // the length is ok
func (r *RedpandaNodePVCReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithName("RedpandaNodePVCReconciler.reconcile")
	Infof(log, "detected node %s deleted; checking if any PVC should be removed", req.Name)

	redpandaNameList := make([]string, 0)
	if r.OperatorMode {
		opts := &client.ListOptions{Namespace: req.Namespace}
		redpandaList := &v1alpha1.RedpandaList{}
		if errList := r.Client.List(ctx, redpandaList, opts); errList != nil {
			return ctrl.Result{}, fmt.Errorf("could not GET list of Redpandas in namespace: %w", errList)
		}

		for i := range redpandaList.Items {
			item := redpandaList.Items[i]
			redpandaNameList = append(redpandaNameList, item.Name)
		}
	} else {
		releaseName, ok := os.LookupEnv(EnvHelmReleaseNameKey)
		if !ok {
			Infof(log, "Skipping reconciliation, expected to find release-name env var: %q", EnvHelmReleaseNameKey)
			return ctrl.Result{}, nil
		}

		redpandaNameList = append(redpandaNameList, releaseName)
	}

	opts := &client.ListOptions{Namespace: req.Namespace}
	pvcList := &corev1.PersistentVolumeClaimList{}
	if errGET := r.Client.List(ctx, pvcList, opts); errGET != nil {
		return ctrl.Result{}, fmt.Errorf("could not GET list of PVCs: %w", errGET)
	}

	var errs error
	// creating map of release name to sideCarEnabled
	doTaskMap := make(map[string]bool, 0)
	// this could be a lot of redpandas, usually 1 but may be a few
	for i := range pvcList.Items {
		pvc := pvcList.Items[i]

		// first check if the application label is redpanda, if not continue
		if len(pvc.Labels) == 0 {
			continue
		}

		if _, ok := pvc.Labels[K8sNameLabelKey]; !ok {
			continue
		}

		if len(pvc.Annotations) == 0 {
			continue
		}

		// Now we check if the node where the PVC was originally located was deleted
		// if it is, then we change the PV to change the reclaim policy and then mark the pvc for deletion
		workerNode, ok := pvc.Annotations[volume.AnnSelectedNode]

		if !ok || workerNode == "" {
			errs = errors.Join(errs, fmt.Errorf("worker node annotation not found or node name is empty: %q", workerNode)) //nolint:goerr113 // joining since we do not error here
		}

		// we are not being deleted, move on
		if workerNode != req.Name {
			continue
		}

		// Now check if we are allowed to operate on this pvc as there may be another controller working
		var releaseName string
		if val, ok := pvc.Labels[K8sInstanceLabelKey]; !ok || !isValidReleaseName(val, redpandaNameList) {
			Infof(log, "could not find instance label or unable retrieve valid releaseName: %s", val)
			continue
		} else {
			releaseName = val
		}

		// we are in operator mode, check if another controller has ownership here
		// we will silently fail where we cannot find get values, parse the values file etc..
		if r.OperatorMode { // nolint:nestif // this is ok
			doTask, foundKey := doTaskMap[releaseName]
			if foundKey && !doTask {
				continue
			}

			if !foundKey {
				valuesMap, err := getHelmValues(log, releaseName, req.Namespace)
				if err != nil {
					Infof(log, "could not retrieve values for release %q, probably not a valid managed helm release: %s", releaseName, err)
					continue
				}

				enabledControllerSideCar, okSideCar, errGetBool := unstructured.NestedBool(valuesMap, "statefulset", "sideCars", "controllers", "enabled")
				if errGetBool != nil {
					Infof(log, "could not retrieve sideCar state for release %q: %s", releaseName, errGetBool)
					continue
				}

				doTaskMap[releaseName] = !enabledControllerSideCar
				if okSideCar && enabledControllerSideCar {
					log.Info("another controller has ownership, moving on")
					continue
				}
			}
		}

		// we are being deleted, before moving forward, try to update PV to avoid data loss
		// this is by best effort, if we cannot, then we move on,
		pvName := pvc.Spec.VolumeName
		bestTrySetRetainPV(r.Client, log, ctx, pvName, req.Namespace)

		// now we are ready to delete PVC
		if deleteErr := r.Client.Delete(ctx, &pvc); deleteErr != nil {
			errs = errors.Join(errs, fmt.Errorf("could not delete PVC %q: %w", pvc.Name, deleteErr)) //nolint:goerr113 // joining since we do not error here
		}
	}

	return ctrl.Result{}, errs
}
