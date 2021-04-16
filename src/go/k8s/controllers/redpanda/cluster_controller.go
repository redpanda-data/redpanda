// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package redpanda contains reconciliation logic for redpanda.vectorized.io CRD
package redpanda

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/certmanager"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	errNonexistentLastObservesState = errors.New("expecting to have statefulset LastObservedState set but it's nil")
	errNodePortMissing              = errors.New("the node port is missing from the service")
)

// ClusterReconciler reconciles a Cluster object
type ClusterReconciler struct {
	client.Client
	Log             logr.Logger
	configuratorTag string
	Scheme          *runtime.Scheme
}

//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;
//+kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates;clusterissuers,verbs=create;get;list;watch;patch;delete;

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the Cluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
func (r *ClusterReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := r.Log.WithValues("redpandacluster", req.NamespacedName)

	log.Info(fmt.Sprintf("Starting reconcile loop for %v", req.NamespacedName))
	defer log.Info(fmt.Sprintf("Finished reconcile loop for %v", req.NamespacedName))

	var redpandaCluster redpandav1alpha1.Cluster
	crb := resources.NewClusterRoleBinding(r.Client, &redpandaCluster, r.Scheme, log)
	if err := r.Get(ctx, req.NamespacedName, &redpandaCluster); err != nil {
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		if apierrors.IsNotFound(err) {
			if removeError := crb.RemoveSubject(ctx, req.NamespacedName); removeError != nil {
				return ctrl.Result{}, fmt.Errorf("unable to remove subject in ClusterroleBinding: %w", removeError)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unable to retrieve Cluster resource: %w", err)
	}

	nodeports := []resources.NamedServicePort{}
	internalListener := redpandaCluster.InternalListener()
	externalListener := redpandaCluster.ExternalListener()
	adminAPIInternal := redpandaCluster.AdminAPIInternal()
	adminAPIExternal := redpandaCluster.AdminAPIExternal()
	if externalListener != nil {
		nodeports = append(nodeports, resources.NamedServicePort{Name: externalListener.Name, Port: internalListener.Port + 1})
	}
	if adminAPIExternal != nil {
		nodeports = append(nodeports, resources.NamedServicePort{Name: resources.AdminPortName, Port: adminAPIInternal.Port + 1})
	}
	headlessPorts := []resources.NamedServicePort{
		{Name: resources.AdminPortName, Port: adminAPIInternal.Port},
		{Name: internalListener.Name, Port: internalListener.Port},
	}
	headlessSvc := resources.NewHeadlessService(r.Client, &redpandaCluster, r.Scheme, headlessPorts, log)
	nodeportSvc := resources.NewNodePortService(r.Client, &redpandaCluster, r.Scheme, nodeports, log)

	pki := certmanager.NewPki(r.Client, &redpandaCluster, headlessSvc.HeadlessServiceFQDN(), r.Scheme, log)
	sa := resources.NewServiceAccount(r.Client, &redpandaCluster, r.Scheme, log)
	sts := resources.NewStatefulSet(
		r.Client,
		&redpandaCluster,
		r.Scheme,
		headlessSvc.HeadlessServiceFQDN(),
		headlessSvc.Key().Name,
		nodeportSvc.Key(),
		pki.NodeCert(),
		pki.OperatorClientCert(),
		pki.AdminCert(),
		pki.AdminAPINodeCert(),
		pki.AdminAPIClientCert(),
		sa.Key().Name,
		r.configuratorTag,
		log)
	toApply := []resources.Reconciler{
		headlessSvc,
		nodeportSvc,
		resources.NewConfigMap(r.Client, &redpandaCluster, r.Scheme, headlessSvc.HeadlessServiceFQDN(), log),
		pki,
		sa,
		resources.NewClusterRole(r.Client, &redpandaCluster, r.Scheme, log),
		crb,
		sts,
	}

	for _, res := range toApply {
		err := res.Ensure(ctx)

		var e *resources.RequeueAfterError
		if errors.As(err, &e) {
			log.Info(e.Error())
			return ctrl.Result{RequeueAfter: e.RequeueAfter}, nil
		}

		if err != nil {
			log.Error(err, "Failed to reconcile resource")
			return ctrl.Result{}, err
		}
	}

	err := r.reportStatus(ctx, &redpandaCluster, sts.LastObservedState, headlessSvc.HeadlessServiceFQDN(), nodeportSvc.Key())
	if err != nil {
		log.Error(err, "Unable to report status")
	}

	return ctrl.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

func (r *ClusterReconciler) reportStatus(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	lastObservedSts *appsv1.StatefulSet,
	internalFQDN string,
	nodeportSvcName types.NamespacedName,
) error {
	var observedPods corev1.PodList

	err := r.List(ctx, &observedPods, &client.ListOptions{
		LabelSelector: labels.ForCluster(redpandaCluster).AsClientSelector(),
		Namespace:     redpandaCluster.Namespace,
	})
	if err != nil {
		return fmt.Errorf("unable to fetch PodList resource: %w", err)
	}

	observedNodesInternal := make([]string, 0, len(observedPods.Items))
	// nolint:gocritic // the copies are necessary for further redpandacluster updates
	for _, item := range observedPods.Items {
		observedNodesInternal = append(observedNodesInternal, fmt.Sprintf("%s.%s", item.Name, internalFQDN))
	}

	observedNodesExternal, observedExternalAdmin, err := r.createExternalNodesList(ctx, observedPods.Items, redpandaCluster, nodeportSvcName)
	if err != nil {
		return fmt.Errorf("failed to construct external node list: %w", err)
	}

	if lastObservedSts == nil {
		return errNonexistentLastObservesState
	}

	if statusShouldBeUpdated(&redpandaCluster.Status, observedNodesInternal, observedNodesExternal, lastObservedSts.Status.ReadyReplicas) {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var cluster redpandav1alpha1.Cluster
			err := r.Get(ctx, types.NamespacedName{
				Name:      redpandaCluster.Name,
				Namespace: redpandaCluster.Namespace,
			}, &cluster)
			if err != nil {
				return err
			}

			cluster.Status.Nodes.Internal = observedNodesInternal
			cluster.Status.Nodes.External = observedNodesExternal
			cluster.Status.Nodes.ExternalAdmin = observedExternalAdmin
			cluster.Status.Replicas = lastObservedSts.Status.ReadyReplicas

			return r.Status().Update(ctx, &cluster)
		})

		if err != nil {
			return fmt.Errorf("failed to update cluster status: %w", err)
		}
	}
	return nil
}

func statusShouldBeUpdated(
	status *redpandav1alpha1.ClusterStatus,
	nodesInternal, nodesExternal []string,
	readyReplicas int32,
) bool {
	return !reflect.DeepEqual(nodesInternal, status.Nodes.Internal) ||
		!reflect.DeepEqual(nodesExternal, status.Nodes.External) ||
		status.Replicas != readyReplicas
}

// WithConfiguratorTag set the configuratorTag
func (r *ClusterReconciler) WithConfiguratorTag(
	configuratorTag string,
) *ClusterReconciler {
	r.configuratorTag = configuratorTag
	return r
}

func (r *ClusterReconciler) createExternalNodesList(
	ctx context.Context,
	pods []corev1.Pod,
	pandaCluster *redpandav1alpha1.Cluster,
	nodePortName types.NamespacedName,
) (external, externalAdmin []string, err error) {
	externalKafkaListener := pandaCluster.ExternalListener()
	externalAdminListener := pandaCluster.AdminAPIExternal()
	if externalKafkaListener == nil && externalAdminListener == nil {
		return []string{}, []string{}, nil
	}

	var nodePortSvc corev1.Service
	if err := r.Get(ctx, nodePortName, &nodePortSvc); err != nil {
		return []string{}, []string{}, fmt.Errorf("failed to retrieve node port service %s: %w", nodePortName, err)
	}

	// we now support only one external kafka and admin port
	expectedPortLength := 2
	if externalAdminListener == nil || externalKafkaListener == nil {
		expectedPortLength = 1
	}
	if len(nodePortSvc.Spec.Ports) != expectedPortLength {
		return []string{}, []string{}, fmt.Errorf("node port service %s: %w", nodePortName, errNodePortMissing)
	}

	for _, port := range nodePortSvc.Spec.Ports {
		if port.NodePort == 0 {
			return []string{}, []string{}, fmt.Errorf("node port service %s, port %s is 0: %w", nodePortName, port.Name, errNodePortMissing)
		}
	}

	var node corev1.Node
	observedNodesExternal := make([]string, 0, len(pods))
	observedNodesExternalAdmin := make([]string, 0, len(pods))
	for i := range pods {
		prefixLen := len(pods[i].GenerateName)
		podName := pods[i].Name[prefixLen:]

		if externalKafkaListener != nil && needExternalIP(externalKafkaListener.External) ||
			externalAdminListener != nil && needExternalIP(externalAdminListener.External) {
			if err := r.Get(ctx, types.NamespacedName{Name: pods[i].Spec.NodeName}, &node); err != nil {
				return []string{}, []string{}, fmt.Errorf("failed to retrieve node %s: %w", pods[i].Spec.NodeName, err)
			}
		}

		if externalKafkaListener != nil && len(externalKafkaListener.External.Subdomain) > 0 {
			address := subdomainAddress(podName, externalKafkaListener.External.Subdomain, getNodePort(&nodePortSvc, externalKafkaListener.Name))
			observedNodesExternal = append(observedNodesExternal, address)
		} else if externalKafkaListener != nil {
			observedNodesExternal = append(observedNodesExternal,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, externalKafkaListener.Name),
				))
		}
		if externalAdminListener != nil && len(externalKafkaListener.External.Subdomain) > 0 {
			address := subdomainAddress(podName, externalAdminListener.External.Subdomain, getNodePort(&nodePortSvc, resources.AdminPortName))
			observedNodesExternalAdmin = append(observedNodesExternalAdmin, address)
		} else if externalAdminListener != nil {
			observedNodesExternalAdmin = append(observedNodesExternalAdmin,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.AdminPortName),
				))
		}
	}
	return observedNodesExternal, observedNodesExternalAdmin, nil
}

func needExternalIP(external redpandav1alpha1.ExternalConnectivityConfig) bool {
	return external.Subdomain == ""
}

func subdomainAddress(name, subdomain string, port int32) string {
	return fmt.Sprintf("%s.%s:%d",
		name,
		subdomain,
		port,
	)
}

func getExternalIP(node *corev1.Node) string {
	if node == nil {
		return ""
	}
	for _, address := range node.Status.Addresses {
		if address.Type == corev1.NodeExternalIP {
			return address.Address
		}
	}
	return ""
}

func getNodePort(svc *corev1.Service, name string) int32 {
	if svc == nil {
		return -1
	}
	for _, port := range svc.Spec.Ports {
		if port.Name == name && port.NodePort != 0 {
			return port.NodePort
		}
	}
	return 0
}
