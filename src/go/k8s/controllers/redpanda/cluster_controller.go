// Copyright 2021 Redpanda Data, Inc.
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
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/networking"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

const (
	PodAnnotationNodeIDKey = "operator.redpanda.com/node-id"
	FinalizerKey           = "operator.redpanda.com/finalizer"

	SecretAnnotationExternalCAKey = "operator.redpanda.com/external-ca"
)

var (
	errNonexistentLastObservesState = errors.New("expecting to have statefulset LastObservedState set but it's nil")
	errNodePortMissing              = errors.New("the node port is missing from the service")
	errInvalidImagePullPolicy       = errors.New("invalid image pull policy")
)

// ClusterReconciler reconciles a Cluster object
type ClusterReconciler struct {
	client.Client
	Log                       logr.Logger
	configuratorSettings      resources.ConfiguratorSettings
	clusterDomain             string
	Scheme                    *runtime.Scheme
	AdminAPIClientFactory     adminutils.AdminAPIClientFactory
	DecommissionWaitInterval  time.Duration
	MetricsTimeout            time.Duration
	RestrictToRedpandaVersion string
	allowPVCDeletion          bool
}

//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates;clusterissuers,verbs=create;get;list;watch;patch;delete;update;
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;delete;
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;update;delete
//+kubebuilder:rbac:groups=core,resources=pods/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;
//+kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=create;get;list;watch;patch;delete;update;
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=create;get;list;watch;patch;delete;update;
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the Cluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
//
//nolint:funlen,gocyclo // todo break down
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
	// if the cluster is being deleted, delete finalizers
	if !redpandaCluster.GetDeletionTimestamp().IsZero() {
		return r.handleClusterDeletion(ctx, &redpandaCluster, log)
	}

	// if the cluster isn't being deleted, add a finalizer
	if !controllerutil.ContainsFinalizer(&redpandaCluster, FinalizerKey) {
		log.V(7).Info("adding finalizer")
		controllerutil.AddFinalizer(&redpandaCluster, FinalizerKey)
		if err := r.Update(ctx, &redpandaCluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("unable to set Cluster finalizer: %w", err)
		}
	}
	// set a finalizer on the pods so we can have the data needed to decommission them
	if err := r.handlePodFinalizer(ctx, &redpandaCluster, log); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting pod finalizer: %w", err)
	}

	if !isRedpandaClusterManaged(log, &redpandaCluster) {
		return ctrl.Result{}, nil
	}

	if !isRedpandaClusterVersionManaged(log, &redpandaCluster, r.RestrictToRedpandaVersion) {
		return ctrl.Result{}, nil
	}

	redpandaPorts := networking.NewRedpandaPorts(&redpandaCluster)
	nodeports := collectNodePorts(redpandaPorts)
	headlessPorts := collectHeadlessPorts(redpandaPorts)
	lbPorts := collectLBPorts(redpandaPorts)
	clusterPorts := collectClusterPorts(redpandaPorts, &redpandaCluster)

	headlessSvc := resources.NewHeadlessService(r.Client, &redpandaCluster, r.Scheme, headlessPorts, log)
	nodeportSvc := resources.NewNodePortService(r.Client, &redpandaCluster, r.Scheme, nodeports, log)
	bootstrapSvc := resources.NewLoadBalancerService(r.Client, &redpandaCluster, r.Scheme, lbPorts, true, log)

	clusterSvc := resources.NewClusterService(r.Client, &redpandaCluster, r.Scheme, clusterPorts, log)
	subdomain := ""
	var ppIngressConfig *redpandav1alpha1.IngressConfig
	proxyAPIExternal := redpandaCluster.PandaproxyAPIExternal()
	if proxyAPIExternal != nil {
		subdomain = proxyAPIExternal.External.Subdomain
		ppIngressConfig = proxyAPIExternal.External.Ingress
	}
	ingress := resources.NewIngress(r.Client,
		&redpandaCluster,
		r.Scheme,
		subdomain,
		clusterSvc.Key().Name,
		resources.PandaproxyPortExternalName,
		log).
		WithAnnotations(map[string]string{resources.SSLPassthroughAnnotation: "true"}).
		WithUserConfig(ppIngressConfig)

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
	sa := resources.NewServiceAccount(r.Client, &redpandaCluster, r.Scheme, log)
	configMapResource := resources.NewConfigMap(r.Client, &redpandaCluster, r.Scheme, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), proxySuKey, schemaRegistrySuKey, pki.BrokerTLSConfigProvider(), log)
	secretResource := resources.PreStartStopScriptSecret(r.Client, &redpandaCluster, r.Scheme, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), proxySuKey, schemaRegistrySuKey, log)

	sts := resources.NewStatefulSet(
		r.Client,
		&redpandaCluster,
		r.Scheme,
		headlessSvc.HeadlessServiceFQDN(r.clusterDomain),
		headlessSvc.Key().Name,
		nodeportSvc.Key(),
		pki.StatefulSetVolumeProvider(),
		pki.AdminAPIConfigProvider(),
		sa.Key().Name,
		r.configuratorSettings,
		configMapResource.GetNodeConfigHash,
		r.AdminAPIClientFactory,
		r.DecommissionWaitInterval,
		log,
		r.MetricsTimeout)

	toApply := []resources.Reconciler{
		headlessSvc,
		clusterSvc,
		nodeportSvc,
		ingress,
		bootstrapSvc,
		proxySu,
		schemaRegistrySu,
		configMapResource,
		secretResource,
		pki,
		sa,
		resources.NewClusterRole(r.Client, &redpandaCluster, r.Scheme, log),
		crb,
		resources.NewPDB(r.Client, &redpandaCluster, r.Scheme, log),
		sts,
	}

	for _, res := range toApply {
		err = res.Ensure(ctx)

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

	adminAPI, err := r.AdminAPIClientFactory(ctx, r.Client, &redpandaCluster, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), pki.AdminAPIConfigProvider())
	if err != nil && !errors.Is(err, &adminutils.NoInternalAdminAPI{}) {
		return ctrl.Result{}, fmt.Errorf("creating admin api client: %w", err)
	}

	var secrets []types.NamespacedName
	if proxySu != nil {
		secrets = append(secrets, proxySu.Key())
	}
	if schemaRegistrySu != nil {
		secrets = append(secrets, schemaRegistrySu.Key())
	}

	if errSetInit := r.setInitialSuperUserPassword(ctx, adminAPI, secrets); errSetInit != nil {
		// we capture all errors here, do not return the error, just requeue
		log.Error(errSetInit, "failed to set initial super user password")
		return ctrl.Result{RequeueAfter: resources.RequeueDuration}, nil
	}

	schemaRegistryPort := config.DefaultSchemaRegPort
	if redpandaCluster.Spec.Configuration.SchemaRegistry != nil {
		schemaRegistryPort = redpandaCluster.Spec.Configuration.SchemaRegistry.Port
	}
	err = r.reportStatus(
		ctx,
		&redpandaCluster,
		sts,
		headlessSvc.HeadlessServiceFQDN(r.clusterDomain),
		clusterSvc.ServiceFQDN(r.clusterDomain),
		schemaRegistryPort,
		nodeportSvc.Key(),
		bootstrapSvc.Key(),
	)
	if err != nil {
		return ctrl.Result{}, err
	}

	err = r.reconcileConfiguration(
		ctx,
		&redpandaCluster,
		configMapResource,
		sts,
		pki,
		headlessSvc.HeadlessServiceFQDN(r.clusterDomain),
		log,
	)
	var requeueErr *resources.RequeueAfterError
	if errors.As(err, &requeueErr) {
		log.Info(requeueErr.Error())
		return ctrl.Result{RequeueAfter: requeueErr.RequeueAfter}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if featuregates.CentralizedConfiguration(redpandaCluster.Spec.Version) {
		if cc := redpandaCluster.Status.GetCondition(redpandav1alpha1.ClusterConfiguredConditionType); cc == nil || cc.Status != corev1.ConditionTrue {
			return ctrl.Result{RequeueAfter: time.Minute * 1}, nil
		}
	}
	// The following should be at the last part as it requires AdminAPI to be running
	if err := r.setPodNodeIDAnnotation(ctx, &redpandaCluster, log); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting pod node_id annotation: %w", err)
	}

	// want: refactor above to resources (i.e. setInitialSuperUserPassword, reconcileConfiguration)
	// ensuring license must be at the end when condition ClusterConfigured=true and AdminAPI is ready
	license := resources.NewLicense(r.Client, r.Scheme, &redpandaCluster, adminAPI, log)
	if err := license.Ensure(ctx); err != nil {
		var raErr *resources.RequeueAfterError
		if errors.As(err, &raErr) {
			log.Info(raErr.Error())
			return ctrl.Result{RequeueAfter: raErr.RequeueAfter}, nil
		}
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := validateImagePullPolicy(r.configuratorSettings.ImagePullPolicy); err != nil {
		return fmt.Errorf("invalid image pull policy \"%s\": %w", r.configuratorSettings.ImagePullPolicy, err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Watches(
			&source.Kind{Type: &corev1.Pod{}},
			handler.EnqueueRequestsFromMapFunc(r.reconcileClusterForPods),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&source.Kind{Type: &corev1.Secret{}},
			handler.EnqueueRequestsFromMapFunc(r.reconcileClusterForExternalCASecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Complete(r)
}

func validateImagePullPolicy(imagePullPolicy corev1.PullPolicy) error {
	switch imagePullPolicy {
	case corev1.PullAlways:
	case corev1.PullIfNotPresent:
	case corev1.PullNever:
	default:
		return fmt.Errorf("available image pull policy: \"%s\", \"%s\" or \"%s\": %w", corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever, errInvalidImagePullPolicy)
	}
	return nil
}

//nolint:funlen,gocyclo // refactor in the next iteration
func (r *ClusterReconciler) handlePodFinalizer(
	ctx context.Context, rp *redpandav1alpha1.Cluster, log logr.Logger,
) error {
	pods, err := r.podList(ctx, rp)
	if err != nil {
		return fmt.Errorf("unable to fetch PodList: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp.IsZero() {
			// if the pod is not being deleted, set the finalizer
			if err = r.setPodFinalizer(ctx, pod, log); err != nil {
				//nolint:goerr113 // not going to use wrapped static error here this time
				return fmt.Errorf(`unable to set the finalizer on pod "%s": %d`, pod.Name, err)
			}
			continue
		}
		// if the pod is being deleted
		// check the node it's assigned to
		node := corev1.Node{}
		key := types.NamespacedName{Name: pod.Spec.NodeName}
		err := r.Get(ctx, key, &node)
		// if the node is not gone
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf(`unable to fetch node "%s": %w`, pod.Spec.NodeName, err)
		}
		if err == nil {
			// nor has a noexecute taint
			untainted := true
			for _, taint := range node.Spec.Taints {
				if taint.Effect == corev1.TaintEffectNoExecute && taint.Key == corev1.TaintNodeUnreachable {
					untainted = false
				}
			}
			if untainted {
				// remove the finalizer and let the pod be restarted
				if err = r.removePodFinalizer(ctx, pod, log); err != nil {
					return fmt.Errorf(`unable to remove finalizer from pod "%s": %w`, pod.Name, err)
				}
				continue
			}
		}
		// get the node id
		nodeIDStr, ok := pod.GetAnnotations()[PodAnnotationNodeIDKey]
		if !ok {
			return fmt.Errorf("cannot determine node_id for pod %s: %w. not removing finalizer", pod.Name, err)
		}
		nodeID, err := strconv.Atoi(nodeIDStr)
		if err != nil {
			return fmt.Errorf("node-id annotation is not an integer: %w", err)
		}
		// get the broker list
		redpandaPorts := networking.NewRedpandaPorts(rp)
		headlessPorts := collectHeadlessPorts(redpandaPorts)
		clusterPorts := collectClusterPorts(redpandaPorts, rp)
		headlessSvc := resources.NewHeadlessService(r.Client, rp, r.Scheme, headlessPorts, log)
		clusterSvc := resources.NewClusterService(r.Client, rp, r.Scheme, clusterPorts, log)

		pki, err := certmanager.NewPki(ctx, r.Client, rp, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), clusterSvc.ServiceFQDN(r.clusterDomain), r.Scheme, log)
		if err != nil {
			return fmt.Errorf("creating pki: %w", err)
		}

		adminClient, err := r.AdminAPIClientFactory(ctx, r.Client, rp, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), pki.AdminAPIConfigProvider())
		if err != nil {
			return fmt.Errorf("unable to create admin client: %w", err)
		}
		brokers, err := adminClient.Brokers(ctx)
		if err != nil {
			return fmt.Errorf("unable to fetch brokers: %w", err)
		}
		// check if the node in the broker list
		var broker *admin.Broker
		for i := range brokers {
			if brokers[i].NodeID == nodeID {
				broker = &brokers[i]
				break
			}
		}
		// if it's not gone
		if broker != nil {
			// decommission it
			log.WithValues("node-id", nodeID).Info("decommissioning broker")
			if err = adminClient.DecommissionBroker(ctx, nodeID); err != nil {
				return fmt.Errorf(`unable to decommission node "%d": %w`, nodeID, err)
			}
		}

		if !r.allowPVCDeletion {
			//   remove the finalizer
			if err = r.removePodFinalizer(ctx, pod, log); err != nil {
				return fmt.Errorf(`unable to remove finalizer from pod "%s/%s: %w"`, pod.GetNamespace(), pod.GetName(), err)
			}
			return nil
		}
		//   delete the associated pvc
		pvc := corev1.PersistentVolumeClaim{}
		//nolint: gocritic // 248 bytes 6 times is not worth decreasing the readability over
		for _, v := range pod.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				key = types.NamespacedName{
					Name:      v.PersistentVolumeClaim.ClaimName,
					Namespace: pod.GetNamespace(),
				}
				err = r.Get(ctx, key, &pvc)
				if err != nil {
					if !apierrors.IsNotFound(err) {
						return fmt.Errorf(`unable to fetch PersistentVolumeClaim "%s/%s": %w`, key.Namespace, key.Name, err)
					}
					continue
				}
				log.WithValues("persistent-volume-claim", key).Info("deleting PersistentVolumeClaim")
				if err := r.Delete(ctx, &pvc, &client.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf(`unable to delete PersistentVolumeClaim "%s/%s": %w`, key.Name, key.Namespace, err)
				}
			}
		}
		//   remove the finalizer
		if err := r.removePodFinalizer(ctx, pod, log); err != nil {
			return fmt.Errorf(`unable to remove finalizer from pod "%s/%s: %w"`, pod.GetNamespace(), pod.GetName(), err)
		}
	}
	return nil
}

func (r *ClusterReconciler) removePodFinalizer(
	ctx context.Context, pod *corev1.Pod, log logr.Logger,
) error {
	if controllerutil.ContainsFinalizer(pod, FinalizerKey) {
		log.V(7).WithValues("namespace", pod.Namespace, "name", pod.Name).Info("removing finalizer")
		controllerutil.RemoveFinalizer(pod, FinalizerKey)
		if err := r.Update(ctx, pod); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterReconciler) setPodFinalizer(
	ctx context.Context, pod *corev1.Pod, log logr.Logger,
) error {
	if !controllerutil.ContainsFinalizer(pod, FinalizerKey) {
		log.V(7).WithValues("namespace", pod.Namespace, "name", pod.Name).Info("adding finalizer")
		controllerutil.AddFinalizer(pod, FinalizerKey)
		if err := r.Update(ctx, pod); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterReconciler) setPodNodeIDAnnotation(
	ctx context.Context, rp *redpandav1alpha1.Cluster, log logr.Logger,
) error {
	log.V(6).Info("setting pod node-id annotation")
	pods, err := r.podList(ctx, rp)
	if err != nil {
		return fmt.Errorf("unable to fetch PodList: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		nodeIDStr, ok := pod.Annotations[PodAnnotationNodeIDKey]

		nodeID, err := r.fetchAdminNodeID(ctx, rp, pod, log)
		if err != nil {
			return fmt.Errorf(`cannot fetch node id for "%s" node-id annotation: %w`, pod.Name, err)
		}

		realNodeIDStr := fmt.Sprintf("%d", nodeID)

		if ok && realNodeIDStr == nodeIDStr {
			continue
		}

		var oldNodeID int
		if ok {
			oldNodeID, err = strconv.Atoi(nodeIDStr)
			if err != nil {
				return fmt.Errorf("unable to convert node ID (%s) to int: %w", nodeIDStr, err)
			}

			log.WithValues("pod-name", pod.Name, "old-node-id", oldNodeID).Info("decommission old node-id")
			if err = r.decommissionBroker(ctx, rp, oldNodeID, log); err != nil {
				return fmt.Errorf("unable to decommission broker: %w", err)
			}
		}

		log.WithValues("pod-name", pod.Name, "new-node-id", nodeID).Info("setting node-id annotation")
		pod.Annotations[PodAnnotationNodeIDKey] = realNodeIDStr
		if err := r.Update(ctx, pod, &client.UpdateOptions{}); err != nil {
			return fmt.Errorf(`unable to update pod "%s" with node-id annotation: %w`, pod.Name, err)
		}
	}
	return nil
}

func (r *ClusterReconciler) decommissionBroker(
	ctx context.Context, rp *redpandav1alpha1.Cluster, nodeID int, log logr.Logger,
) error {
	log.V(6).WithValues("node-id", nodeID).Info("decommission broker")

	redpandaPorts := networking.NewRedpandaPorts(rp)
	headlessPorts := collectHeadlessPorts(redpandaPorts)
	clusterPorts := collectClusterPorts(redpandaPorts, rp)
	headlessSvc := resources.NewHeadlessService(r.Client, rp, r.Scheme, headlessPorts, log)
	clusterSvc := resources.NewClusterService(r.Client, rp, r.Scheme, clusterPorts, log)

	pki, err := certmanager.NewPki(ctx, r.Client, rp, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), clusterSvc.ServiceFQDN(r.clusterDomain), r.Scheme, log)
	if err != nil {
		return fmt.Errorf("unable to create pki: %w", err)
	}

	adminClient, err := r.AdminAPIClientFactory(ctx, r.Client, rp, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), pki.AdminAPIConfigProvider())
	if err != nil {
		return fmt.Errorf("unable to create admin client: %w", err)
	}

	err = adminClient.DecommissionBroker(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("unable to decommission broker: %w", err)
	}
	return nil
}

func (r *ClusterReconciler) fetchAdminNodeID(ctx context.Context, rp *redpandav1alpha1.Cluster, pod *corev1.Pod, log logr.Logger) (int32, error) {
	redpandaPorts := networking.NewRedpandaPorts(rp)
	headlessPorts := collectHeadlessPorts(redpandaPorts)
	clusterPorts := collectClusterPorts(redpandaPorts, rp)
	headlessSvc := resources.NewHeadlessService(r.Client, rp, r.Scheme, headlessPorts, log)
	clusterSvc := resources.NewClusterService(r.Client, rp, r.Scheme, clusterPorts, log)

	pki, err := certmanager.NewPki(ctx, r.Client, rp, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), clusterSvc.ServiceFQDN(r.clusterDomain), r.Scheme, log)
	if err != nil {
		return -1, fmt.Errorf("creating pki: %w", err)
	}

	ordinal, err := utils.GetPodOrdinal(pod.Name, rp.Name)
	if err != nil {
		return -1, fmt.Errorf("cluster %s: cannot convert pod name (%s) to ordinal: %w", rp.Name, pod.Name, err)
	}

	adminClient, err := r.AdminAPIClientFactory(ctx, r.Client, rp, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), pki.AdminAPIConfigProvider(), int32(ordinal))
	if err != nil {
		return -1, fmt.Errorf("unable to create admin client: %w", err)
	}
	cfg, err := adminClient.GetNodeConfig(ctx)
	if err != nil {
		return -1, fmt.Errorf("unable to fetch /v1/node_config from %s: %w", pod.Name, err)
	}
	return int32(cfg.NodeID), nil
}

func (r *ClusterReconciler) reportStatus(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	sts *resources.StatefulSetResource,
	internalFQDN string,
	clusterFQDN string,
	schemaRegistryPort int,
	nodeportSvcName types.NamespacedName,
	bootstrapSvcName types.NamespacedName,
) error {
	observedPods, err := r.podList(ctx, redpandaCluster)
	if err != nil {
		return fmt.Errorf("unable to fetch PodList: %w", err)
	}

	observedNodesInternal := make([]string, 0, len(observedPods.Items))
	//nolint:gocritic // the copies are necessary for further redpandacluster updates
	for _, item := range observedPods.Items {
		observedNodesInternal = append(observedNodesInternal, fmt.Sprintf("%s.%s", item.Name, internalFQDN))
	}

	nodeList, err := r.createExternalNodesList(ctx, observedPods.Items, redpandaCluster, nodeportSvcName, bootstrapSvcName)
	if err != nil {
		return fmt.Errorf("failed to construct external node list: %w", err)
	}

	if sts.LastObservedState == nil {
		return errNonexistentLastObservesState
	}

	if nodeList == nil {
		nodeList = &redpandav1alpha1.NodesList{
			SchemaRegistry: &redpandav1alpha1.SchemaRegistryStatus{},
		}
	}
	nodeList.Internal = observedNodesInternal
	nodeList.SchemaRegistry.Internal = fmt.Sprintf("%s:%d", clusterFQDN, schemaRegistryPort)

	if statusShouldBeUpdated(&redpandaCluster.Status, nodeList, sts) {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var cluster redpandav1alpha1.Cluster
			err := r.Get(ctx, types.NamespacedName{
				Name:      redpandaCluster.Name,
				Namespace: redpandaCluster.Namespace,
			}, &cluster)
			if err != nil {
				return err
			}

			cluster.Status.Nodes = *nodeList
			cluster.Status.ReadyReplicas = sts.LastObservedState.Status.ReadyReplicas
			cluster.Status.Replicas = sts.LastObservedState.Status.Replicas
			cluster.Status.Version = sts.Version()

			err = r.Status().Update(ctx, &cluster)
			if err == nil {
				// sync original cluster variable to avoid conflicts on subsequent operations
				*redpandaCluster = cluster
			}
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to update cluster status: %w", err)
		}
	}
	return nil
}

func statusShouldBeUpdated(
	status *redpandav1alpha1.ClusterStatus,
	nodeList *redpandav1alpha1.NodesList,
	sts *resources.StatefulSetResource,
) bool {
	return nodeList != nil &&
		(!reflect.DeepEqual(nodeList.Internal, status.Nodes.Internal) ||
			!reflect.DeepEqual(nodeList.External, status.Nodes.External) ||
			!reflect.DeepEqual(nodeList.ExternalAdmin, status.Nodes.ExternalAdmin) ||
			!reflect.DeepEqual(nodeList.ExternalPandaproxy, status.Nodes.ExternalPandaproxy) ||
			!reflect.DeepEqual(nodeList.SchemaRegistry, status.Nodes.SchemaRegistry) ||
			!reflect.DeepEqual(nodeList.ExternalBootstrap, status.Nodes.ExternalBootstrap)) ||
		status.Replicas != sts.LastObservedState.Status.Replicas ||
		status.ReadyReplicas != sts.LastObservedState.Status.ReadyReplicas ||
		status.Version != sts.Version()
}

func (r *ClusterReconciler) podList(ctx context.Context, redpandaCluster *redpandav1alpha1.Cluster) (corev1.PodList, error) {
	var observedPods corev1.PodList

	err := r.List(ctx, &observedPods, &client.ListOptions{
		LabelSelector: labels.ForCluster(redpandaCluster).AsClientSelector(),
		Namespace:     redpandaCluster.Namespace,
	})
	if err != nil {
		return observedPods, fmt.Errorf("unable to fetch PodList resource: %w", err)
	}

	return observedPods, nil
}

func (r *ClusterReconciler) reconcileClusterForPods(pod client.Object) []reconcile.Request {
	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Namespace: pod.GetNamespace(),
				Name:      pod.GetLabels()[labels.InstanceKey],
			},
		},
	}
}

func (r *ClusterReconciler) reconcileClusterForExternalCASecret(s client.Object) []reconcile.Request {
	hasExternalCA, found := s.GetAnnotations()[SecretAnnotationExternalCAKey]
	if !found || hasExternalCA != "true" {
		return nil
	}

	clusterName, found := s.GetLabels()[labels.InstanceKey]
	if !found {
		return nil
	}

	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: s.GetNamespace(),
			Name:      clusterName,
		},
	}}
}

// WithConfiguratorSettings set the configurator image settings
func (r *ClusterReconciler) WithConfiguratorSettings(
	configuratorSettings resources.ConfiguratorSettings,
) *ClusterReconciler {
	r.configuratorSettings = configuratorSettings
	return r
}

// WithClusterDomain set the clusterDomain
func (r *ClusterReconciler) WithClusterDomain(
	clusterDomain string,
) *ClusterReconciler {
	r.clusterDomain = clusterDomain
	return r
}

func (r *ClusterReconciler) WithAllowPVCDeletion(
	allowPVCDeletion bool,
) *ClusterReconciler {
	r.allowPVCDeletion = allowPVCDeletion
	return r
}

//nolint:funlen,gocyclo // External nodes list should be refactored
func (r *ClusterReconciler) createExternalNodesList(
	ctx context.Context,
	pods []corev1.Pod,
	pandaCluster *redpandav1alpha1.Cluster,
	nodePortName types.NamespacedName,
	bootstrapName types.NamespacedName,
) (*redpandav1alpha1.NodesList, error) {
	externalKafkaListener := pandaCluster.ExternalListener()
	externalAdminListener := pandaCluster.AdminAPIExternal()
	externalProxyListener := pandaCluster.PandaproxyAPIExternal()
	schemaRegistryConf := pandaCluster.Spec.Configuration.SchemaRegistry
	if externalKafkaListener == nil && externalAdminListener == nil && externalProxyListener == nil &&
		(schemaRegistryConf == nil || !pandaCluster.IsSchemaRegistryExternallyAvailable()) {
		return nil, nil
	}

	var nodePortSvc corev1.Service
	if err := r.Get(ctx, nodePortName, &nodePortSvc); err != nil {
		return nil, fmt.Errorf("failed to retrieve node port service %s: %w", nodePortName, err)
	}

	for _, port := range nodePortSvc.Spec.Ports {
		if port.NodePort == 0 {
			return nil, fmt.Errorf("node port service %s, port %s is 0: %w", nodePortName, port.Name, errNodePortMissing)
		}
	}

	var node corev1.Node
	result := &redpandav1alpha1.NodesList{
		External:           make([]string, 0, len(pods)),
		ExternalAdmin:      make([]string, 0, len(pods)),
		ExternalPandaproxy: make([]string, 0, len(pods)),
		SchemaRegistry: &redpandav1alpha1.SchemaRegistryStatus{
			Internal:        "",
			External:        "",
			ExternalNodeIPs: make([]string, 0, len(pods)),
		},
	}

	for i := range pods {
		pod := pods[i]

		if externalKafkaListener != nil && needExternalIP(externalKafkaListener.External) ||
			externalAdminListener != nil && needExternalIP(externalAdminListener.External) ||
			externalProxyListener != nil && needExternalIP(externalProxyListener.External.ExternalConnectivityConfig) ||
			schemaRegistryConf != nil && schemaRegistryConf.External != nil && needExternalIP(*schemaRegistryConf.GetExternal()) {
			if err := r.Get(ctx, types.NamespacedName{Name: pods[i].Spec.NodeName}, &node); err != nil {
				return nil, fmt.Errorf("failed to retrieve node %s: %w", pods[i].Spec.NodeName, err)
			}
		}

		if externalKafkaListener != nil && len(externalKafkaListener.External.Subdomain) > 0 {
			address, err := subdomainAddress(externalKafkaListener.External.EndpointTemplate, &pod, externalKafkaListener.External.Subdomain, getNodePort(&nodePortSvc, resources.ExternalListenerName))
			if err != nil {
				return nil, err
			}
			result.External = append(result.External, address)
		} else if externalKafkaListener != nil {
			result.External = append(result.External,
				fmt.Sprintf("%s:%d",
					networking.GetPreferredAddress(&node, corev1.NodeAddressType(externalKafkaListener.External.PreferredAddressType)),
					getNodePort(&nodePortSvc, resources.ExternalListenerName),
				))
		}

		if externalAdminListener != nil && len(externalAdminListener.External.Subdomain) > 0 {
			address, err := subdomainAddress(externalAdminListener.External.EndpointTemplate, &pod, externalAdminListener.External.Subdomain, getNodePort(&nodePortSvc, resources.AdminPortExternalName))
			if err != nil {
				return nil, err
			}
			result.ExternalAdmin = append(result.ExternalAdmin, address)
		} else if externalAdminListener != nil {
			result.ExternalAdmin = append(result.ExternalAdmin,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.AdminPortExternalName),
				))
		}

		if externalProxyListener != nil && len(externalProxyListener.External.Subdomain) > 0 {
			address, err := subdomainAddress(externalProxyListener.External.EndpointTemplate, &pod, externalProxyListener.External.Subdomain, getNodePort(&nodePortSvc, resources.PandaproxyPortExternalName))
			if err != nil {
				return nil, err
			}
			result.ExternalPandaproxy = append(result.ExternalPandaproxy, address)
		} else if externalProxyListener != nil {
			result.ExternalPandaproxy = append(result.ExternalPandaproxy,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.PandaproxyPortExternalName),
				))
		}

		if schemaRegistryConf != nil && schemaRegistryConf.External != nil && needExternalIP(*schemaRegistryConf.GetExternal()) {
			result.SchemaRegistry.ExternalNodeIPs = append(result.SchemaRegistry.ExternalNodeIPs,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.SchemaRegistryPortName),
				))
		}
	}

	if schemaRegistryConf != nil && schemaRegistryConf.External != nil && len(schemaRegistryConf.External.Subdomain) > 0 {
		prefix := ""
		if schemaRegistryConf.External.Endpoint != "" {
			prefix = fmt.Sprintf("%s.", schemaRegistryConf.External.Endpoint)
		}
		result.SchemaRegistry.External = fmt.Sprintf("%s%s:%d",
			prefix,
			schemaRegistryConf.External.Subdomain,
			getNodePort(&nodePortSvc, resources.SchemaRegistryPortName),
		)
	}

	if externalProxyListener != nil && len(externalProxyListener.External.Subdomain) > 0 {
		result.PandaproxyIngress = &externalProxyListener.External.Subdomain
	}

	if externalKafkaListener.External.Bootstrap != nil {
		var bootstrapSvc corev1.Service
		if err := r.Get(ctx, bootstrapName, &bootstrapSvc); err != nil {
			return nil, fmt.Errorf("failed to retrieve bootstrap lb service %s: %w", bootstrapName, err)
		}
		result.ExternalBootstrap = &redpandav1alpha1.LoadBalancerStatus{
			LoadBalancerStatus: bootstrapSvc.Status.LoadBalancer,
		}
	}
	return result, nil
}

func (r *ClusterReconciler) handleClusterDeletion(
	ctx context.Context, redpandaCluster *redpandav1alpha1.Cluster, log logr.Logger,
) (reconcile.Result, error) {
	if controllerutil.ContainsFinalizer(redpandaCluster, FinalizerKey) {
		log.V(7).Info("removing finalizers")
		pods, err := r.podList(ctx, redpandaCluster)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("unable to list Pods: %w", err)
		}
		for i := range pods.Items {
			if err := r.removePodFinalizer(ctx, &pods.Items[i], log); err != nil {
				return ctrl.Result{}, fmt.Errorf(`unable to remove finalizer for pod "%s": %w`, pods.Items[i].GetName(), err)
			}
		}
		controllerutil.RemoveFinalizer(redpandaCluster, FinalizerKey)
		if err := r.Update(ctx, redpandaCluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("unable to remove Cluster finalizer: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

// setInitialSuperUserPassword should be idempotent, create user if not found, updates if found
func (r *ClusterReconciler) setInitialSuperUserPassword(
	ctx context.Context,
	adminAPI adminutils.AdminAPIClient,
	objs []types.NamespacedName,
) error {
	// might not have internal AdminAPI listener
	if adminAPI == nil {
		return nil
	}

	// list out users here
	users, err := adminAPI.ListUsers(ctx)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "unavailable") {
			return fmt.Errorf("could not fetch users from the Redpanda admin api: %w", err)
		}
	}

	errs := make([]error, 0)
	for _, obj := range objs {
		// We first check that the secret has been created
		// This should have been done by this point, if not
		// requeue, this is created by the controller and is
		// the source of truth, not the admin API
		secret := &corev1.Secret{}
		errGet := r.Get(ctx, obj, secret)
		if errGet != nil {
			errs = append(errs, fmt.Errorf("could not fetch user secret (%s): %w", obj.String(), errGet))
			continue
		}
		// if we do not find the user in the list, we should create them
		userFound := false
		expectedUser := string(secret.Data[corev1.BasicAuthUsernameKey])

		for i := range users {
			if users[i] == expectedUser {
				userFound = true
				break
			}
		}

		if userFound {
			// update the user here, we cannot retrieve password changes here to date
			if updateErr := updateUserOnAdminAPI(ctx, adminAPI, secret); updateErr != nil {
				errs = append(errs, fmt.Errorf("redpanda admin api: %w", updateErr))
			}
			continue
		}

		// we did not find user, so create them
		if createErr := createUserOnAdminAPI(ctx, adminAPI, secret); createErr != nil {
			errs = append(errs, fmt.Errorf("redpanda admin api: %w", createErr))
		}
	}

	return utilerrors.NewAggregate(errs)
}

// createUserOnAdminAPI will return to requeue only when api error occurred
func createUserOnAdminAPI(ctx context.Context, adminAPI adminutils.AdminAPIClient, secret *corev1.Secret) error {
	username := string(secret.Data[corev1.BasicAuthUsernameKey])
	password := string(secret.Data[corev1.BasicAuthPasswordKey])

	err := adminAPI.CreateUser(ctx, username, password, admin.ScramSha256)
	// {"message": "Creating user: User already exists", "code": 400}
	if err != nil { // TODO if user already exists, we only receive "400". Check for specific error code when available.
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("could not create user %q: %w", username, err)
	}
	return err
}

func updateUserOnAdminAPI(ctx context.Context, adminAPI adminutils.AdminAPIClient, secret *corev1.Secret) error {
	username := string(secret.Data[corev1.BasicAuthUsernameKey])
	password := string(secret.Data[corev1.BasicAuthPasswordKey])

	err := adminAPI.UpdateUser(ctx, username, password, admin.ScramSha256)
	if err != nil {
		return fmt.Errorf("could not update user %q: %w", username, err)
	}

	return err
}

func needExternalIP(external redpandav1alpha1.ExternalConnectivityConfig) bool {
	return external.Subdomain == ""
}

func subdomainAddress(
	tmpl string, pod *corev1.Pod, subdomain string, port int32,
) (string, error) {
	prefixLen := len(pod.GenerateName)
	index, err := strconv.Atoi(pod.Name[prefixLen:])
	if err != nil {
		return "", fmt.Errorf("could not parse node ID from pod name %s: %w", pod.Name, err)
	}
	data := utils.NewEndpointTemplateData(index, pod.Status.HostIP)
	ep, err := utils.ComputeEndpoint(tmpl, data)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.%s:%d",
		ep,
		subdomain,
		port,
	), nil
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

func collectNodePorts(
	redpandaPorts *networking.RedpandaPorts,
) []resources.NamedServiceNodePort {
	nodeports := []resources.NamedServiceNodePort{}
	kafkaAPINamedNodePort := redpandaPorts.KafkaAPI.ToNamedServiceNodePort()
	if kafkaAPINamedNodePort != nil {
		nodeports = append(nodeports, *kafkaAPINamedNodePort)
	}
	adminAPINodePort := redpandaPorts.AdminAPI.ToNamedServiceNodePort()
	if adminAPINodePort != nil {
		nodeports = append(nodeports, *adminAPINodePort)
	}
	pandaProxyNodePort := redpandaPorts.PandaProxy.ToNamedServiceNodePort()
	if pandaProxyNodePort != nil {
		nodeports = append(nodeports, *pandaProxyNodePort)
	}
	schemaRegistryNodePort := redpandaPorts.SchemaRegistry.ToNamedServiceNodePort()
	if schemaRegistryNodePort != nil {
		nodeports = append(nodeports, *schemaRegistryNodePort)
	}
	return nodeports
}

func collectHeadlessPorts(
	redpandaPorts *networking.RedpandaPorts,
) []resources.NamedServicePort {
	headlessPorts := []resources.NamedServicePort{}
	if redpandaPorts.AdminAPI.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: resources.AdminPortName, Port: *redpandaPorts.AdminAPI.InternalPort()})
	}
	if redpandaPorts.KafkaAPI.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: resources.InternalListenerName, Port: *redpandaPorts.KafkaAPI.InternalPort()})
	}
	if redpandaPorts.PandaProxy.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: resources.PandaproxyPortInternalName, Port: *redpandaPorts.PandaProxy.InternalPort()})
	}
	return headlessPorts
}

func collectLBPorts(
	redpandaPorts *networking.RedpandaPorts,
) []resources.NamedServicePort {
	lbPorts := []resources.NamedServicePort{}
	if redpandaPorts.KafkaAPI.ExternalBootstrap != nil {
		lbPorts = append(lbPorts, *redpandaPorts.KafkaAPI.ExternalBootstrap)
	}
	return lbPorts
}

func collectClusterPorts(
	redpandaPorts *networking.RedpandaPorts,
	redpandaCluster *redpandav1alpha1.Cluster,
) []resources.NamedServicePort {
	clusterPorts := []resources.NamedServicePort{}
	if redpandaPorts.PandaProxy.External != nil {
		clusterPorts = append(clusterPorts, resources.NamedServicePort{Name: resources.PandaproxyPortExternalName, Port: *redpandaPorts.PandaProxy.ExternalPort()})
	}
	if redpandaCluster.Spec.Configuration.SchemaRegistry != nil {
		port := redpandaCluster.Spec.Configuration.SchemaRegistry.Port
		clusterPorts = append(clusterPorts, resources.NamedServicePort{Name: resources.SchemaRegistryPortName, Port: port})
	}
	return clusterPorts
}

func isRedpandaClusterManaged(
	log logr.Logger, redpandaCluster *redpandav1alpha1.Cluster,
) bool {
	managedAnnotationKey := redpandav1alpha1.GroupVersion.Group + "/managed"
	if managed, exists := redpandaCluster.Annotations[managedAnnotationKey]; exists && managed == "false" {
		log.Info(fmt.Sprintf("management of %s is disabled; to enable it, change the '%s' annotation to true or remove it",
			redpandaCluster.Name, managedAnnotationKey))
		return false
	}
	return true
}

func isRedpandaClusterVersionManaged(
	log logr.Logger,
	redpandaCluster *redpandav1alpha1.Cluster,
	restrictToRedpandaVersion string,
) bool {
	if restrictToRedpandaVersion != "" && restrictToRedpandaVersion != redpandaCluster.Spec.Version {
		log.Info(fmt.Sprintf("management of %s is restricted to cluster (spec) version %s; cluster has spec version %s and status version %s",
			redpandaCluster.Name, restrictToRedpandaVersion, redpandaCluster.Spec.Version, redpandaCluster.Status.Version))
		return false
	}
	return true
}
