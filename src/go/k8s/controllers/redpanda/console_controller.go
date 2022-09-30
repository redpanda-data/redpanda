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
	"strings"
	"time"

	"github.com/go-logr/logr"
	cmapiv1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	consolepkg "github.com/redpanda-data/redpanda/src/go/k8s/pkg/console"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ConsoleReconciler reconciles a Console object
type ConsoleReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Log                     logr.Logger
	AdminAPIClientFactory   adminutils.AdminAPIClientFactory
	clusterDomain           string
	Store                   *consolepkg.Store
	EventRecorder           record.EventRecorder
	KafkaAdminClientFactory consolepkg.KafkaAdminClientFactory
}

const (
	// ClusterNotFoundEvent is a warning event if referenced Cluster not found
	ClusterNotFoundEvent = "ClusterNotFound"

	// ClusterNotConfiguredEvent is a warning event if referenced Cluster not yet configured
	ClusterNotConfiguredEvent = "ClusterNotConfigured"

	// NoSubdomainEvent is warning event if subdomain is not found in Cluster ExternalListener
	NoSubdomainEvent = "NoSubdomain"
)

//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;update;patch

//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=consoles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=consoles/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=consoles/finalizers,verbs=update

// Reconcile handles Console reconcile requests
func (r *ConsoleReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := r.Log.WithValues("redpandaconsole", req.NamespacedName)

	log.Info(fmt.Sprintf("Starting reconcile loop for %v", req.NamespacedName))
	defer log.Info(fmt.Sprintf("Finished reconcile loop for %v", req.NamespacedName))

	console := &redpandav1alpha1.Console{}
	if err := r.Get(ctx, req.NamespacedName, console); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	// Checks if Console is valid to be created in specified namespace
	if !console.IsAllowedNamespace() {
		err := fmt.Errorf("invalid Console namespace") //nolint:goerr113 // no need to declare new error type
		log.Error(err, "Console must be created in Redpanda namespace. Set --allow-console-any-ns=true to enable")
		return ctrl.Result{}, err
	}

	cluster, err := console.GetCluster(ctx, r.Client)
	if err != nil {
		// Create event instead of logging the error, so user can see in Console CR instead of checking logs in operator
		switch {
		case apierrors.IsNotFound(err):
			// If deleting and cluster is not found, nothing to do
			if console.GetDeletionTimestamp() != nil {
				controllerutil.RemoveFinalizer(console, consolepkg.ConsoleSAFinalizer)
				controllerutil.RemoveFinalizer(console, consolepkg.ConsoleACLFinalizer)
				return ctrl.Result{}, r.Update(ctx, console)
			}
			r.EventRecorder.Eventf(
				console,
				corev1.EventTypeWarning, ClusterNotFoundEvent,
				"Unable to reconcile Console as the referenced Cluster %s is not found", console.GetClusterRef(),
			)
		case errors.Is(err, redpandav1alpha1.ErrClusterNotConfigured):
			r.EventRecorder.Eventf(
				console,
				corev1.EventTypeWarning, ClusterNotConfiguredEvent,
				"Unable to reconcile Console as the referenced Cluster %s is not yet configured", console.GetClusterRef(),
			)
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	var s state
	switch {
	case console.GetDeletionTimestamp() != nil:
		s = &Deleting{r}
	case !console.GenerationMatchesObserved():
		if err := r.handleSpecChange(ctx, console); err != nil {
			return ctrl.Result{}, fmt.Errorf("handle spec change: %w", err)
		}
		fallthrough
	default:
		s = &Reconciling{r}
	}

	return s.Do(ctx, console, cluster, log)
}

// Reconciling is the state of the Console that handles reconciliation
type Reconciling ConsoleState

// Do handles reconciliation of Console
func (r *Reconciling) Do(
	ctx context.Context,
	console *redpandav1alpha1.Console,
	cluster *redpandav1alpha1.Cluster,
	log logr.Logger,
) (ctrl.Result, error) {
	// Ensure items in the store are updated
	if err := r.Store.Sync(cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync console store: %w", err)
	}

	// ConfigMap is set to immutable and a new one is created if needed every reconcile
	// Cleanup unused ConfigMaps before ensuring Resources which might create new ConfigMaps again
	// Otherwise, if reconciliation always fail, a lot of unused ConfigMaps will be created
	configmapResource := consolepkg.NewConfigMap(r.Client, r.Scheme, console, cluster, log)
	if err := configmapResource.DeleteUnused(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("deleting unused configmaps: %w", err)
	}

	// NewIngress will not create Ingress if subdomain is empty
	subdomain := ""
	if ex := cluster.ExternalListener(); ex != nil && ex.GetExternal().Subdomain != "" {
		subdomain = ex.GetExternal().Subdomain
	} else {
		r.EventRecorder.Event(
			console,
			corev1.EventTypeWarning, NoSubdomainEvent,
			"No Ingress created because no subdomain is found in Cluster ExternalListener",
		)
	}

	// Ingress with TLS and "/debug" "/admin" paths disabled
	ingressResource := resources.NewIngress(r.Client, console, r.Scheme, subdomain, console.GetName(), consolepkg.ServicePortName, log)
	ingressResource = ingressResource.WithDefaultEndpoint("console")
	ingressResource = ingressResource.WithUserConfig(console.Spec.Ingress)
	ingressResource = ingressResource.WithAnnotations(map[string]string{
		"nginx.ingress.kubernetes.io/server-snippet": "if ($request_uri ~* ^/(debug|admin)) {\n\treturn 403;\n\t}",
	})
	if ext := cluster.ExternalListener(); ext != nil && ext.TLS.Enabled {
		// Default to using Cluster wildcard certificate
		log.Info("Using shared cluster certificate for console")
		redpandaNodeCertSecret, present := r.Store.GetRedpandaNodeCert(cluster)
		if !present {
			log.Info("Waiting for cluster certificate to be created")
			return ctrl.Result{Requeue: true}, nil
		}
		ingressResource = ingressResource.WithClusterTLS(redpandaNodeCertSecret)
	} else {
		// Issue a new certificate for the console
		log.Info("Using owned certificate for console")
		ingressResource = ingressResource.WithTLS(resources.LEClusterIssuer, fmt.Sprintf("%s-redpanda", cluster.GetName()))
	}

	applyResources := []resources.Resource{
		consolepkg.NewKafkaSA(r.Client, r.Scheme, console, cluster, r.clusterDomain, r.AdminAPIClientFactory, log),
		consolepkg.NewKafkaACL(r.Client, r.Scheme, console, cluster, r.KafkaAdminClientFactory, log),
		configmapResource,
		consolepkg.NewDeployment(r.Client, r.Scheme, console, cluster, r.Store, log),
		consolepkg.NewService(r.Client, r.Scheme, console, r.clusterDomain, log),
		ingressResource,
	}
	for _, each := range applyResources {
		if err := each.Ensure(ctx); err != nil { //nolint:gocritic // more readable
			var ra *resources.RequeueAfterError
			if errors.As(err, &ra) {
				log.V(debugLogLevel).Info(fmt.Sprintf("Requeue ensuring resource after %d: %s", ra.RequeueAfter, ra.Msg))
				// RequeueAfterError is used to delay retry
				log.Info(fmt.Sprintf("Ensuring resource failed, requeueing after %s: %s", ra.RequeueAfter, ra.Msg))
				return ctrl.Result{RequeueAfter: ra.RequeueAfter}, nil
			}
			var r *resources.RequeueError
			if errors.As(err, &r) {
				log.V(debugLogLevel).Info(fmt.Sprintf("Requeue ensuring resource: %s", r.Msg))
				// RequeueError is used to skip controller logging the error and using default retry backoff
				// Don't return the error, as it is most likely not an actual error
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}

	if !console.GenerationMatchesObserved() {
		console.Status.ObservedGeneration = console.GetGeneration()
		if err := r.Status().Update(ctx, console); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// Deleting is the state of the Console that handles deletion
type Deleting ConsoleState

// Do handles deletion of Console
func (r *Deleting) Do(
	ctx context.Context,
	console *redpandav1alpha1.Console,
	cluster *redpandav1alpha1.Cluster,
	log logr.Logger,
) (ctrl.Result, error) {
	applyResources := []resources.ManagedResource{
		consolepkg.NewKafkaSA(r.Client, r.Scheme, console, cluster, r.clusterDomain, r.AdminAPIClientFactory, log),
		consolepkg.NewKafkaACL(r.Client, r.Scheme, console, cluster, r.KafkaAdminClientFactory, log),
	}

	for _, each := range applyResources {
		if err := each.Cleanup(ctx); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// handleSpecChange is a hook to call before Reconciling
func (r *ConsoleReconciler) handleSpecChange(
	ctx context.Context, console *redpandav1alpha1.Console,
) error {
	if console.Status.ConfigMapRef != nil {
		// We are creating new ConfigMap for every spec change so Deployment can detect changes and redeploy Pods
		// Unset Status.ConfigMapRef so we can delete the previous unused ConfigMap
		console.Status.ConfigMapRef = nil
		if err := r.Status().Update(ctx, console); err != nil {
			return err
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConsoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Console{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Watches(&source.Kind{Type: &cmapiv1.Certificate{}},
			handler.EnqueueRequestsFromMapFunc(func(a client.Object) []reconcile.Request {
				if c, ok := a.(*cmapiv1.Certificate); ok {
					cons, err := r.getAffectedConsolesByClusterResource(types.NamespacedName{Namespace: c.Namespace, Name: c.Name})
					if err != nil {
						r.Log.Error(err, "could not determine the list of consoles affected by certificate change")
						return []reconcile.Request{}
					}
					return cons
				}
				return []reconcile.Request{}
			})).
		Watches(&source.Kind{Type: &redpandav1alpha1.Cluster{}},
			handler.EnqueueRequestsFromMapFunc(func(a client.Object) []reconcile.Request {
				if c, ok := a.(*redpandav1alpha1.Cluster); ok {
					cons, err := r.getAffectedConsolesByClusterResource(types.NamespacedName{Namespace: c.Namespace, Name: c.Name})
					if err != nil {
						r.Log.Error(err, "could not determine the list of consoles affected by cluster change")
						return []reconcile.Request{}
					}
					return cons
				}
				return []reconcile.Request{}
			})).
		Complete(r)
}

// getAffectedConsolesByClusterResource returns Consoles that might be
// affected by change in a cluster resource. Cluster resources (e.g. clusters or
// certificates associated to them) share a common prefix that is the cluster
// name itself. We use it to retrieve consoles that might be affected.
//
// Being based on name prefix, it might trigger more reconciliation
// requests than needed.
func (r *ConsoleReconciler) getAffectedConsolesByClusterResource(res types.NamespacedName) ([]reconcile.Request, error) {
	// Unfortunately handler does not have context injected
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	consoles := redpandav1alpha1.ConsoleList{}
	if err := r.List(ctx, &consoles); err != nil {
		return nil, fmt.Errorf("could not list consoles for propagating event: %w", err)
	}

	var reqs []reconcile.Request
	for i := range consoles.Items {
		c := &consoles.Items[i]
		if c.Spec.ClusterRef.Namespace == res.Namespace && strings.HasPrefix(res.Name, c.Spec.ClusterRef.Name) {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: c.Namespace,
					Name:      c.Name,
				},
			})
		}
	}
	return reqs, nil
}

// WithClusterDomain sets the clusterDomain
func (r *ConsoleReconciler) WithClusterDomain(
	clusterDomain string,
) *ConsoleReconciler {
	r.clusterDomain = clusterDomain
	return r
}
