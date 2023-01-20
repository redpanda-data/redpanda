package testutils

import (
	"context"
	"strings"
	"time"

	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	timeout  = 30 * time.Second
	interval = 100 * time.Millisecond
)

func DeleteAllInNamespace(testEnv *envtest.Environment, k8sClient client.Client, namespaces ...client.Object) {
	ctx := context.Background()
	clientGo, err := kubernetes.NewForConfig(testEnv.Config)
	Expect(err).ShouldNot(HaveOccurred())
	for _, obj := range namespaces {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, obj))).Should(Succeed())

		if ns, ok := obj.(*v1.Namespace); ok {
			// Normally the kube-controller-manager would handle finalization
			// and garbage collection of namespaces, but with envtest, we aren't
			// running a kube-controller-manager. Instead we're gonna approximate
			// (poorly) the kube-controller-manager by explicitly deleting some
			// resources within the namespace and then removing the `kubernetes`
			// finalizer from the namespace resource so it can finish deleting.
			// Note that any resources within the namespace that we don't
			// successfully delete could reappear if the namespace is ever
			// recreated with the same name.

			// Look up all namespaced resources under the discovery API
			_, apiResources, err := clientGo.Discovery().ServerGroupsAndResources()
			Expect(err).ShouldNot(HaveOccurred())
			namespacedGVKs := make(map[string]schema.GroupVersionKind)
			for _, apiResourceList := range apiResources {
				defaultGV, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
				Expect(err).ShouldNot(HaveOccurred())
				for _, r := range apiResourceList.APIResources {
					if !r.Namespaced || strings.Contains(r.Name, "/") {
						// skip non-namespaced and subresources
						continue
					}
					gvk := schema.GroupVersionKind{
						Group:   defaultGV.Group,
						Version: defaultGV.Version,
						Kind:    r.Kind,
					}
					if r.Group != "" {
						gvk.Group = r.Group
					}
					if r.Version != "" {
						gvk.Version = r.Version
					}
					namespacedGVKs[gvk.String()] = gvk
				}
			}

			// Delete all namespaced resources in this namespace
			for _, gvk := range namespacedGVKs {
				var u unstructured.Unstructured
				u.SetGroupVersionKind(gvk)
				err := k8sClient.DeleteAllOf(ctx, &u, client.InNamespace(ns.Name))
				Expect(client.IgnoreNotFound(ignoreMethodNotAllowed(err))).ShouldNot(HaveOccurred())
			}

			Eventually(func() error {
				key := client.ObjectKeyFromObject(ns)
				if err != nil {
					return err
				}
				if err := k8sClient.Get(ctx, key, ns); err != nil {
					return client.IgnoreNotFound(err)
				}
				// remove `kubernetes` finalizer
				const kubernetes = "kubernetes"
				finalizers := []v1.FinalizerName{}
				for _, f := range ns.Spec.Finalizers {
					if f != kubernetes {
						finalizers = append(finalizers, f)
					}
				}
				ns.Spec.Finalizers = finalizers

				// We have to use the k8s.io/client-go library here to expose
				// ability to patch the /finalize subresource on the namespace
				_, err = clientGo.CoreV1().Namespaces().Finalize(ctx, ns, metav1.UpdateOptions{})
				return err
			}, timeout, interval).Should(Succeed())
		}

		Eventually(func() metav1.StatusReason {
			key := client.ObjectKeyFromObject(obj)
			if err := k8sClient.Get(ctx, key, obj); err != nil {
				return apierrors.ReasonForError(err)
			}
			return ""
		}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
	}
}

func ignoreMethodNotAllowed(err error) error {
	if err != nil {
		if apierrors.ReasonForError(err) == metav1.StatusReasonMethodNotAllowed {
			return nil
		}
	}
	return err
}
