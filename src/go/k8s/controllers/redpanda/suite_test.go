// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	cmapiv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	redpandacontrollers "github.com/redpanda-data/redpanda/src/go/k8s/controllers/redpanda"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	consolepkg "github.com/redpanda-data/redpanda/src/go/k8s/pkg/console"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var (
	k8sClient             client.Client
	testEnv               *envtest.Environment
	cfg                   *rest.Config
	testAdminAPI          *adminutils.MockAdminAPI
	testAdminAPIFactory   adminutils.AdminAPIClientFactory
	testStore             *consolepkg.Store
	testKafkaAdmin        *mockKafkaAdmin
	testKafkaAdminFactory consolepkg.KafkaAdminClientFactory

	ctx              context.Context
	controllerCancel context.CancelFunc
)

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = scheme.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())
	err = redpandav1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())
	err = cmapiv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	//+kubebuilder:scaffold:scheme

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
	})
	Expect(err).ToNot(HaveOccurred())
	ctx = ctrl.SetupSignalHandler()
	ctx, controllerCancel = context.WithCancel(ctx)

	testAdminAPI = &adminutils.MockAdminAPI{Log: ctrl.Log.WithName("testAdminAPI").WithName("mockAdminAPI")}
	testAdminAPIFactory = func(
		_ context.Context,
		_ client.Reader,
		_ *redpandav1alpha1.Cluster,
		_ string,
		_ types.AdminTLSConfigProvider,
		ordinals ...int32,
	) (adminutils.AdminAPIClient, error) {
		if len(ordinals) == 1 {
			return &adminutils.ScopedMockAdminAPI{
				MockAdminAPI: testAdminAPI,
				Ordinal:      ordinals[0],
			}, nil
		}
		return testAdminAPI, nil
	}

	testStore = consolepkg.NewStore(k8sManager.GetClient(), k8sManager.GetScheme())
	testKafkaAdmin = &mockKafkaAdmin{}
	testKafkaAdminFactory = func(context.Context, client.Client, *redpandav1alpha1.Cluster, *consolepkg.Store) (consolepkg.KafkaAdminClient, error) {
		return testKafkaAdmin, nil
	}

	err = (&redpandacontrollers.ClusterReconciler{
		Client:                   k8sManager.GetClient(),
		Log:                      ctrl.Log.WithName("controllers").WithName("core").WithName("RedpandaCluster"),
		Scheme:                   k8sManager.GetScheme(),
		AdminAPIClientFactory:    testAdminAPIFactory,
		DecommissionWaitInterval: 100 * time.Millisecond,
	}).WithClusterDomain("cluster.local").WithConfiguratorSettings(resources.ConfiguratorSettings{
		ConfiguratorBaseImage: "vectorized/configurator",
		ConfiguratorTag:       "latest",
		ImagePullPolicy:       "Always",
	}).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	driftCheckPeriod := 500 * time.Millisecond
	err = (&redpandacontrollers.ClusterConfigurationDriftReconciler{
		Client:                k8sManager.GetClient(),
		Log:                   ctrl.Log.WithName("controllers").WithName("core").WithName("RedpandaCluster"),
		Scheme:                k8sManager.GetScheme(),
		AdminAPIClientFactory: testAdminAPIFactory,
		DriftCheckPeriod:      &driftCheckPeriod,
	}).WithClusterDomain("cluster.local").SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	err = (&redpandacontrollers.ConsoleReconciler{
		Client:                  k8sManager.GetClient(),
		Scheme:                  k8sManager.GetScheme(),
		Log:                     ctrl.Log.WithName("controllers").WithName("redpanda").WithName("Console"),
		AdminAPIClientFactory:   testAdminAPIFactory,
		Store:                   testStore,
		EventRecorder:           k8sManager.GetEventRecorderFor("Console"),
		KafkaAdminClientFactory: testKafkaAdminFactory,
	}).WithClusterDomain("cluster.local").SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	go func() {
		err = k8sManager.Start(ctx)
		Expect(err).ToNot(HaveOccurred())
	}()
	Expect(k8sManager.GetCache().WaitForCacheSync(context.Background())).To(BeTrue())

	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).ToNot(BeNil())

	close(done)
}, 60)

var _ = BeforeEach(func() {
	By("Cleaning the admin API")
	testAdminAPI.Clear()
	// Register some known properties for all tests
	testAdminAPI.RegisterPropertySchema("auto_create_topics_enabled", admin.ConfigPropertyMetadata{NeedsRestart: false})
	testAdminAPI.RegisterPropertySchema("cloud_storage_segment_max_upload_interval_sec", admin.ConfigPropertyMetadata{NeedsRestart: true})
	testAdminAPI.RegisterPropertySchema("log_segment_size", admin.ConfigPropertyMetadata{NeedsRestart: true})
	testAdminAPI.RegisterPropertySchema("enable_rack_awareness", admin.ConfigPropertyMetadata{NeedsRestart: false})

	// By default we set the following properties and they'll be loaded by redpanda from the .bootstrap.yaml
	// So we initialize the test admin API with those
	testAdminAPI.SetProperty("auto_create_topics_enabled", false)
	testAdminAPI.SetProperty("cloud_storage_segment_max_upload_interval_sec", 1800)
	testAdminAPI.SetProperty("log_segment_size", 536870912)
	testAdminAPI.SetProperty("enable_rack_awareness", true)
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	// kube-apiserver hanging during cleanup
	// stopping the controllers prevents the hang
	controllerCancel()
	gexec.KillAndWait(5 * time.Second)
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
