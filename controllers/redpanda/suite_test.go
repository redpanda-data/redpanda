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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	cmapiv1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	redpandacontrollers "github.com/redpanda-data/redpanda/src/go/k8s/controllers/redpanda"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	consolepkg "github.com/redpanda-data/redpanda/src/go/k8s/pkg/console"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/envtest/printer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var (
	k8sClient             client.Client
	testEnv               *envtest.Environment
	cfg                   *rest.Config
	testAdminAPI          *mockAdminAPI
	testAdminAPIFactory   adminutils.AdminAPIClientFactory
	testStore             *consolepkg.Store
	testKafkaAdmin        *mockKafkaAdmin
	testKafkaAdminFactory consolepkg.KafkaAdminClientFactory
)

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecsWithDefaultAndCustomReporters(t,
		"Controller Suite",
		[]Reporter{printer.NewlineReporter{}})
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

	testAdminAPI = &mockAdminAPI{}
	testAdminAPIFactory = func(
		_ context.Context,
		_ client.Reader,
		_ *redpandav1alpha1.Cluster,
		_ string,
		_ types.AdminTLSConfigProvider,
		ordinals ...int32,
	) (adminutils.AdminAPIClient, error) {
		if len(ordinals) == 1 {
			return &scopedMockAdminAPI{
				mockAdminAPI: testAdminAPI,
				ordinal:      ordinals[0],
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
		err = k8sManager.Start(ctrl.SetupSignalHandler())
		Expect(err).ToNot(HaveOccurred())
	}()

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
	gexec.KillAndWait(5 * time.Second)
	// kube-apiserver hanging during cleanup
	// REF https://book.kubebuilder.io/reference/envtest.html#kubernetes-120-and-121-binary-issues
	timeout := 30 * time.Second
	poll := 5 * time.Second
	Eventually(func() error {
		return testEnv.Stop()
	}, timeout, poll).ShouldNot(HaveOccurred())
})

type mockAdminAPI struct {
	config           admin.Config
	schema           admin.ConfigSchema
	patches          []configuration.CentralConfigurationPatch
	unavailable      bool
	invalid          []string
	unknown          []string
	directValidation bool
	brokers          []admin.Broker
	monitor          sync.Mutex
}

type scopedMockAdminAPI struct {
	*mockAdminAPI
	ordinal int32
}

var _ adminutils.AdminAPIClient = &mockAdminAPI{}

type unavailableError struct{}

func (*unavailableError) Error() string {
	return "unavailable"
}

func (m *mockAdminAPI) Config(_ context.Context) (admin.Config, error) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.Config{}, &unavailableError{}
	}
	var res admin.Config
	makeCopy(m.config, &res)
	return res, nil
}

func (m *mockAdminAPI) ClusterConfigStatus(
	_ context.Context, _ bool,
) (admin.ConfigStatusResponse, error) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.ConfigStatusResponse{}, &unavailableError{}
	}
	node := admin.ConfigStatus{
		Invalid: append([]string{}, m.invalid...),
		Unknown: append([]string{}, m.unknown...),
	}
	return []admin.ConfigStatus{node}, nil
}

func (m *mockAdminAPI) ClusterConfigSchema(
	_ context.Context,
) (admin.ConfigSchema, error) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.ConfigSchema{}, &unavailableError{}
	}
	var res admin.ConfigSchema
	makeCopy(m.schema, &res)
	return res, nil
}

func (m *mockAdminAPI) PatchClusterConfig(
	_ context.Context, upsert map[string]interface{}, remove []string,
) (admin.ClusterConfigWriteResult, error) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.ClusterConfigWriteResult{}, &unavailableError{}
	}
	m.patches = append(m.patches, configuration.CentralConfigurationPatch{
		Upsert: upsert,
		Remove: remove,
	})
	var newInvalid []string
	var newUnknown []string
	for k := range upsert {
		if meta, ok := m.schema[k]; !ok {
			newUnknown = append(newUnknown, k)
		} else if meta.Description == "invalid" {
			newInvalid = append(newInvalid, k)
		}
	}
	invalidRequest := len(newInvalid)+len(newUnknown) > 0
	if m.directValidation && invalidRequest {
		return admin.ClusterConfigWriteResult{}, &admin.HTTPResponseError{
			Method: http.MethodPut,
			URL:    "/v1/cluster_config",
			Response: &http.Response{
				Status:     "Bad Request",
				StatusCode: 400,
			},
			Body: []byte("Mock bad request message"),
		}
	}
	if invalidRequest {
		m.invalid = addAsSet(m.invalid, newInvalid...)
		m.unknown = addAsSet(m.unknown, newUnknown...)
		return admin.ClusterConfigWriteResult{}, nil
	}
	if m.config == nil {
		m.config = make(map[string]interface{})
	}
	for k, v := range upsert {
		m.config[k] = v
	}
	for _, k := range remove {
		delete(m.config, k)
		for i := range m.invalid {
			if m.invalid[i] == k {
				m.invalid = append(m.invalid[0:i], m.invalid[i+1:]...)
			}
		}
		for i := range m.unknown {
			if m.unknown[i] == k {
				m.unknown = append(m.unknown[0:i], m.unknown[i+1:]...)
			}
		}
	}
	return admin.ClusterConfigWriteResult{}, nil
}

func (m *mockAdminAPI) CreateUser(_ context.Context, _, _, _ string) error {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return &unavailableError{}
	}
	return nil
}

func (m *mockAdminAPI) DeleteUser(_ context.Context, _ string) error {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return &unavailableError{}
	}
	return nil
}

func (m *mockAdminAPI) Clear() {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.config = nil
	m.schema = nil
	m.patches = nil
	m.unavailable = false
	m.directValidation = false
	m.brokers = nil
}

func (m *mockAdminAPI) GetFeatures(
	_ context.Context,
) (admin.FeaturesResponse, error) {
	return admin.FeaturesResponse{
		ClusterVersion: 0,
		Features: []admin.Feature{
			{
				Name:      "central_config",
				State:     admin.FeatureStateActive,
				WasActive: true,
			},
		},
	}, nil
}

func (m *mockAdminAPI) SetLicense(_ context.Context, _ interface{}) error {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return &unavailableError{}
	}
	return nil
}

//nolint:gocritic // It's test API
func (m *mockAdminAPI) RegisterPropertySchema(
	name string, metadata admin.ConfigPropertyMetadata,
) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.schema == nil {
		m.schema = make(map[string]admin.ConfigPropertyMetadata)
	}
	m.schema[name] = metadata
}

func (m *mockAdminAPI) PropertyGetter(name string) func() interface{} {
	return func() interface{} {
		m.monitor.Lock()
		defer m.monitor.Unlock()
		return m.config[name]
	}
}

func (m *mockAdminAPI) ConfigGetter() func() admin.Config {
	return func() admin.Config {
		m.monitor.Lock()
		defer m.monitor.Unlock()
		var res admin.Config
		makeCopy(m.config, &res)
		return res
	}
}

func (m *mockAdminAPI) PatchesGetter() func() []configuration.CentralConfigurationPatch {
	return func() []configuration.CentralConfigurationPatch {
		m.monitor.Lock()
		defer m.monitor.Unlock()
		var res []configuration.CentralConfigurationPatch
		makeCopy(m.patches, &res)
		return res
	}
}

func (m *mockAdminAPI) NumPatchesGetter() func() int {
	return func() int {
		return len(m.PatchesGetter()())
	}
}

func (m *mockAdminAPI) SetProperty(key string, value interface{}) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.config == nil {
		m.config = make(map[string]interface{})
	}
	m.config[key] = value
}

func (m *mockAdminAPI) SetUnavailable(unavailable bool) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.unavailable = unavailable
}

func (m *mockAdminAPI) GetNodeConfig(
	_ context.Context,
) (admin.NodeConfig, error) {
	return admin.NodeConfig{}, nil
}

//nolint:goerr113 // test code
func (s *scopedMockAdminAPI) GetNodeConfig(
	ctx context.Context,
) (admin.NodeConfig, error) {
	brokers, err := s.Brokers(ctx)
	if err != nil {
		return admin.NodeConfig{}, err
	}
	if len(brokers) <= int(s.ordinal) {
		return admin.NodeConfig{}, fmt.Errorf("broker not registered")
	}
	return admin.NodeConfig{
		NodeID: brokers[int(s.ordinal)].NodeID,
	}, nil
}

func (m *mockAdminAPI) SetDirectValidationEnabled(directValidation bool) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.directValidation = directValidation
}

func (m *mockAdminAPI) AddBroker(broker admin.Broker) {
	m.monitor.Lock()
	defer m.monitor.Unlock()

	m.brokers = append(m.brokers, broker)
}

func (m *mockAdminAPI) RemoveBroker(id int) bool {
	m.monitor.Lock()
	defer m.monitor.Unlock()

	idx := -1
	for i := range m.brokers {
		if m.brokers[i].NodeID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}
	m.brokers = append(m.brokers[:idx], m.brokers[idx+1:]...)
	return true
}

func (m *mockAdminAPI) Brokers(_ context.Context) ([]admin.Broker, error) {
	m.monitor.Lock()
	defer m.monitor.Unlock()

	return append([]admin.Broker{}, m.brokers...), nil
}

func (m *mockAdminAPI) BrokerStatusGetter(
	id int,
) func() admin.MembershipStatus {
	return func() admin.MembershipStatus {
		m.monitor.Lock()
		defer m.monitor.Unlock()

		for i := range m.brokers {
			if m.brokers[i].NodeID == id {
				return m.brokers[i].MembershipStatus
			}
		}
		return ""
	}
}

func (m *mockAdminAPI) DecommissionBroker(_ context.Context, id int) error {
	return m.SetBrokerStatus(id, admin.MembershipStatusDraining)
}

func (m *mockAdminAPI) RecommissionBroker(_ context.Context, id int) error {
	return m.SetBrokerStatus(id, admin.MembershipStatusActive)
}

func (m *mockAdminAPI) EnableMaintenanceMode(_ context.Context, _ int) error {
	return nil
}

func (m *mockAdminAPI) DisableMaintenanceMode(_ context.Context, _ int) error {
	return nil
}

//nolint:goerr113 // test code
func (m *mockAdminAPI) SetBrokerStatus(
	id int, status admin.MembershipStatus,
) error {
	m.monitor.Lock()
	defer m.monitor.Unlock()

	for i := range m.brokers {
		if m.brokers[i].NodeID == id {
			m.brokers[i].MembershipStatus = status
			return nil
		}
	}
	return fmt.Errorf("unknown broker %d", id)
}

func makeCopy(input, output interface{}) {
	ser, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(ser))
	decoder.UseNumber()
	err = decoder.Decode(output)
	if err != nil {
		panic(err)
	}
}

func addAsSet(sliceSet []string, vals ...string) []string {
	asSet := make(map[string]bool, len(sliceSet)+len(vals))
	for _, k := range sliceSet {
		asSet[k] = true
	}
	for _, v := range vals {
		asSet[v] = true
	}
	lst := make([]string, 0, len(asSet))
	for k := range asSet {
		lst = append(lst, k)
	}
	sort.Strings(lst)
	return lst
}
