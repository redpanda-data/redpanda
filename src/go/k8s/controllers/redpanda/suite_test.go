// Copyright 2021 Vectorized, Inc.
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
	"encoding/json"
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
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
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

var k8sClient client.Client
var testEnv *envtest.Environment
var cfg *rest.Config
var testAdminAPI *mockAdminAPI

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
	fakeAdminAPIFactory := func(_ *redpandav1alpha1.Cluster, _ string) (utils.AdminAPIClient, error) { return testAdminAPI, nil }

	err = (&redpandacontrollers.ClusterReconciler{
		Client:                k8sManager.GetClient(),
		Log:                   ctrl.Log.WithName("controllers").WithName("core").WithName("RedpandaCluster"),
		Scheme:                k8sManager.GetScheme(),
		AdminAPIClientFactory: fakeAdminAPIFactory,
	}).WithClusterDomain("cluster.local").WithConfiguratorSettings(resources.ConfiguratorSettings{
		ConfiguratorBaseImage: "vectorized/configurator",
		ConfiguratorTag:       "latest",
		ImagePullPolicy:       "Always",
	}).SetupWithManager(k8sManager)
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
	testAdminAPI.RegisterPropertySchema("enable_idempotence", admin.ConfigPropertyMetadata{NeedsRestart: true})
	testAdminAPI.RegisterPropertySchema("enable_transactions", admin.ConfigPropertyMetadata{NeedsRestart: true})
	testAdminAPI.RegisterPropertySchema("log_segment_size", admin.ConfigPropertyMetadata{NeedsRestart: true})

	// By default we set the following properties and they'll be loaded by redpanda from the .bootstrap.yaml
	// So we initialize the test admin API with those
	testAdminAPI.SetProperty("auto_create_topics_enabled", false)
	testAdminAPI.SetProperty("cloud_storage_segment_max_upload_interval_sec", 1800)
	testAdminAPI.SetProperty("enable_idempotence", false)
	testAdminAPI.SetProperty("enable_transactions", false)
	testAdminAPI.SetProperty("log_segment_size", 536870912)
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	gexec.KillAndWait(5 * time.Second)
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

type mockAdminAPI struct {
	config           admin.Config
	schema           admin.ConfigSchema
	patches          []configuration.CentralConfigurationPatch
	unavailable      bool
	invalid          []string
	unknown          []string
	directValidation bool
	monitor          sync.Mutex
}

var _ utils.AdminAPIClient = &mockAdminAPI{}

type unavailableError struct{}

func (*unavailableError) Error() string {
	return "unavailable"
}

func (m *mockAdminAPI) Config() (admin.Config, error) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.Config{}, &unavailableError{}
	}
	var res admin.Config
	makeCopy(m.config, &res)
	return res, nil
}

func (m *mockAdminAPI) ClusterConfigStatus() (
	admin.ConfigStatusResponse,
	error,
) {
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

func (m *mockAdminAPI) ClusterConfigSchema() (admin.ConfigSchema, error) {
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
	upsert map[string]interface{}, remove []string,
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
		return admin.ClusterConfigWriteResult{}, &admin.HttpError{
			Method: http.MethodPut,
			Url:    "/v1/cluster_config",
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

func (m *mockAdminAPI) CreateUser(_, _, _ string) error {
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
}

// nolint:gocritic // It's test API
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

func (m *mockAdminAPI) SetDirectValidationEnabled(directValidation bool) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.directValidation = directValidation
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
