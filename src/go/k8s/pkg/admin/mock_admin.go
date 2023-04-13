// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	ctrl "sigs.k8s.io/controller-runtime"
)

type MockAdminAPI struct {
	config            admin.Config
	schema            admin.ConfigSchema
	patches           []configuration.CentralConfigurationPatch
	unavailable       bool
	invalid           []string
	unknown           []string
	directValidation  bool
	brokers           []admin.Broker
	monitor           sync.Mutex
	Log               logr.Logger
	clusterHealth     bool
	MaintenanceStatus *admin.MaintenanceStatus
}

var _ AdminAPIClient = &MockAdminAPI{Log: ctrl.Log.WithName("AdminAPIClient").WithName("mockAdminAPI")}

type ScopedMockAdminAPI struct {
	*MockAdminAPI
	Ordinal int32
}

type unavailableError struct{}

func (*unavailableError) Error() string {
	return "unavailable"
}

func (m *MockAdminAPI) SetClusterHealth(health bool) {
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.clusterHealth = health
}

func (m *MockAdminAPI) Config(context.Context, bool) (admin.Config, error) {
	m.Log.WithName("Config").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.Config{}, &unavailableError{}
	}
	var res admin.Config
	makeCopy(m.config, &res)
	return res, nil
}

func (m *MockAdminAPI) ClusterConfigStatus(
	_ context.Context, _ bool,
) (admin.ConfigStatusResponse, error) {
	m.Log.WithName("ClusterConfigStatus").Info("called")
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

func (m *MockAdminAPI) ClusterConfigSchema(
	_ context.Context,
) (admin.ConfigSchema, error) {
	m.Log.WithName("ClusterConfigSchema").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.ConfigSchema{}, &unavailableError{}
	}
	var res admin.ConfigSchema
	makeCopy(m.schema, &res)
	return res, nil
}

func (m *MockAdminAPI) PatchClusterConfig(
	_ context.Context, upsert map[string]interface{}, remove []string,
) (admin.ClusterConfigWriteResult, error) {
	m.Log.WithName("PatchClusterConfig").WithValues("upsert", upsert, "remove", remove).Info("called")
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

func (m *MockAdminAPI) CreateUser(_ context.Context, _, _, _ string) error {
	m.Log.WithName("CreateUser").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return &unavailableError{}
	}
	return nil
}

func (m *MockAdminAPI) UpdateUser(_ context.Context, _, _, _ string) error {
	m.Log.WithName("UpdateUser").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return &unavailableError{}
	}
	return nil
}

func (m *MockAdminAPI) ListUsers(_ context.Context) ([]string, error) {
	m.Log.WithName("ListUsers").Info("called")
	users := make([]string, 0)
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return users, &unavailableError{}
	}
	return users, nil
}

func (m *MockAdminAPI) DeleteUser(_ context.Context, _ string) error {
	m.Log.WithName("DeleteUser").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return &unavailableError{}
	}
	return nil
}

func (m *MockAdminAPI) Clear() {
	m.Log.WithName("Clear").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.config = nil
	m.schema = nil
	m.patches = nil
	m.unavailable = false
	m.invalid = nil
	m.unknown = nil
	m.directValidation = false
	m.brokers = nil
	m.clusterHealth = true
	m.MaintenanceStatus = &admin.MaintenanceStatus{}
}

func (m *MockAdminAPI) GetFeatures(
	_ context.Context,
) (admin.FeaturesResponse, error) {
	m.Log.WithName("GetFeatures").Info("called")
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

func (m *MockAdminAPI) SetLicense(_ context.Context, _ interface{}) error {
	m.Log.WithName("SetLicense").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return &unavailableError{}
	}
	return nil
}

func (m *MockAdminAPI) GetLicenseInfo(_ context.Context) (admin.License, error) {
	m.Log.WithName("GetLicenseInfo").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.unavailable {
		return admin.License{}, &unavailableError{}
	}
	return admin.License{}, nil
}

//nolint:gocritic // It's test API
func (m *MockAdminAPI) RegisterPropertySchema(
	name string, metadata admin.ConfigPropertyMetadata,
) {
	m.Log.WithName("RegisterPropertySchema").WithValues("name", name, "metadata", metadata).Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.schema == nil {
		m.schema = make(map[string]admin.ConfigPropertyMetadata)
	}
	m.schema[name] = metadata
}

func (m *MockAdminAPI) PropertyGetter(name string) func() interface{} {
	return func() interface{} {
		m.Log.WithName("PropertyGetter").WithValues("name", name).Info("called")
		m.monitor.Lock()
		defer m.monitor.Unlock()
		return m.config[name]
	}
}

func (m *MockAdminAPI) ConfigGetter() func() admin.Config {
	return func() admin.Config {
		m.Log.WithName("ConfigGetter").Info("called")
		m.monitor.Lock()
		defer m.monitor.Unlock()
		var res admin.Config
		makeCopy(m.config, &res)
		return res
	}
}

func (m *MockAdminAPI) PatchesGetter() func() []configuration.CentralConfigurationPatch {
	return func() []configuration.CentralConfigurationPatch {
		m.Log.WithName("PatchesGetter(func)").Info("called")
		m.monitor.Lock()
		defer m.monitor.Unlock()
		var res []configuration.CentralConfigurationPatch
		makeCopy(m.patches, &res)
		return res
	}
}

func (m *MockAdminAPI) NumPatchesGetter() func() int {
	return func() int {
		m.Log.WithName("NumPatchesGetter(func)").Info("called")
		return len(m.PatchesGetter()())
	}
}

func (m *MockAdminAPI) SetProperty(key string, value interface{}) {
	m.Log.WithName("SetProperty").WithValues("key", key, "value", value).Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	if m.config == nil {
		m.config = make(map[string]interface{})
	}
	m.config[key] = value
}

func (m *MockAdminAPI) SetUnavailable(unavailable bool) {
	m.Log.WithName("SetUnavailable").WithValues("unavailable", unavailable).Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.unavailable = unavailable
}

func (m *MockAdminAPI) GetNodeConfig(
	_ context.Context,
) (admin.NodeConfig, error) {
	m.Log.WithName("GetNodeConfig").Info("called")
	return admin.NodeConfig{}, nil
}

//nolint:goerr113 // test code
func (s *ScopedMockAdminAPI) GetNodeConfig(
	ctx context.Context,
) (admin.NodeConfig, error) {
	brokers, err := s.Brokers(ctx)
	if err != nil {
		return admin.NodeConfig{}, err
	}
	if len(brokers) <= int(s.Ordinal) {
		return admin.NodeConfig{}, fmt.Errorf("broker not registered")
	}
	return admin.NodeConfig{
		NodeID: brokers[int(s.Ordinal)].NodeID,
	}, nil
}

func (m *MockAdminAPI) SetDirectValidationEnabled(directValidation bool) {
	m.Log.WithName("SetDirectValicationEnabled").WithValues("directValidation", directValidation).Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	m.directValidation = directValidation
}

func (m *MockAdminAPI) AddBroker(broker admin.Broker) {
	m.Log.WithName("AddBroker").WithValues("broker", broker).Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()

	m.brokers = append(m.brokers, broker)
}

func (m *MockAdminAPI) RemoveBroker(id int) bool {
	m.Log.WithName("RemoveBroker").WithValues("id", id).Info("called")
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

func (m *MockAdminAPI) Brokers(_ context.Context) ([]admin.Broker, error) {
	m.Log.WithName("RemoveBroker").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()

	return append([]admin.Broker{}, m.brokers...), nil
}

func (m *MockAdminAPI) BrokerStatusGetter(
	id int,
) func() admin.MembershipStatus {
	return func() admin.MembershipStatus {
		m.Log.WithName("BrokerStatusGetter(func)").WithValues("id", id).Info("called")
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

func (m *MockAdminAPI) DecommissionBroker(_ context.Context, id int) error {
	m.Log.WithName("DecommissionBroker").WithValues("id", id).Info("called")
	return m.SetBrokerStatus(id, admin.MembershipStatusDraining)
}

func (m *MockAdminAPI) RecommissionBroker(_ context.Context, id int) error {
	m.Log.WithName("RecommissionBroker").WithValues("id", id).Info("called")
	return m.SetBrokerStatus(id, admin.MembershipStatusActive)
}

func (m *MockAdminAPI) EnableMaintenanceMode(_ context.Context, _ int) error {
	m.Log.WithName("EnableMaintenanceMode").Info("called")
	return nil
}

func (m *MockAdminAPI) DisableMaintenanceMode(_ context.Context, _ int) error {
	m.Log.WithName("DisableMaintenanceMode").Info("called")
	return nil
}

func (m *MockAdminAPI) GetHealthOverview(_ context.Context) (admin.ClusterHealthOverview, error) {
	m.Log.WithName("GetHealthOverview").Info("called")
	m.monitor.Lock()
	defer m.monitor.Unlock()
	return admin.ClusterHealthOverview{
		IsHealthy: m.clusterHealth,
	}, nil
}

//nolint:goerr113 // test code
func (m *MockAdminAPI) SetBrokerStatus(
	id int, status admin.MembershipStatus,
) error {
	m.Log.WithName("SetBrokerStatus").WithValues("id", id, "status", status).Info("called")
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

func (m *MockAdminAPI) Broker(_ context.Context, nodeID int) (admin.Broker, error) {
	t := true
	return admin.Broker{
		NodeID:           nodeID,
		NumCores:         2,
		MembershipStatus: "",
		IsAlive:          &t,
		Version:          "unversioned",
		Maintenance:      m.MaintenanceStatus,
	}, nil
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
