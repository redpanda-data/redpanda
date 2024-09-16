// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloud

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/provider"
	"github.com/stretchr/testify/require"
)

type mockProvider struct {
	available bool
	name      string
	vmType    string
}

func (v *mockProvider) Name() string {
	return v.name
}

func (v *mockProvider) Init() (provider.InitializedProvider, error) {
	if !v.available {
		return nil, fmt.Errorf("mockProvider '%s' is not available", v.name)
	}
	return v, nil
}

func (v *mockProvider) VMType() (string, error) {
	return v.vmType, nil
}

func TestAvailableProvider(t *testing.T) {
	var (
		name1 = "provider1"
		name2 = "provider2"
		name3 = "provider3"
	)
	provider := make(map[string]provider.Provider)
	provider[name1] = &mockProvider{false, name1, ""}
	provider[name2] = &mockProvider{true, name2, ""}
	provider[name3] = &mockProvider{false, name3, ""}

	availableProvider, err := availableProviderFrom(provider)
	require.NoError(t, err)
	require.Equal(t, name2, availableProvider.Name())
}

func TestUnvailableProvider(t *testing.T) {
	var (
		name1 = "provider1"
		name2 = "provider2"
		name3 = "provider3"
	)
	provider := make(map[string]provider.Provider)
	provider[name1] = &mockProvider{false, name1, ""}
	provider[name2] = &mockProvider{false, name2, ""}
	provider[name3] = &mockProvider{false, name3, ""}

	_, err := availableProviderFrom(provider)
	require.EqualError(t, err, "The cloud provider couldn't be detected")
}
