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
	"errors"
	"sync"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/aws"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/gcp"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/provider"
	"go.uber.org/zap"
)

func providers() map[string]provider.Provider {
	providers := make(map[string]provider.Provider)
	awsProvider := &aws.AwsProvider{}
	providers[awsProvider.Name()] = awsProvider
	gcpProvider := &gcp.GcpProvider{}
	providers[gcpProvider.Name()] = gcpProvider

	return providers
}

// AvailableProviders tries to initialize the providers and returns the one available, or an error
// if none could be initialized.
func AvailableProviders() (provider.InitializedProvider, error) {
	return availableProviderFrom(providers())
}

func availableProviderFrom(
	providers map[string]provider.Provider,
) (provider.InitializedProvider, error) {
	type initResult struct {
		provider provider.InitializedProvider
		err      error
	}
	initAsync := func(v provider.Provider, c chan<- initResult) {
		iv, err := v.Init()
		c <- initResult{iv, err}
	}
	var wg sync.WaitGroup
	wg.Add(len(providers))

	ch := make(chan initResult)
	go func() {
		wg.Wait()
		close(ch)
	}()

	for _, v := range providers {
		go initAsync(v, ch)
	}

	var v provider.InitializedProvider
	for res := range ch {
		if res.err == nil {
			v = res.provider
		} else {
			zap.L().Sugar().Debug(res.err)
		}
		wg.Done()
	}
	if v == nil {
		return nil, errors.New("The cloud provider couldn't be detected")
	}
	return v, nil
}
