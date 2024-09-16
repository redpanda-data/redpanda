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

func vendors() map[string]provider.Vendor {
	vendors := make(map[string]provider.Vendor)
	awsVendor := &aws.AwsVendor{}
	vendors[awsVendor.Name()] = awsVendor
	gcpVendor := &gcp.GcpVendor{}
	vendors[gcpVendor.Name()] = gcpVendor

	return vendors
}

// AvailableVendor tries to initialize the vendors and returns the one available, or an error
// if none could be initialized.
func AvailableVendor() (provider.InitializedVendor, error) {
	return availableVendorFrom(vendors())
}

func availableVendorFrom(
	vendors map[string]provider.Vendor,
) (provider.InitializedVendor, error) {
	type initResult struct {
		vendor provider.InitializedVendor
		err    error
	}
	initAsync := func(v provider.Vendor, c chan<- initResult) {
		iv, err := v.Init()
		c <- initResult{iv, err}
	}
	var wg sync.WaitGroup
	wg.Add(len(vendors))

	ch := make(chan initResult)
	go func() {
		wg.Wait()
		close(ch)
	}()

	for _, v := range vendors {
		go initAsync(v, ch)
	}

	var v provider.InitializedVendor
	for res := range ch {
		if res.err == nil {
			v = res.vendor
		} else {
			zap.L().Sugar().Debug(res.err)
		}
		wg.Done()
	}
	if v == nil {
		return nil, errors.New("The cloud vendor couldn't be detected")
	}
	return v, nil
}
