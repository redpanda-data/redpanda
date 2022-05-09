// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package aws

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/vendor"
)

const name = "aws"

type AwsVendor struct{}

type InitializedAwsVendor struct {
	client *ec2metadata.EC2Metadata
}

func (*AwsVendor) Name() string {
	return name
}

func (*AwsVendor) Init() (vendor.InitializedVendor, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	client := ec2metadata.New(s)
	if available(client, 500*time.Millisecond) {
		return &InitializedAwsVendor{client}, nil
	}
	return nil, errors.New("vendor AWS couldn't be initialized")
}

func (v *InitializedAwsVendor) VMType() (string, error) {
	return v.client.GetMetadata("instance-type")
}

func (*InitializedAwsVendor) Name() string {
	return name
}

func available(client *ec2metadata.EC2Metadata, timeout time.Duration) bool {
	result := make(chan bool)

	go func(c *ec2metadata.EC2Metadata, res chan<- bool) {
		res <- c.Available()
	}(client, result)

	select {
	case res := <-result:
		return res
	case <-time.After(timeout):
		return false
	}
}
