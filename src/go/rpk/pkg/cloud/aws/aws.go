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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/provider"
)

const name = "aws"

type AwsProvider struct{}

type InitializedAwsProvider struct {
	client *ec2metadata.EC2Metadata
}

func (*AwsProvider) Name() string {
	return name
}

func (*AwsProvider) Init() (provider.InitializedProvider, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	client := ec2metadata.New(s)
	if available(client, 500*time.Millisecond) {
		return &InitializedAwsProvider{client}, nil
	}
	return nil, errors.New("provider AWS couldn't be initialized")
}

func (v *InitializedAwsProvider) VMType() (string, error) {
	return v.client.GetMetadata("instance-type")
}

func (*InitializedAwsProvider) Name() string {
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
