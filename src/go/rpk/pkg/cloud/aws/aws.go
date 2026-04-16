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
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/provider"
)

const name = "aws"

type AwsProvider struct{}

type InitializedAwsProvider struct {
	client *imds.Client
}

func (*AwsProvider) Name() string {
	return name
}

func (*AwsProvider) Init() (provider.InitializedProvider, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := imds.NewFromConfig(cfg)

	// Check availability by attempting a metadata request.
	resp, err := client.GetMetadata(ctx, &imds.GetMetadataInput{Path: "instance-type"})
	if err != nil {
		return nil, errors.New("provider AWS couldn't be initialized")
	}
	resp.Content.Close()
	return &InitializedAwsProvider{client: client}, nil
}

func (v *InitializedAwsProvider) VMType() (string, error) {
	resp, err := v.client.GetMetadata(context.Background(), &imds.GetMetadataInput{
		Path: "instance-type",
	})
	if err != nil {
		return "", err
	}
	defer resp.Content.Close()
	b, err := io.ReadAll(resp.Content)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (*InitializedAwsProvider) Name() string {
	return name
}
