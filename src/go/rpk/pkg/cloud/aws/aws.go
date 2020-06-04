package aws

import (
	"errors"
	"time"
	"vectorized/pkg/cloud/vendor"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
)

const name = "aws"

type AwsVendor struct{}

type InitializedAwsVendor struct {
	client *ec2metadata.EC2Metadata
}

func (v *AwsVendor) Name() string {
	return name
}

func (v *AwsVendor) Init() (vendor.InitializedVendor, error) {
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

func (v *InitializedAwsVendor) VmType() (string, error) {
	return v.client.GetMetadata("instance-type")
}

func (v *InitializedAwsVendor) Name() string {
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
