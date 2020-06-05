package gcp

import (
	"errors"
	"net/http"
	"path/filepath"
	"time"
	"vectorized/pkg/cloud/vendor"

	"cloud.google.com/go/compute/metadata"
)

const name = "gcp"

type GcpVendor struct{}

type InitializedGcpVendor struct {
	client *metadata.Client
}

func (v *GcpVendor) Name() string {
	return name
}

func (v *GcpVendor) Init() (vendor.InitializedVendor, error) {
	timeout := 500 * time.Millisecond
	client := metadata.NewClient(&http.Client{Timeout: timeout})
	if available(client, timeout) {
		return &InitializedGcpVendor{client}, nil
	}
	return nil, errors.New("vendor GCP couldn't be initialized")
}

func (v *InitializedGcpVendor) VmType() (string, error) {
	t, err := v.client.Get("instance/machine-type")
	if err != nil {
		return "", err
	}
	// t will have the form projects/<project ID>/machineTypes/<machine type>
	// so we have to return only the last segment.
	// See https://cloud.google.com/compute/docs/storing-retrieving-metadata#default
	// for more info.
	return filepath.Base(t), nil
}

func (v *InitializedGcpVendor) Name() string {
	return name
}

func available(client *metadata.Client, timeout time.Duration) bool {
	result := make(chan bool)

	go func(c *metadata.Client, res chan<- bool) {
		_, err := c.ProjectID()
		res <- err == nil
	}(client, result)

	select {
	case res := <-result:
		return res
	case <-time.After(timeout):
		return false
	}
}
