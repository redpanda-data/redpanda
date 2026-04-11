// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package publicapi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"buf.build/gen/go/redpandadata/dataplane/connectrpc/go/redpanda/api/dataplane/v1/dataplanev1connect"
	"buf.build/gen/go/redpandadata/dataplane/connectrpc/go/redpanda/api/dataplane/v1alpha3/dataplanev1alpha3connect"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

// DataPlaneClientSet holds the respective service clients to interact with
// the data plane endpoints of the Public API.
type DataPlaneClientSet struct {
	ACL           dataplanev1connect.ACLServiceClient
	AIAgent       dataplanev1alpha3connect.AIAgentServiceClient
	CloudStorage  dataplanev1connect.CloudStorageServiceClient
	KafkaConnect  dataplanev1connect.KafkaConnectServiceClient
	KnowledgeBase dataplanev1alpha3connect.KnowledgeBaseServiceClient
	MCPServer     dataplanev1alpha3connect.MCPServerServiceClient
	Monitoring    dataplanev1connect.MonitoringServiceClient
	Pipeline      dataplanev1connect.PipelineServiceClient
	Quota         dataplanev1connect.QuotaServiceClient
	Secret        dataplanev1connect.SecretServiceClient
	Security      dataplanev1connect.SecurityServiceClient
	ShadowLink    dataplanev1connect.ShadowLinkServiceClient
	Topic         dataplanev1connect.TopicServiceClient
	Transform     *transformServiceClient
	User          dataplanev1connect.UserServiceClient

	m         sync.RWMutex
	authToken string
}

type DataplaneAPIURLContextKey struct{}

type DynamicTransport struct {
	Base http.RoundTripper
}

func (t *DynamicTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if urlStr, ok := req.Context().Value(DataplaneAPIURLContextKey{}).(string); ok {
		u, err := url.Parse(urlStr)
		if err == nil {
			req.URL.Host = u.Host
			req.URL.Scheme = u.Scheme
		}
	}
	return t.Base.RoundTrip(req)
}

func (dpCl *DataPlaneClientSet) Token() string {
	dpCl.m.RLock()
	defer dpCl.m.RUnlock()
	return dpCl.authToken
}

func (dpCl *DataPlaneClientSet) UpdateAuthToken(authToken string) {
	dpCl.m.Lock()
	defer dpCl.m.Unlock()
	dpCl.authToken = authToken
}

// NewDataPlaneClientSet creates a Public API client set with the service
// clients of each resource available to interact with this package.
func NewDataPlaneClientSet(host, authToken string, opts ...connect.ClientOption) (*DataPlaneClientSet, error) {
	dpCl := &DataPlaneClientSet{}
	dpCl.authToken = authToken

	// Use reloading auth interceptor for dynamic transport (empty host), static auth for specific host
	var authInterceptor connect.UnaryInterceptorFunc
	if host == "" {
		authInterceptor = newReloadingAuthInterceptor(dpCl.Token)
	} else {
		authInterceptor = newAuthInterceptor(authToken)
	}

	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			authInterceptor,                            // Add the Bearer token.
			newLoggerInterceptor(),                     // Add logs to every request.
			newAgentInterceptor(defaultRpkUserAgent()), // Add the User-Agent.
		),
	}, opts...)

	// Use dynamic transport if host is empty (for multi-dataplane scenarios)
	var httpCl *http.Client
	if host == "" {
		httpCl = &http.Client{
			Timeout:   time.Second * 30,
			Transport: &DynamicTransport{Base: http.DefaultTransport},
		}
	} else {
		httpCl = http.DefaultClient
	}

	dpCl.Transform = newTransformServiceClient(httpCl, host, authToken, opts...)
	dpCl.CloudStorage = dataplanev1connect.NewCloudStorageServiceClient(httpCl, host, opts...)
	dpCl.User = dataplanev1connect.NewUserServiceClient(httpCl, host, opts...)
	dpCl.Secret = dataplanev1connect.NewSecretServiceClient(httpCl, host, opts...)
	dpCl.MCPServer = dataplanev1alpha3connect.NewMCPServerServiceClient(httpCl, host, opts...)
	dpCl.Topic = dataplanev1connect.NewTopicServiceClient(httpCl, host, opts...)
	dpCl.Pipeline = dataplanev1connect.NewPipelineServiceClient(httpCl, host, opts...)
	dpCl.ACL = dataplanev1connect.NewACLServiceClient(httpCl, host, opts...)
	dpCl.KafkaConnect = dataplanev1connect.NewKafkaConnectServiceClient(httpCl, host, opts...)
	dpCl.Quota = dataplanev1connect.NewQuotaServiceClient(httpCl, host, opts...)
	dpCl.Security = dataplanev1connect.NewSecurityServiceClient(httpCl, host, opts...)
	dpCl.Monitoring = dataplanev1connect.NewMonitoringServiceClient(httpCl, host, opts...)
	dpCl.AIAgent = dataplanev1alpha3connect.NewAIAgentServiceClient(httpCl, host, opts...)
	dpCl.KnowledgeBase = dataplanev1alpha3connect.NewKnowledgeBaseServiceClient(httpCl, host, opts...)
	dpCl.ShadowLink = dataplanev1connect.NewShadowLinkServiceClient(httpCl, host, opts...)

	return dpCl, nil
}

// DataplaneClientFromRpkProfile creates a DataPlaneClientSet with the
// information loaded in the profile. If the profile is not from cloud it will
// return an error.
func DataplaneClientFromRpkProfile(p *config.RpkProfile, opts ...connect.ClientOption) (*DataPlaneClientSet, error) {
	url, err := p.CloudCluster.CheckClusterURL()
	if err != nil {
		return nil, fmt.Errorf("unable to get cluster information from your profile: %v", err)
	}
	return NewDataPlaneClientSet(url, p.CurrentAuth().AuthToken, opts...)
}

// ListAllShadowLinkTopics returns all the ShadowTopics for a given shadow link
// using the pagination feature to traverse all pages of the list.
func (dpCl *DataPlaneClientSet) ListAllShadowLinkTopics(ctx context.Context, shadowLinkName string) ([]*dataplanev1.ShadowTopic, error) {
	fetchPage := func(ctx context.Context, pageToken string) ([]*dataplanev1.ShadowTopic, string, error) {
		req := connect.NewRequest(&dataplanev1.ListShadowLinkTopicsRequest{
			ShadowLinkName: shadowLinkName,
			PageToken:      pageToken,
			PageSize:       100,
		})
		resp, err := dpCl.ShadowLink.ListShadowLinkTopics(ctx, req)
		if err != nil {
			return nil, "", err
		}
		return resp.Msg.GetShadowTopics(), resp.Msg.GetNextPageToken(), nil
	}
	return Paginate(ctx, maxPages, fetchPage)
}
