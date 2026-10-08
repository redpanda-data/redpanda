// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package metastore

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	cloudtopicsv1 "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1/cloudtopicsv1connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
)

const numMetastorePartitionsKey = "cloud_topics_num_metastore_partitions"

// adminTransport bundles the inputs every metastore subcommand needs: a
// Connect client for the (internal, unstable) MetastoreService and the
// discovered metastore partition count.
type adminTransport struct {
	MS         cloudtopicsv1connect.MetastoreServiceClient
	NumMSParts uint32
}

// loadAdminTransport reads the rpk profile, builds an authenticated admin
// client, discovers the metastore partition count, and returns a Connect
// client for the MetastoreService. It is the per-command setup phase shared
// by `layout` and `files`. Fatal errors are surfaced via out.MaybeDie.
func loadAdminTransport(ctx context.Context, fs afero.Fs, params *config.Params) adminTransport {
	p, err := params.LoadVirtualProfile(fs)
	out.MaybeDie(err, "rpk unable to load config: %v", err)

	if len(p.AdminAPI.Addresses) == 0 {
		out.Die("no admin API addresses configured in the current rpk profile")
	}

	cl, err := adminapi.NewClient(ctx, fs, p)
	out.MaybeDie(err, "unable to initialize admin client: %v", err)

	count, err := fetchNumMetastorePartitions(ctx, cl)
	out.MaybeDie(err, "unable to discover metastore partition count: %v", err)

	// rpadmin.AdminAPI implements connect.HTTPClient and already handles
	// auth, TLS, and admin-host addressing; baseURL "/" lets AdminAPI.Do
	// prepend the resolved host, matching how rpadmin's own v2 service
	// accessors are wired.
	ms := cloudtopicsv1connect.NewMetastoreServiceClient(cl, "/")
	return adminTransport{MS: ms, NumMSParts: count}
}

// partitionResult is the GetDatabaseStats fan-out result type used by
// layout and files. It stays here because it's transport-shaped.
type partitionResult struct {
	Partition uint32
	Stats     *cloudtopicsv1.GetDatabaseStatsResponse
	Err       error
}

// runDatabaseStatsBroadcast fans GetDatabaseStats out across the requested
// metastore partition set with bounded concurrency, returning the result in
// ascending partition order. Per-partition errors surface in
// partitionResult.Err; they never abort the run.
func runDatabaseStatsBroadcast(ctx context.Context, t adminTransport, parts []uint32) []partitionResult {
	const maxInFlight = 8
	results := make([]partitionResult, len(parts))
	sem := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	for i, part := range parts {
		i, part := i, part
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = partitionResult{Partition: part}
			resp, err := t.MS.GetDatabaseStats(ctx, connect.NewRequest(&cloudtopicsv1.GetDatabaseStatsRequest{
				MetastorePartition: part,
			}))
			if err != nil {
				results[i].Err = err
				return
			}
			results[i].Stats = resp.Msg
		}()
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].Partition < results[j].Partition })
	return results
}

// fetchNumMetastorePartitions reads cloud_topics_num_metastore_partitions
// via the cluster config endpoint. A missing or non-numeric value means
// Cloud Topics isn't enabled.
func fetchNumMetastorePartitions(ctx context.Context, cl *rpadmin.AdminAPI) (uint32, error) {
	cfg, err := cl.SingleKeyConfig(ctx, numMetastorePartitionsKey)
	if err != nil {
		return 0, err
	}
	v, ok := cfg[numMetastorePartitionsKey]
	if !ok {
		return 0, fmt.Errorf("cloud topics not enabled on this cluster (%s unset)", numMetastorePartitionsKey)
	}
	switch n := v.(type) {
	case float64:
		if n < 1 {
			return 0, fmt.Errorf("invalid %s value %v", numMetastorePartitionsKey, v)
		}
		return uint32(n), nil
	case int:
		if n < 1 {
			return 0, fmt.Errorf("invalid %s value %v", numMetastorePartitionsKey, v)
		}
		return uint32(n), nil
	case int64:
		if n < 1 {
			return 0, fmt.Errorf("invalid %s value %v", numMetastorePartitionsKey, v)
		}
		return uint32(n), nil
	default:
		return 0, fmt.Errorf("unexpected %s type %T", numMetastorePartitionsKey, v)
	}
}
