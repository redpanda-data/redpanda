// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

// scrapeMetrics sums the named seastar counters across the cluster, reading
// each broker's internal /metrics endpoint once and adding every matching
// series (one per shard). Returns name -> cluster-wide total.
func scrapeMetrics(names []string) map[string]float64 {
	sums := make(map[string]float64, len(names))

	hosts := adminHosts()
	fmt.Printf("scrapeMetrics: scraping %d hosts %v for %v\n", len(hosts), hosts, names)

	for _, h := range hosts {
		resp, err := httpClient.Get("http://" + h + "/metrics")
		if err != nil {
			fmt.Printf("scrapeMetrics: host %s GET /metrics failed: %v\n", h, err)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("scrapeMetrics: host %s read /metrics body failed after %d bytes: %v\n", h, len(body), err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			snippet := string(body)
			if len(snippet) > 200 {
				snippet = snippet[:200]
			}
			fmt.Printf("scrapeMetrics: host %s GET /metrics -> %d: %s\n", h, resp.StatusCode, snippet)
			continue
		}
		matched := 0
		hostSums := make(map[string]float64, len(names))
		for line := range strings.SplitSeq(string(body), "\n") {
			if line == "" || line[0] == '#' {
				continue
			}
			// Prometheus text: `name{labels} value` or `name value`.
			for _, name := range names {
				if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
					continue
				}
				fields := strings.Fields(line)
				if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
					sums[name] += v
					hostSums[name] += v
					matched++
				}
				break
			}
		}
		fmt.Printf("scrapeMetrics: host %s status=%d bytes=%d matched_series=%d sums=%v\n",
			h, resp.StatusCode, len(body), matched, hostSums)
	}
	return sums
}

// anytime_check_cloud_io: assert the cloud-topics I/O paths are exercised at
// least sometimes across the run — the L0 batcher uploads to object storage,
// and the L1 read path (file-io reads, plus index-driven byte skipping) runs.
//
// Note: the batch-cache miss / pipeline cloud-read counters stay ~0 here — the
// batcher populates caches write-through and the workload reads recent, local
// data, so reads are served warm. Genuinely cold object-storage reads would
// need cache eviction (larger backlog) or reads of data no broker cached; see
// the read path via level_one_file_io instead.
func checkCloudIO() error {
	const (
		readMetric     = "vectorized_cloud_topics_level_one_reader_read_bytes"
		uploadMetric   = "vectorized_cloud_topics_batcher_bytes_uploaded"
		fileReadMetric = "vectorized_cloud_topics_level_one_file_io_reads"
		skippedMetric  = "vectorized_cloud_topics_level_one_reader_skipped_bytes"
	)
	sums := scrapeMetrics([]string{
		readMetric, uploadMetric, fileReadMetric, skippedMetric,
	})

	assert.Sometimes(sums[readMetric] > 0,
		"cloud topics L1 reader sometimes reads bytes",
		map[string]any{"read_bytes": sums[readMetric]})
	assert.Sometimes(sums[uploadMetric] > 0,
		"cloud topics batcher sometimes uploads bytes",
		map[string]any{"bytes_uploaded": sums[uploadMetric]})
	assert.Sometimes(sums[fileReadMetric] > 0,
		"cloud topics L1 file-io sometimes performs reads",
		map[string]any{"reads": sums[fileReadMetric]})
	assert.Sometimes(sums[skippedMetric] > 0,
		"cloud topics L1 reader sometimes skips bytes via index",
		map[string]any{"skipped_bytes": sums[skippedMetric]})

	fmt.Printf("L1 read_bytes=%.0f uploaded=%.0f file_reads=%.0f skipped_bytes=%.0f\n",
		sums[readMetric], sums[uploadMetric], sums[fileReadMetric], sums[skippedMetric])
	return nil
}
