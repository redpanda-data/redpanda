/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "metrics/metrics.h"

#include <chrono>
#include <cstdint>

namespace cloud_topics {

class level_one_reader_probe {
public:
    level_one_reader_probe();

    void register_footer_read(size_t bytes_requested) {
        _footer_bytes_read += bytes_requested;
    }

    void register_bytes_read(size_t bytes_read) { _bytes_read += bytes_read; }

    void register_bytes_skipped(size_t bytes_skipped) {
        _bytes_skipped += bytes_skipped;
    }

    // Per-phase wall-clock accumulators for the serialized per-partition L1
    // read (CORE-15812). At fetch_max_read_concurrency=1 a fetch reads its
    // partitions strictly serially and the broker is latency-bound; these
    // attribute that wall-clock to a specific phase of a single read so the
    // dominant one can be identified before picking a fix. Each phase pairs a
    // cumulative duration with an op count, so the mean per-call latency is
    // recoverable as duration/count and the inter-phase ratio gives dominance.
    //
    // Phases (see level_one_log_reader_impl):
    //  - metastore_lookup: get_extent_metadata_forwards RPC round-trip
    //  - footer_read:      per-object footer fetch (local cache disk I/O)
    //  - stream_open:      opening the object data stream (cache lookup/open)
    //  - batch_read:       streaming the partition data + parsing batches
    //                      (no decompression happens here, so this is
    //                      dominated by local cache disk I/O, not CPU)
    void record_metastore_lookup_duration(std::chrono::nanoseconds d) {
        _metastore_lookup_ns += to_ns(d);
        ++_metastore_lookup_count;
    }
    void record_footer_read_duration(std::chrono::nanoseconds d) {
        _footer_read_ns += to_ns(d);
        ++_footer_read_count;
    }
    void record_stream_open_duration(std::chrono::nanoseconds d) {
        _stream_open_ns += to_ns(d);
        ++_stream_open_count;
    }
    void record_batch_read_duration(std::chrono::nanoseconds d) {
        _batch_read_ns += to_ns(d);
        ++_batch_read_count;
    }

private:
    static uint64_t to_ns(std::chrono::nanoseconds d) {
        return d.count() < 0 ? 0 : static_cast<uint64_t>(d.count());
    }

    void setup_metrics();
    // The per-phase timing counters are registered on the public endpoint:
    // CDT runs disable internal metrics, and the phase ratios are the signal
    // we need there.
    void setup_public_metrics();

    uint64_t _footer_bytes_read{0};
    uint64_t _bytes_read{0};
    uint64_t _bytes_skipped{0};

    uint64_t _metastore_lookup_ns{0};
    uint64_t _metastore_lookup_count{0};
    uint64_t _footer_read_ns{0};
    uint64_t _footer_read_count{0};
    uint64_t _stream_open_ns{0};
    uint64_t _stream_open_count{0};
    uint64_t _batch_read_ns{0};
    uint64_t _batch_read_count{0};

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace cloud_topics
