/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/protocol/types.h"
#include "ssx/metrics.h"

namespace kafka {

class handler_probe {
public:
    explicit handler_probe(api_key);
    void setup_metrics();

    void sample_in_progress();
    void request_completed() {
        sample_in_progress();

        _requests_completed++;
        _requests_in_progress--;
    }
    void request_errored() { _requests_errored++; }
    void request_started() {
        sample_in_progress();

        _requests_in_progress++;
    }

    void add_bytes_received(size_t bytes) { _bytes_received += bytes; }

    void add_bytes_sent(size_t bytes) { _bytes_sent += bytes; }

private:
    ss::metrics::metric_groups _metrics;
    api_key _key;

    uint64_t _requests_completed{0};
    uint64_t _requests_errored{0};

    uint64_t _requests_in_progress{0};
    uint64_t _requests_in_progress_every_ns{0};

    ss::lowres_clock::time_point _last_recorded_in_progress;

    uint64_t _bytes_received{0};
    uint64_t _bytes_sent{0};
};

class handler_probe_manager {
public:
    handler_probe_manager();
    handler_probe& get_probe(api_key key);

private:
    std::vector<std::unique_ptr<handler_probe>> _probes;
    size_t _unknown_handler_index;
};

} // namespace kafka
