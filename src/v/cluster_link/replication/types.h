/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "ssx/semaphore.h"

#include <cstdio>

namespace cluster_link::replication {
struct partition_offsets_report {
    kafka::offset source_start_offset{-1};
    kafka::offset source_hwm{-1};
    kafka::offset source_lso{::model::invalid_lso};
    ss::lowres_clock::time_point update_time{};
    kafka::offset shadow_hwm{-1};

    fmt::iterator format_to(fmt::iterator) const;
};

struct fetch_data {
    chunked_vector<::model::record_batch> batches;
    ssx::semaphore_units units;
};

struct fetch_counters {
    size_t n_bytes{};
    size_t n_records{};

    static fetch_counters from_fetch_data(const fetch_data& data);

    fetch_counters& operator+=(const fetch_counters& rhs) {
        n_bytes += rhs.n_bytes;
        n_records += rhs.n_records;
        return *this;
    }
};

struct link_data_probe {
    fetch_counters total_fetched;
    fetch_counters total_written;

    void add_fetched(const fetch_counters& fc) { total_fetched += fc; }

    void add_written(const fetch_counters& fc) { total_written += fc; }
};

using link_data_probe_ptr = ss::lw_shared_ptr<link_data_probe>;
} // namespace cluster_link::replication
