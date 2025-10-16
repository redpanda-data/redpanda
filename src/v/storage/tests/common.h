/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/fundamental.h"
#include "storage/batch_cache.h"
#include "storage/log_manager.h"
#include "storage/segment.h"

namespace storage::testing_details {
class log_manager_accessor {
public:
    static batch_cache& batch_cache(storage::log_manager& m) {
        return m._batch_cache;
    }

    static ss::future<> housekeeping_scan(storage::log_manager& m) {
        return m.housekeeping_scan(m.lowest_ts_to_retain());
    }

    static storage::log_manager::compaction_list_type&
    logs_list(storage::log_manager& m) {
        return m._logs_list;
    }

    static ss::abort_source& abort_source(storage::log_manager& m) {
        return m._abort_source;
    }
};

class offset_tracker_accessor {
public:
    static model::offset& base_offset(storage::segment::offset_tracker& ot) {
        return ot._base_offset;
    }
};

class segment_accessor {
public:
    static std::optional<size_t> data_disk_usage_size(storage::segment& s) {
        return s._data_disk_usage_size;
    }

    static std::optional<size_t> compaction_index_size(storage::segment& s) {
        return s._compaction_index_size;
    }
};

} // namespace storage::testing_details
