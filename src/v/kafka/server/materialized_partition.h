/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/server/partition_proxy.h"
#include "storage/log.h"

namespace kafka {
class materialized_partition final : public kafka::partition_proxy::impl {
public:
    explicit materialized_partition(storage::log log)
      : _log(log) {}

    const model::ntp& ntp() const final { return _log.config().ntp(); }
    model::offset start_offset() const final {
        return _log.offsets().start_offset;
    }

    model::offset high_watermark() const final {
        return _log.offsets().dirty_offset;
    }

    model::offset last_stable_offset() const final {
        return _log.offsets().dirty_offset;
    }

    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point>) final {
        return _log.make_reader(cfg);
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(model::timestamp ts, ss::io_priority_class io_pc) final {
        storage::timequery_config cfg(ts, _log.offsets().dirty_offset, io_pc);
        return _log.timequery(cfg);
    };

private:
    storage::log _log;
};

} // namespace kafka
