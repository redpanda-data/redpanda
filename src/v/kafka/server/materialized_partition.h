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
#include "cluster/partition_probe.h"
#include "kafka/server/partition_proxy.h"
#include "storage/log.h"

namespace kafka {
class materialized_partition final : public kafka::partition_proxy::impl {
public:
    materialized_partition(
      storage::log log, ss::lw_shared_ptr<cluster::partition> p)
      : _log(log)
      , _probe(cluster::make_materialized_partition_probe())
      , _partition(p) {}

    const model::ntp& ntp() const final { return _log.config().ntp(); }
    model::offset start_offset() const final {
        model::offset start = _log.offsets().start_offset;
        return start < model::offset{0} ? model::offset{0} : start;
    }

    model::offset high_watermark() const final {
        return raft::details::next_offset(_log.offsets().dirty_offset);
    }

    model::offset last_stable_offset() const final {
        return raft::details::next_offset(_log.offsets().dirty_offset);
    }

    bool is_leader() const final { return _partition->is_leader(); }

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

    ss::future<std::vector<cluster::rm_stm::tx_range>>
    aborted_transactions(model::offset, model::offset) final {
        return ss::make_ready_future<std::vector<cluster::rm_stm::tx_range>>(
          std::vector<cluster::rm_stm::tx_range>());
    }

    cluster::partition_probe& probe() final { return _probe; }

private:
    static model::offset offset_or_zero(model::offset o) {
        return o > model::offset(0) ? o : model::offset(0);
    }

    storage::log _log;
    cluster::partition_probe _probe;
    ss::lw_shared_ptr<cluster::partition> _partition;
};

} // namespace kafka
