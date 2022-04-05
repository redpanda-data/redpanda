/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "coproc/partition.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "raft/errc.h"
#include "storage/log.h"

#include <system_error>

namespace kafka {
class materialized_partition final : public kafka::partition_proxy::impl {
public:
    explicit materialized_partition(
      ss::lw_shared_ptr<coproc::partition> p) noexcept
      : _probe(cluster::make_materialized_partition_probe())
      , _partition(p) {}

    const model::ntp& ntp() const final { return _partition->ntp(); }
    model::offset start_offset() const final {
        model::offset start = _partition->start_offset();
        return start < model::offset{0} ? model::offset{0} : start;
    }

    model::offset high_watermark() const final {
        return raft::details::next_offset(_partition->dirty_offset());
    }

    model::offset last_stable_offset() const final {
        return raft::details::next_offset(_partition->dirty_offset());
    }

    bool is_leader() const final { return _partition->is_leader(); }

    kafka::leader_epoch leader_epoch() const final {
        return leader_epoch_from_term(_partition->term());
    }

    std::optional<model::offset>
    get_leader_epoch_last_offset(kafka::leader_epoch epoch) const final {
        return _partition->get_term_last_offset(model::term_id(epoch()));
    }

    ss::future<std::error_code> linearizable_barrier() final {
        return _partition->source_partition()->linearizable_barrier().then(
          [](result<model::offset> r) {
              if (r) {
                  return raft::make_error_code(raft::errc::success);
              }
              return r.error();
          });
    }

    ss::future<storage::translating_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point>) final {
        co_return storage::translating_reader(
          co_await _partition->make_reader(cfg));
    }

    ss::future<std::optional<storage::timequery_result>> timequery(
      model::timestamp ts,
      model::offset offset_limit,
      ss::io_priority_class io_pc) final {
        storage::timequery_config cfg(ts, offset_limit, io_pc);
        return _partition->timequery(cfg);
    };

    ss::future<std::vector<cluster::rm_stm::tx_range>> aborted_transactions(
      model::offset,
      model::offset,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final {
        return ss::make_ready_future<std::vector<cluster::rm_stm::tx_range>>(
          std::vector<cluster::rm_stm::tx_range>());
    }

    cluster::partition_probe& probe() final { return _probe; }

private:
    static model::offset offset_or_zero(model::offset o) {
        return o > model::offset(0) ? o : model::offset(0);
    }

    cluster::partition_probe _probe;
    ss::lw_shared_ptr<coproc::partition> _partition;
};

} // namespace kafka
