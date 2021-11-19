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

#include "cluster/partition.h"
#include "cluster/partition_probe.h"
#include "kafka/server/partition_proxy.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/types.h"

#include <seastar/core/coroutine.hh>

#include <memory>

namespace kafka {

class replicated_partition final : public kafka::partition_proxy::impl {
public:
    explicit replicated_partition(
      ss::lw_shared_ptr<cluster::partition> p) noexcept;

    const model::ntp& ntp() const final { return _partition->ntp(); }

    model::offset start_offset() const final {
        auto local_kafka_start_offset = _translator->from_log_offset(
          _partition->start_offset());
        if (
          _partition->cloud_data_available()
          && (_partition->start_cloud_offset() < local_kafka_start_offset)) {
            return _partition->start_cloud_offset();
        }
        return local_kafka_start_offset;
    }

    model::offset high_watermark() const final {
        return _translator->from_log_offset(_partition->high_watermark());
    }

    model::offset last_stable_offset() const final {
        return _translator->from_log_offset(_partition->last_stable_offset());
    }

    bool is_leader() const final { return _partition->is_leader(); }

    ss::future<result<model::offset>> linearizable_barrier() {
        auto r = co_await _partition->linearizable_barrier();
        if (r) {
            co_return result<model::offset>(
              _translator->from_log_offset(r.value()));
        }
        co_return r;
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(model::timestamp ts, ss::io_priority_class io_pc) final;

    ss::future<result<model::offset>>
      replicate(model::record_batch_reader, raft::replicate_options);

    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options);

    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point>) final;

    ss::future<std::vector<cluster::rm_stm::tx_range>>
    aborted_transactions(model::offset base, model::offset last) final {
        model::offset local_kafka_start_offset = _translator->from_log_offset(
          _partition->start_offset());
        if (base < local_kafka_start_offset) {
            // TODO: get offset translation information for the offsets range
            // that we have read and use it to query
            // _partition->aborted_transactions.
            co_return std::vector<cluster::rm_stm::tx_range>{};
        }

        auto source = co_await _partition->aborted_transactions(
          _translator->to_log_offset(base), _translator->to_log_offset(last));

        std::vector<cluster::rm_stm::tx_range> target;
        target.reserve(source.size());
        for (const auto& range : source) {
            target.push_back(cluster::rm_stm::tx_range{
              .pid = range.pid,
              .first = _translator->from_log_offset(range.first),
              .last = _translator->from_log_offset(range.last)});
        }

        co_return target;
    }

    cluster::partition_probe& probe() final { return _partition->probe(); }

private:
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::lw_shared_ptr<raft::offset_translator> _translator;
};

} // namespace kafka
