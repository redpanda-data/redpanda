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
#include "kafka/server/offset_translator.h"
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
        return _translator->to_kafka_offset(_partition->start_offset());
    }

    model::offset high_watermark() const final {
        return _translator->to_kafka_offset(_partition->high_watermark());
    }

    model::offset last_stable_offset() const final {
        return _translator->to_kafka_offset(_partition->last_stable_offset());
    }

    bool is_leader() const final { return _partition->is_leader(); }

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
        auto source = co_await _partition->aborted_transactions(
          _translator->from_kafka_offset(base),
          _translator->from_kafka_offset(last));

        std::vector<cluster::rm_stm::tx_range> target;
        target.reserve(source.size());
        for (const auto& range : source) {
            target.push_back(cluster::rm_stm::tx_range{
              .pid = range.pid,
              .first = _translator->to_kafka_offset(range.first),
              .last = _translator->to_kafka_offset(range.last)});
        }

        co_return target;
    }

    cluster::partition_probe& probe() final { return _partition->probe(); }

private:
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::lw_shared_ptr<offset_translator> _translator;
};

} // namespace kafka
