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

#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "storage/types.h"

#include <optional>

namespace kafka {

/**
 * Kafka layer wrapper for partition, this wrapper performs offsets translation
 * to eliminate offset skew caused by existantece of Raft configuration batches.
 */
class partition_proxy {
public:
    struct impl {
        virtual const model::ntp& ntp() const = 0;
        virtual model::offset start_offset() const = 0;
        virtual model::offset high_watermark() const = 0;
        virtual model::offset last_stable_offset() const = 0;
        virtual bool is_leader() const = 0;
        virtual ss::future<model::record_batch_reader> make_reader(
          storage::log_reader_config,
          std::optional<model::timeout_clock::time_point>)
          = 0;
        virtual ss::future<std::optional<storage::timequery_result>>
          timequery(model::timestamp, ss::io_priority_class) = 0;
        virtual ss::future<std::vector<cluster::rm_stm::tx_range>>
          aborted_transactions(model::offset, model::offset) = 0;
        virtual cluster::partition_probe& probe() = 0;
        virtual ~impl() noexcept = default;
    };

    explicit partition_proxy(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    model::offset start_offset() const { return _impl->start_offset(); }

    model::offset high_watermark() const { return _impl->high_watermark(); }

    model::offset last_stable_offset() const {
        return _impl->last_stable_offset();
    }

    bool is_leader() const { return _impl->is_leader(); }

    const model::ntp& ntp() const { return _impl->ntp(); }

    ss::future<std::vector<cluster::rm_stm::tx_range>>
    aborted_transactions(model::offset base, model::offset last) {
        return _impl->aborted_transactions(base, last);
    }

    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point> deadline = std::nullopt) {
        return _impl->make_reader(cfg, deadline);
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(model::timestamp ts, ss::io_priority_class io_pc) {
        return _impl->timequery(ts, io_pc);
    }

    cluster::partition_probe& probe() { return _impl->probe(); }

private:
    std::unique_ptr<impl> _impl;
};

template<typename Impl, typename... Args>
partition_proxy make_partition_proxy(Args&&... args) {
    return partition_proxy(std::make_unique<Impl>(std::forward<Args>(args)...));
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp&, cluster::metadata_cache&, cluster::partition_manager&);

} // namespace kafka
