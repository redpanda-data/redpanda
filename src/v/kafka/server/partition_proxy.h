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

#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "kafka/protocol/errors.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/record_batch_reader.h"
#include "storage/translating_reader.h"
#include "storage/types.h"

#include <optional>
#include <system_error>

namespace kafka {

/**
 * Kafka layer wrapper for partition, this wrapper performs offsets translation
 * to eliminate offset skew caused by existantece of Raft configuration batches.
 */
class partition_proxy {
public:
    struct impl {
        virtual const model::ntp& ntp() const = 0;
        virtual ss::future<result<model::offset, error_code>>
          sync_effective_start(model::timeout_clock::duration) = 0;
        virtual model::offset start_offset() const = 0;
        virtual model::offset high_watermark() const = 0;
        virtual checked<model::offset, error_code>
        last_stable_offset() const = 0;
        virtual kafka::leader_epoch leader_epoch() const = 0;
        virtual ss::future<std::optional<model::offset>>
          get_leader_epoch_last_offset(kafka::leader_epoch) const = 0;
        virtual bool is_elected_leader() const = 0;
        virtual bool is_leader() const = 0;
        virtual ss::future<std::error_code> linearizable_barrier() = 0;
        virtual ss::future<error_code>
          prefix_truncate(model::offset, ss::lowres_clock::time_point) = 0;
        virtual ss::future<storage::translating_reader> make_reader(
          storage::log_reader_config,
          std::optional<model::timeout_clock::time_point>)
          = 0;
        virtual ss::future<std::optional<storage::timequery_result>>
          timequery(storage::timequery_config) = 0;
        virtual ss::future<std::vector<cluster::rm_stm::tx_range>>
          aborted_transactions(
            model::offset,
            model::offset,
            ss::lw_shared_ptr<const storage::offset_translator_state>)
          = 0;
        virtual ss::future<error_code> validate_fetch_offset(
          model::offset, bool, model::timeout_clock::time_point)
          = 0;

        virtual ss::future<result<model::offset>>
          replicate(model::record_batch_reader, raft::replicate_options) = 0;

        virtual raft::replicate_stages replicate(
          model::batch_identity,
          model::record_batch_reader&&,
          raft::replicate_options)
          = 0;

        virtual result<partition_info> get_partition_info() const = 0;
        virtual cluster::partition_probe& probe() = 0;
        virtual ~impl() noexcept = default;
    };

    explicit partition_proxy(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    ss::future<result<model::offset, error_code>> sync_effective_start(
      model::timeout_clock::duration timeout = std::chrono::seconds(5)) {
        return _impl->sync_effective_start(timeout);
    }

    model::offset start_offset() const { return _impl->start_offset(); }

    model::offset high_watermark() const { return _impl->high_watermark(); }

    checked<model::offset, error_code> last_stable_offset() const {
        return _impl->last_stable_offset();
    }

    ss::future<std::error_code> linearizable_barrier() {
        return _impl->linearizable_barrier();
    }

    ss::future<error_code>
    prefix_truncate(model::offset o, ss::lowres_clock::time_point deadline) {
        return _impl->prefix_truncate(o, deadline);
    }

    bool is_elected_leader() const { return _impl->is_elected_leader(); }

    bool is_leader() const { return _impl->is_leader(); }

    const model::ntp& ntp() const { return _impl->ntp(); }

    ss::future<std::vector<cluster::rm_stm::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
        return _impl->aborted_transactions(base, last, std::move(ot_state));
    }

    ss::future<storage::translating_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point> deadline = std::nullopt) {
        return _impl->make_reader(cfg, deadline);
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) {
        return _impl->timequery(cfg);
    }

    cluster::partition_probe& probe() { return _impl->probe(); }

    kafka::leader_epoch leader_epoch() const { return _impl->leader_epoch(); }

    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch epoch) const {
        return _impl->get_leader_epoch_last_offset(epoch);
    }

    ss::future<error_code> validate_fetch_offset(
      model::offset o,
      bool is_follower,
      model::timeout_clock::time_point deadline) {
        return _impl->validate_fetch_offset(o, is_follower, deadline);
    }

    result<partition_info> get_partition_info() const {
        return _impl->get_partition_info();
    }

    ss::future<result<model::offset>> replicate(
      model::record_batch_reader r, raft::replicate_options opts) const {
        return _impl->replicate(std::move(r), opts);
    }

    raft::replicate_stages replicate(
      model::batch_identity bi,
      model::record_batch_reader&& r,
      raft::replicate_options opts) {
        return _impl->replicate(bi, std::move(r), opts);
    }

private:
    std::unique_ptr<impl> _impl;
};

std::optional<partition_proxy>
make_partition_proxy(const model::ktp&, cluster::partition_manager&);
std::optional<partition_proxy>
make_partition_proxy(const model::ntp&, cluster::partition_manager&);

} // namespace kafka
