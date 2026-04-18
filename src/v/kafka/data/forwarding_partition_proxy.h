/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/data/partition_proxy.h"

#include <memory>

namespace kafka {

/// A forwarding base class for partition_proxy::impl that delegates every
/// virtual method to an inner implementation. Decorator subclasses inherit
/// from this and override only the methods they intercept. If new methods
/// are added to partition_proxy::impl, the compiler forces an update here
/// because the pure virtuals remain unimplemented.
class forwarding_partition_proxy_impl : public partition_proxy::impl {
public:
    explicit forwarding_partition_proxy_impl(
      std::unique_ptr<partition_proxy::impl> inner)
      : _inner(std::move(inner)) {}

    const model::ntp& ntp() const override { return _inner->ntp(); }

    ss::future<result<model::offset, error_code>>
    sync_effective_start(model::timeout_clock::duration timeout) override {
        return _inner->sync_effective_start(timeout);
    }

    model::offset local_start_offset() const override {
        return _inner->local_start_offset();
    }

    model::offset start_offset() const override {
        return _inner->start_offset();
    }

    model::offset high_watermark() const override {
        return _inner->high_watermark();
    }

    checked<model::offset, error_code> last_stable_offset() const override {
        return _inner->last_stable_offset();
    }

    kafka::leader_epoch leader_epoch() const override {
        return _inner->leader_epoch();
    }

    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch epoch) const override {
        return _inner->get_leader_epoch_last_offset(epoch);
    }

    bool is_leader() const override { return _inner->is_leader(); }

    ss::future<std::error_code> linearizable_barrier() override {
        return _inner->linearizable_barrier();
    }

    ss::future<error_code> prefix_truncate(
      model::offset o, ss::lowres_clock::time_point deadline) override {
        return _inner->prefix_truncate(o, deadline);
    }

    ss::future<storage::translating_reader>
    make_reader(kafka::log_reader_config cfg) override {
        return _inner->make_reader(cfg);
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) override {
        return _inner->timequery(cfg);
    }

    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state> ot_state)
      override {
        return _inner->aborted_transactions(base, last, std::move(ot_state));
    }

    ss::future<error_code> validate_fetch_offset(
      model::offset o,
      bool is_follower,
      model::timeout_clock::time_point deadline) override {
        return _inner->validate_fetch_offset(o, is_follower, deadline);
    }

    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch> batches,
      raft::replicate_options opts) override {
        return _inner->replicate(std::move(batches), opts);
    }

    raft::replicate_stages replicate(
      model::batch_identity bi,
      model::record_batch batch,
      raft::replicate_options opts) override {
        return _inner->replicate(bi, std::move(batch), opts);
    }

    std::unique_ptr<exact_offset_replicator> make_exact_offset_replicator()
      && override {
        return std::move(*_inner).make_exact_offset_replicator();
    }

    result<partition_info> get_partition_info() const override {
        return _inner->get_partition_info();
    }

    size_t estimate_size_between(
      kafka::offset begin, kafka::offset end) const override {
        return _inner->estimate_size_between(begin, end);
    }

    cluster::partition_probe& probe() override { return _inner->probe(); }

    size_t local_size_bytes() const override {
        return _inner->local_size_bytes();
    }

    ss::future<std::optional<size_t>> cloud_size_bytes() const override {
        return _inner->cloud_size_bytes();
    }

    model::offset offset_lag() const override { return _inner->offset_lag(); }

    ss::future<cluster::partition_cloud_storage_status>
    get_cloud_storage_status() const override {
        return _inner->get_cloud_storage_status();
    }

protected:
    std::unique_ptr<partition_proxy::impl> _inner;
};

} // namespace kafka
