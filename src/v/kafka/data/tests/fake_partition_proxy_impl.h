/*
 * Copyright 2026 Redpanda Data, Inc.
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

#include <stdexcept>

namespace tests {

/// Test double for kafka::partition_proxy::impl.
///
/// Returns canned values for the methods commonly read by handler logic
/// (ntp, local_size_bytes, offset_lag, cloud_size_bytes); all other
/// partition_proxy::impl methods throw if called, which surfaces
/// accidental coupling if a function under test grows a new dependency.
/// Subclass and selectively override more methods if a test needs to
/// stub additional behavior.
class fake_partition_proxy_impl : public kafka::partition_proxy::impl {
public:
    fake_partition_proxy_impl(
      model::ntp ntp,
      size_t local_size,
      model::offset offset_lag,
      std::optional<size_t> cloud_size)
      : _ntp(std::move(ntp))
      , _local_size(local_size)
      , _offset_lag(offset_lag)
      , _cloud_size(cloud_size) {}

    const model::ntp& ntp() const override { return _ntp; }
    size_t local_size_bytes() const override { return _local_size; }
    model::offset offset_lag() const override { return _offset_lag; }
    ss::future<std::optional<size_t>> cloud_size_bytes() const override {
        return ss::make_ready_future<std::optional<size_t>>(_cloud_size);
    }

    ss::future<result<model::offset, kafka::error_code>>
    sync_effective_start(model::timeout_clock::duration) override {
        unexpected();
    }
    model::offset local_start_offset() const override { unexpected(); }
    model::offset start_offset() const override { unexpected(); }
    model::offset high_watermark() const override { unexpected(); }
    checked<model::offset, kafka::error_code>
    last_stable_offset() const override {
        unexpected();
    }
    kafka::leader_epoch leader_epoch() const override { unexpected(); }
    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch) const override {
        unexpected();
    }
    bool is_leader() const override { unexpected(); }
    ss::future<std::error_code> linearizable_barrier() override {
        unexpected();
    }
    ss::future<kafka::error_code>
    prefix_truncate(model::offset, ss::lowres_clock::time_point) override {
        unexpected();
    }
    ss::future<storage::translating_reader>
    make_reader(kafka::log_reader_config) override {
        unexpected();
    }
    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config) override {
        unexpected();
    }
    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset,
      model::offset,
      ss::lw_shared_ptr<const storage::offset_translator_state>) override {
        unexpected();
    }
    ss::future<kafka::error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) override {
        unexpected();
    }
    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch>, raft::replicate_options) override {
        unexpected();
    }
    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch,
      raft::replicate_options) override {
        unexpected();
    }
    std::unique_ptr<kafka::exact_offset_replicator>
      make_exact_offset_replicator() && override {
        unexpected();
    }
    result<kafka::partition_info> get_partition_info() const override {
        unexpected();
    }
    size_t estimate_size_between(kafka::offset, kafka::offset) const override {
        unexpected();
    }
    cluster::partition_probe& probe() override { unexpected(); }
    ss::future<cluster::partition_cloud_storage_status>
    get_cloud_storage_status() const override {
        unexpected();
    }

private:
    [[noreturn]] static void unexpected() {
        throw std::runtime_error(
          "fake_partition_proxy_impl: unexpected partition_proxy method "
          "called");
    }

    model::ntp _ntp;
    size_t _local_size;
    model::offset _offset_lag;
    std::optional<size_t> _cloud_size;
};

} // namespace tests
