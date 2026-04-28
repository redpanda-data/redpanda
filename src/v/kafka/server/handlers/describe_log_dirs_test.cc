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
#include "kafka/data/partition_proxy.h"
#include "kafka/server/handlers/describe_log_dirs.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace {

// Fake that returns canned values for the four methods describe_partition
// reads. All other partition_proxy::impl methods throw if called, which both
// documents what describe_partition depends on and surfaces accidental
// coupling if the dependency set grows.
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
          "describe_partition called an unexpected partition_proxy method");
    }

    model::ntp _ntp;
    size_t _local_size;
    model::offset _offset_lag;
    std::optional<size_t> _cloud_size;
};

kafka::partition_proxy make_proxy(
  model::ntp ntp,
  size_t local_size,
  model::offset offset_lag,
  std::optional<size_t> cloud_size) {
    return kafka::partition_proxy(
      std::make_unique<fake_partition_proxy_impl>(
        std::move(ntp), local_size, offset_lag, cloud_size));
}

model::ntp make_ntp(int32_t partition) {
    return model::ntp(
      model::ns("kafka"),
      model::topic("test-topic"),
      model::partition_id(partition));
}

} // namespace

using kafka::describe_log_dirs::detail::describe_partition;

TEST_CORO(DescribePartition, LocalOnlyNoCloudData) {
    auto proxy = make_proxy(make_ntp(7), 1024, model::offset(3), std::nullopt);
    auto data = co_await describe_partition(proxy, /*include_remote=*/true);

    EXPECT_EQ(data.local.partition_index, 7);
    EXPECT_EQ(data.local.partition_size, 1024);
    EXPECT_EQ(data.local.offset_lag, 3);
    EXPECT_FALSE(data.local.is_future_key);
    EXPECT_FALSE(data.remote.has_value());
}

TEST_CORO(DescribePartition, CloudDataButRemoteDisabled) {
    auto proxy = make_proxy(
      make_ntp(0), 100, model::offset(0), std::optional<size_t>(2048));
    auto data = co_await describe_partition(proxy, /*include_remote=*/false);

    EXPECT_EQ(data.local.partition_size, 100);
    EXPECT_FALSE(data.remote.has_value());
}

TEST_CORO(DescribePartition, CloudDataAndRemoteEnabled) {
    auto proxy = make_proxy(
      make_ntp(2), 100, model::offset(5), std::optional<size_t>(2048));
    auto data = co_await describe_partition(proxy, /*include_remote=*/true);

    EXPECT_EQ(data.local.partition_index, 2);
    EXPECT_EQ(data.local.partition_size, 100);
    EXPECT_EQ(data.local.offset_lag, 5);

    ASSERT_TRUE_CORO(data.remote.has_value());
    EXPECT_EQ(data.remote->partition_index, 2);
    EXPECT_EQ(data.remote->partition_size, 2048);
    EXPECT_EQ(data.remote->offset_lag, 5);
    EXPECT_FALSE(data.remote->is_future_key);
}

TEST_CORO(DescribePartition, NegativeOffsetLagClampsAtZero) {
    auto proxy = make_proxy(make_ntp(0), 0, model::offset(-7), std::nullopt);
    auto data = co_await describe_partition(proxy, /*include_remote=*/false);

    EXPECT_EQ(data.local.offset_lag, 0);
}
