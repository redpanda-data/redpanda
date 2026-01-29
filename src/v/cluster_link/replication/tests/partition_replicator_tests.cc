/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/units.h"
#include "cluster_link/replication/partition_replicator.h"
#include "cluster_link/replication/tests/deps_test_impl.h"
#include "cluster_link/replication/types.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "ssx/future-util.h"
#include "ssx/mutex.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>

#include <system_error>

using namespace std::chrono_literals;

namespace cluster_link::replication {

class test_data_source : public data_source {
public:
    ss::future<> start(kafka::offset start_offset) final {
        _start_offset = start_offset;
        _next_to_consume = start_offset;
        return ss::now();
    }
    ss::future<> stop() noexcept final {
        _max_memory.broken();
        return _gate.close();
    }
    ss::future<> reset(kafka::offset start_offset) final {
        _start_offset = start_offset;
        _next_to_consume = start_offset;
        _num_resets++;
        _last_reset_offset = start_offset;
        _data = {};
        return ss::now();
    }

    int num_resets() const { return _num_resets; }
    int num_fetches() const { return _num_fetches; }
    kafka::offset last_reset_offset() const { return _last_reset_offset; }

    ss::future<fetch_data> fetch_next(ss::abort_source& as) final {
        _num_fetches++;
        auto holder = _gate.hold();
        while (_data.batches.empty()) {
            co_await ss::sleep_abortable(10ms, as);
        }
        co_return std::exchange(_data, {});
    }

    ss::future<> push_data() {
        // until atleast 1 unit is available
        co_await _max_memory.wait();
        _max_memory.signal();
        auto offset = kafka::offset_cast(_next_to_consume);
        auto batch = model::test::make_random_batch(
          offset, 10, true, model::record_batch_type::raft_data);
        auto size = static_cast<size_t>(batch.size_bytes());
        auto units = co_await ss::get_units(
          _max_memory, std::min(size, _max_memory.current()));
        _next_to_consume = kafka::next_offset(
          model::offset_cast(batch.last_offset()));
        _data.batches.push_back(std::move(batch));
        if (_data.units.count()) {
            _data.units.adopt(std::move(units));
        } else {
            _data.units = std::move(units);
        }
    }

    size_t available_memory() const { return _max_memory.current(); }

    std::optional<data_source::source_partition_offsets_report>
    get_offsets() final {
        source_partition_offsets_report ret;
        ret.source_start_offset = _start_offset.value_or(kafka::offset{-1});
        ret.source_hwm = _next_to_consume;
        ret.source_lso = _next_to_consume;
        return ret;
    }

private:
    std::optional<kafka::offset> _start_offset{};
    kafka::offset _next_to_consume;
    kafka::offset _last_reset_offset;
    int _num_fetches = 0;
    int _num_resets = 0;
    fetch_data _data;
    ss::gate _gate;
    ssx::semaphore _max_memory{5_MiB, "test_data_source"};
};

class test_data_sink : public data_sink {
public:
    ss::future<> start() final { return ss::now(); }
    ss::future<> reset() final {
        // Simulate re-syncing to the actual committed offset, similar to
        // the real sink which queries the STM for the expected last offset.
        _last_replicated_offset = _committed_offset;
        return ss::now();
    }
    ss::future<> stop() noexcept final { return ss::now(); }
    kafka::offset last_replicated_offset() const final {
        return _last_replicated_offset;
    }

    ss::future<result<raft::replicate_result>>
    replicate_success(kafka::offset offset) {
        co_await ss::sleep(1ms);
        raft::replicate_result result;
        result.last_offset = kafka::offset_cast(offset);
        result.last_term = model::term_id(0);
        _last_replicated_offset = offset;
        _committed_offset = offset;
        co_return result;
    }

    raft::replicate_stages replicate(
      chunked_vector<model::record_batch> batches,
      model::timeout_clock::duration duration,
      ss::abort_source&) final {
        auto batch_last_offset = model::offset_cast(
          batches.back().last_offset());
        if (_fail_replication) {
            // Simulate eager offset update before replication completes.
            // This is the bug scenario: the offset is updated, but the
            // replication will fail. Without reset(), the stale offset
            // would cause the source to be reset to the wrong position.
            _last_replicated_offset = batch_last_offset;
            if (::tests::random_bool()) {
                throw std::runtime_error("Simulated replication failure");
            }
            result<raft::replicate_result> err{raft::errc::not_leader};
            return raft::replicate_stages{ss::now(), ssx::now(err)};
        }
        auto with_timeout = [duration](auto&& f) {
            return ss::with_timeout(
              model::timeout_clock::now() + duration,
              std::forward<decltype(f)>(f));
        };
        return raft::replicate_stages{
          with_timeout(_replication_mu.get_units().discard_result()),
          with_timeout(replicate_success(batch_last_offset))};
    }

    void notify_replicator_failure(model::term_id) final {}

    ss::future<ssx::mutex::units> block_replication() {
        co_return co_await _replication_mu.get_units();
    }

    void set_fail_replication(bool value) { _fail_replication = value; }

    kafka::offset high_watermark() const final {
        return kafka::next_offset(_last_replicated_offset);
    }

    bool can_prefix_truncate() const final { return true; }

    ss::future<kafka::error_code> prefix_truncate(
      kafka::offset truncation_offset, ss::lowres_clock::time_point) final {
        if (truncation_offset <= start_offset()) {
            // No-op, return early
            co_return kafka::error_code::none;
        }

        if (truncation_offset > high_watermark()) {
            co_return kafka::error_code::offset_out_of_range;
        }

        _start_offset = truncation_offset;

        co_return kafka::error_code::none;
    }

    kafka::offset start_offset() final { return _start_offset; }

    ss::future<> maybe_sync_pid() final { return ss::now(); }

    kafka::offset committed_offset() const { return _committed_offset; }

private:
    model::ntp _ntp{"kafka", "test", model::partition_id(0)};
    ssx::mutex _replication_mu{"test_replication"};
    bool _fail_replication = false;
    kafka::offset _start_offset{0};
    kafka::offset _last_replicated_offset;
    // Tracks the actual committed offset (successful replications only).
    kafka::offset _committed_offset;
};

class PartitionReplicatorFixture : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        auto source = std::make_unique<test_data_source>();
        auto sink = std::make_unique<test_data_sink>();
        _config_provider = std::make_unique<tests::test_config_provider>();
        _source = source.get();
        _sink = sink.get();
        _replicator = std::make_unique<partition_replicator>(
          _ntp,
          model::term_id(0),
          *_config_provider,
          std::move(source),
          std::move(sink));
        return _replicator->start();
    }

    ss::future<> TearDownAsync() override {
        if (_replicator) {
            co_await _replicator->stop();
        }
    }

    ss::future<> push_data() { co_await _source->push_data(); }

protected:
    test_data_source* _source;
    test_data_sink* _sink;
    std::unique_ptr<partition_replicator> _replicator;
    model::ntp _ntp{"kafka", "test", 0};

private:
    std::unique_ptr<link_configuration_provider> _config_provider;
};

TEST_F_CORO(PartitionReplicatorFixture, TestHappyPath) {
    // push some data into the source and wait for it be replicated
    for (int i = 0; i < 10; ++i) {
        co_await push_data();
    }
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [&] { return _sink->last_replicated_offset() == kafka::offset(99); });
}

TEST_F_CORO(PartitionReplicatorFixture, TestResetOnFailure) {
    for (int i = 0; i < 10; ++i) {
        co_await push_data();
    }
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [&] { return _sink->last_replicated_offset() == kafka::offset(99); });

    // initial state, no resets yet
    ASSERT_EQ_CORO(_source->num_resets(), 0);

    // now fail the replication
    _sink->set_fail_replication(true);
    co_await push_data();
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [&] {
        return _sink->last_replicated_offset() == kafka::offset(99)
               && _source->num_resets() == 1;
    });

    _sink->set_fail_replication(false);
    co_await push_data();
    // ensure replication is unblocked
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [&] {
        return _sink->last_replicated_offset() == kafka::offset(109)
               && _source->num_resets() == 1;
    });
}

TEST_F_CORO(PartitionReplicatorFixture, TestMemoryExhaustion) {
    auto units = co_await _sink->block_replication();
    while (_source->available_memory() > 0) {
        co_await push_data();
    }
    // fetches should be blocked at this point
    auto num_fetches = _source->num_fetches();
    ASSERT_THROW_CORO(
      co_await ::tests::cooperative_spin_wait_with_timeout(
        3s, [&] { return _source->num_fetches() > num_fetches; }),
      ss::timed_out_error);

    // unblock replication and wait for memory to be freed up.
    units.return_all();
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      3s, [&] { return _source->available_memory() == 5_MiB; });
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      3s, [&] { return _source->num_fetches() > num_fetches; });
}

TEST_F_CORO(PartitionReplicatorFixture, ShadowPartitionLag) {
    co_await push_data();
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [&] { return _sink->last_replicated_offset() == kafka::offset(9); });

    auto units = co_await _sink->block_replication();
    co_await push_data();
    RPTEST_REQUIRE_EQ_CORO(_replicator->get_partition_lag(), kafka::offset(10));
    co_await push_data();
    RPTEST_REQUIRE_EQ_CORO(_replicator->get_partition_lag(), kafka::offset(20));

    units.return_all();
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      3s, [&] { return _replicator->get_partition_lag() == kafka::offset{0}; });
}

// Test that ensures offset monotonicity is preserved when replication failures
// occur. Specifically, this tests the fix where the sink is reset before
// querying last_replicated_offset to determine the source reset position.
// Without the sink reset, the source could be reset to a stale (too high)
// offset, causing data to be skipped.
TEST_F_CORO(PartitionReplicatorFixture, TestOffsetMonotonicityOnFailure) {
    // Step 1: Replicate some data successfully
    for (int i = 0; i < 10; ++i) {
        co_await push_data();
    }
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [&] { return _sink->last_replicated_offset() == kafka::offset(99); });

    // Verify committed offset matches
    ASSERT_EQ_CORO(_sink->committed_offset(), kafka::offset(99));
    ASSERT_EQ_CORO(_source->num_resets(), 0);

    // Step 2: Enable failure mode. The sink will eagerly update
    // last_replicated_offset when replicate() is called, but the replication
    // will fail. This simulates the bug scenario where the sink has a stale
    // (too high) offset.
    _sink->set_fail_replication(true);
    co_await push_data();

    // Wait for the reset to happen
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [&] { return _source->num_resets() == 1; });

    // Step 3: Verify offset monotonicity - the source should be reset to
    // next_offset(committed_offset) = 100, NOT next_offset(stale_offset) = 110.
    // This is the key assertion: the sink's reset() was called before
    // determining the source reset position, so the correct offset is used.
    auto expected_reset_offset = kafka::next_offset(_sink->committed_offset());
    ASSERT_EQ_CORO(_source->last_reset_offset(), expected_reset_offset);
    ASSERT_EQ_CORO(_source->last_reset_offset(), kafka::offset(100));

    // Step 4: Disable failure mode and verify replication continues correctly
    _sink->set_fail_replication(false);
    co_await push_data();

    // Replication should continue from offset 100 and complete successfully
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [&] {
        return _sink->last_replicated_offset() == kafka::offset(109)
               && _source->num_resets() == 1;
    });
}

} // namespace cluster_link::replication
