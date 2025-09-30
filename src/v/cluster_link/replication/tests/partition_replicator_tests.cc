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
#include "model/tests/random_batch.h"
#include "ssx/future-util.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"
#include "utils/mutex.h"

#include <seastar/core/sleep.hh>

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
        _data = {};
        return ss::now();
    }

    int num_resets() const { return _num_resets; }
    int num_fetches() const { return _num_fetches; }

    ss::future<data_source::data> fetch_next(ss::abort_source& as) final {
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

private:
    std::optional<kafka::offset> _start_offset{};
    kafka::offset _next_to_consume;
    int _num_fetches = 0;
    int _num_resets = 0;
    data _data;
    ss::gate _gate;
    ssx::semaphore _max_memory{5_MiB, "test_data_source"};
};

class test_data_sink : public data_sink {
public:
    ss::future<> start() final { return ss::now(); }
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
        co_return result;
    }

    raft::replicate_stages replicate(
      chunked_vector<model::record_batch> batches,
      model::timeout_clock::duration duration,
      ss::abort_source&) final {
        if (_fail_replication) {
            if (tests::random_bool()) {
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
          with_timeout(replicate_success(
            model::offset_cast(batches.back().last_offset())))};
    }

    void notify_replicator_failure(model::term_id) final {}

    ss::future<mutex::units> block_replication() {
        co_return co_await _replication_mu.get_units();
    }

    void set_fail_replication(bool value) { _fail_replication = value; }

private:
    model::ntp _ntp{"kafka", "test", model::partition_id(0)};
    mutex _replication_mu{"test_replication"};
    bool _fail_replication = false;
    kafka::offset _last_replicated_offset;
};

class PartitionReplicatorFixture : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        auto source = std::make_unique<test_data_source>();
        auto sink = std::make_unique<test_data_sink>();
        _source = source.get();
        _sink = sink.get();
        _replicator = std::make_unique<partition_replicator>(
          _ntp, model::term_id(0), std::move(source), std::move(sink));
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
      co_await tests::cooperative_spin_wait_with_timeout(
        3s, [&] { return _source->num_fetches() > num_fetches; }),
      ss::timed_out_error);

    // unblock replication and wait for memory to be freed up.
    units.return_all();
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      3s, [&] { return _source->available_memory() == 5_MiB; });
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      3s, [&] { return _source->num_fetches() > num_fetches; });
}

} // namespace cluster_link::replication
