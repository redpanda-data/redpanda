/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/link_replication_mgr.h"
#include "cluster_link/replication/types.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

using namespace std::chrono_literals;

namespace {
std::chrono::milliseconds sleep_for() {
    return std::chrono::milliseconds{random_generators::get_int(5, 15)};
}
} // namespace

namespace cluster_link::replication {

class test_config_provider : public link_configuration_provider {
public:
    ss::future<kafka::offset>
    start_offset(const ::model::ntp&, ss::abort_source&) override {
        co_return kafka::offset{0};
    }
};

class test_data_source : public data_source {
public:
    ss::future<> reset(kafka::offset) override { return ss::now(); }
    ss::future<> start(kafka::offset) override {
        if (tests::random_bool()) {
            return ss::make_exception_future<>(
              std::runtime_error("Simulated start failure"));
        }
        return ss::make_ready_future<>();
    }
    ss::future<> stop() noexcept override { return _gate.close(); }
    ss::future<fetch_data> fetch_next(ss::abort_source& as) override {
        auto holder = _gate.hold();
        co_await ss::sleep_abortable(sleep_for(), as);
        auto batch = model::test::make_random_batch(
          model::offset{0}, 5, true, model::record_batch_type::raft_data);
        chunked_vector<model::record_batch> batches;
        batches.push_back(std::move(batch));
        co_return fetch_data{std::move(batches), ssx::semaphore_units{}};
    }
    std::optional<data_source::source_partition_offsets_report>
    get_offsets() final {
        return std::nullopt;
    }

private:
    ss::gate _gate;
};

class test_data_sink : public data_sink {
public:
    ss::future<> start() override { return ss::make_ready_future<>(); }
    ss::future<> reset() override { return ss::make_ready_future<>(); }
    ss::future<> stop() noexcept override { return _gate.close(); }

    kafka::offset last_replicated_offset() const final {
        return kafka::offset{0};
    }

    raft::replicate_stages replicate(
      chunked_vector<model::record_batch>,
      model::timeout_clock::duration,
      ss::abort_source&) override {
        auto holder = _gate.hold();
        if (tests::random_bool()) {
            if (tests::random_bool()) {
                return {
                  ss::now(),
                  ssx::now<result<raft::replicate_result>>(
                    raft::errc::not_leader)};
            }
            throw std::runtime_error("Simulated replication failure");
        }
        auto result_f = ss::sleep(sleep_for()).then([] {
            return ssx::now(
              result<raft::replicate_result>(raft::replicate_result{}));
        });
        return {ss::sleep(sleep_for()), std::move(result_f)};
    };

    void notify_replicator_failure(model::term_id) final {}

    kafka::offset high_watermark() const final { return {}; }

    bool can_prefix_truncate() const final { return true; }

    ss::future<kafka::error_code>
    prefix_truncate(kafka::offset, ss::lowres_clock::time_point) final {
        co_return kafka::error_code::none;
    }

    kafka::offset start_offset() final { return {}; }

    ss::future<> maybe_sync_pid() final { return ss::now(); }

private:
    ss::gate _gate;
};

class test_data_source_factory : public data_source_factory {
    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() noexcept override { return ss::now(); }
    std::unique_ptr<data_source> make_source(const model::ntp&) override {
        return std::make_unique<test_data_source>();
    }
};

class test_data_sink_factory : public data_sink_factory {
    std::unique_ptr<data_sink> make_sink(const model::ntp&) override {
        return std::make_unique<test_data_sink>();
    }
};

// Mimics local_partition_data_sink_factory when the partition is no longer
// resident on this shard: make_sink throws while throw_on_make is set (e.g.
// leadership moved away between the residency check at start_replicator() time
// and reconcile_ntp_once() actually running), and succeeds once it is cleared.
class toggle_data_sink_factory : public data_sink_factory {
public:
    std::unique_ptr<data_sink> make_sink(const model::ntp& ntp) override {
        ++make_sink_calls;
        if (throw_on_make) {
            throw std::runtime_error(
              fmt::format("Partition not found: {} on this shard", ntp));
        }
        return std::make_unique<test_data_sink>();
    }
    bool throw_on_make{true};
    size_t make_sink_calls{0};
};

class LinkReplicationMgrFixture : public seastar_test {
    ss::future<> SetUpAsync() override {
        _mgr = std::make_unique<link_replication_manager>(
          ss::default_scheduling_group(),
          std::make_unique<test_config_provider>(),
          std::make_unique<test_data_source_factory>(),
          std::make_unique<test_data_sink_factory>());
        co_await _mgr->start();
    }

    ss::future<> TearDownAsync() override {
        if (_mgr) {
            co_await _mgr->stop();
        }
    }

protected:
    std::unique_ptr<link_replication_manager> _mgr;
};

TEST_F_CORO(LinkReplicationMgrFixture, TestFuzzStartStop) {
    struct ntp_term {
        model::ntp ntp;
        model::term_id term;
    };
    std::vector<ntp_term> added;
    int64_t term = 0;
    int32_t partition_id = 0;
    auto add_one = [this, &added, &term, &partition_id]() {
        auto ntp = model::ntp(
          "kafka", "test", model::partition_id(partition_id++));
        auto term_id = model::term_id(term++);
        _mgr->start_replicator(ntp, term_id);
        added.push_back({std::move(ntp), term_id});
        return ss::sleep(sleep_for());
    };

    auto remove_one = [this, &added]() {
        if (added.empty()) {
            return ss::sleep(sleep_for());
        }
        auto idx = random_generators::get_int<size_t>(added.size() - 1);
        auto to_remove = added[idx];
        added.erase(added.begin() + idx);
        _mgr->stop_replicator(std::move(to_remove.ntp), to_remove.term);
        return ss::sleep(sleep_for());
    };

    auto end_time = ss::lowres_clock::now() + 10s;
    auto add_f = ss::do_until(
      [&] { return end_time < ss::lowres_clock::now(); },
      [&]() { return add_one(); });
    auto remove_f = ss::do_until(
      [&] { return end_time < ss::lowres_clock::now(); },
      [&]() { return remove_one(); });

    co_await ss::when_all_succeed(std::move(add_f), std::move(remove_f));
}

TEST_F_CORO(LinkReplicationMgrFixture, TestStartStopBackToBack) {
    model::term_id term{0};
    // set to term if replicator is started
    std::optional<model::term_id> started_term{};
    model::ntp ntp{"kafka", "partition", model::partition_id{0}};

    auto maybe_start_replicator = [this, &started_term, &ntp, &term]() {
        if (!started_term) {
            started_term = term++;
            _mgr->start_replicator(ntp, started_term.value());
        }
        return tests::random_bool() ? ss::now() : ss::sleep(sleep_for());
    };

    auto maybe_stop_replicator = [this, &started_term, &ntp]() {
        if (started_term) {
            _mgr->stop_replicator(
              ntp, std::exchange(started_term, std::nullopt));
        }
        return tests::random_bool() ? ss::now() : ss::sleep(sleep_for());
    };

    auto end_time = ss::lowres_clock::now() + 10s;
    auto start_f = ss::do_until(
      [&] { return end_time < ss::lowres_clock::now(); },
      [&]() { return maybe_start_replicator(); });
    auto stop_f = ss::do_until(
      [&] { return end_time < ss::lowres_clock::now(); },
      [&]() { return maybe_stop_replicator(); });

    co_await ss::when_all_succeed(std::move(start_f), std::move(stop_f));
};

// Reproduces CORE-16742: when the partition is no longer resident on this
// shard by the time reconcile_ntp_once() runs, make_sink() throws. Previously
// the throw escaped reconcile_ntp_once() (only replicator->start() was wrapped
// in a try/catch, not the factory calls) and was dropped by spawn_with_gate,
// leaking an exceptional future to the seastar reactor:
//   "Exceptional future ignored: std::runtime_error (Partition not found ...)"
// It also left the ntp stuck with in_progress set, so it could never be
// reconciled again. Both are asserted below.
TEST_F_CORO(seastar_test, PartitionNotResidentIsHandled) {
    auto sink_factory = std::make_unique<toggle_data_sink_factory>();
    auto* sink_factory_ptr = sink_factory.get();
    auto mgr = std::make_unique<link_replication_manager>(
      ss::default_scheduling_group(),
      std::make_unique<test_config_provider>(),
      std::make_unique<test_data_source_factory>(),
      std::move(sink_factory));
    co_await mgr->start();

    model::ntp ntp{"kafka", "cloud-compacted-topic", model::partition_id{0}};

    // make_sink() throws: no replicator starts, and no future is leaked
    // (enforced by --fail-on-abandoned-failed-futures). Wait for the throwing
    // path to actually run so the test still checks it if reconciliation is
    // slow.
    mgr->start_replicator(ntp, model::term_id{1});
    co_await tests::cooperative_spin_wait_with_timeout(
      5s, [&] { return sink_factory_ptr->make_sink_calls >= 1; });
    ASSERT_TRUE_CORO(mgr->get_partition_offsets_report().empty());

    // Leadership returns: the ntp must not be permanently stuck with
    // in_progress set — a fresh start action must reconcile successfully.
    sink_factory_ptr->throw_on_make = false;
    mgr->start_replicator(ntp, model::term_id{2});
    co_await tests::cooperative_spin_wait_with_timeout(
      5s, [&] { return mgr->get_partition_offsets_report().size() == 1; });

    co_await mgr->stop();
}

} // namespace cluster_link::replication
