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
#include "cluster/partition.h"
#include "cluster/partition_probe.h"
#include "kafka/data/exact_offset_replicator.h"
#include "kafka/data/migrated_partition.h"
#include "kafka/data/partition_proxy.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>

#include <memory>
#include <optional>

// migrated_partition is almost entirely a router/delegator over two child
// partition_proxy::impl backends (the tiered-storage path `_ts` for offsets at
// or below the migration boundary, and the cloud-topics path `_ct` for
// everything above it, plus all writes and metadata). These tests drive it over
// fake children that tag every return value with a per-child id, so each method
// can assert *which* child answered (and, for the transformed methods, that the
// arguments were adjusted correctly). The one method that cannot be covered
// here is the cloud-abort-translation body of aborted_transactions for the TS
// range, which reads cluster::partition::aborted_transactions_cloud and so
// needs a real partition (covered end-to-end by the ducktape transactions
// test).

namespace kafka {
namespace {

constexpr int ts_id = 1;
constexpr int ct_id = 2;

// A sentinel exact_offset_replicator carrying the id of the child that produced
// it, so make_exact_offset_replicator delegation can be asserted.
class fake_eor final : public exact_offset_replicator {
public:
    explicit fake_eor(int id)
      : id(id) {}
    int id;

    raft::replicate_stages replicate(
      chunked_vector<model::record_batch>,
      chunked_vector<kafka::offset>,
      std::optional<kafka::offset>,
      model::timeout_clock::duration,
      std::optional<std::reference_wrapper<ss::abort_source>>) final {
        return raft::replicate_stages{raft::errc::success};
    }
    ss::future<result<kafka::offset>> get_last_offset(
      model::timeout_clock::duration,
      std::optional<std::reference_wrapper<ss::abort_source>>) final {
        return ss::make_ready_future<result<kafka::offset>>(kafka::offset{});
    }
    ss::future<std::error_code> ensure_truncatable(
      kafka::offset,
      model::timeout_clock::duration,
      std::optional<std::reference_wrapper<ss::abort_source>>) final {
        return ss::make_ready_future<std::error_code>(std::error_code{});
    }
};

// partition_proxy::impl whose every return value encodes its id, plus call
// counters / captured args for the side-effecting methods.
class fake_impl final : public partition_proxy::impl {
public:
    explicit fake_impl(int id)
      : _id(id)
      , _ntp(
          model::kafka_namespace,
          model::topic(id == ts_id ? "ts" : "ct"),
          model::partition_id(0))
      , _probe(nullptr) {}

    // id-tagged value so ts (1) and ct (2) are always distinguishable.
    model::offset tag() const { return model::offset(_id); }

    // captured args / counters for side-effecting methods
    std::optional<kafka::log_reader_config> last_reader_cfg;
    std::optional<std::pair<model::offset, model::offset>> last_aborted;
    std::optional<model::offset> last_validated;
    std::optional<model::offset> last_prefix_truncate;
    int make_reader_calls{0};
    int aborted_calls{0};
    int validate_calls{0};
    int prefix_truncate_calls{0};
    int barrier_calls{0};
    int replicate_calls{0};
    int replicate_stages_calls{0};
    int timequery_calls{0};
    // overridable so the start_offset min() can be exercised in both
    // orientations; defaults to the id tag.
    model::offset start_offset_val{tag()};

    const model::ntp& ntp() const final { return _ntp; }

    ss::future<result<model::offset, error_code>>
    sync_effective_start(model::timeout_clock::duration) final {
        co_return tag();
    }
    model::offset local_start_offset() const final { return tag(); }
    model::offset start_offset() const final { return start_offset_val; }
    model::offset high_watermark() const final { return tag(); }
    checked<model::offset, error_code> last_stable_offset() const final {
        return tag();
    }
    kafka::leader_epoch leader_epoch() const final {
        return kafka::leader_epoch(_id);
    }
    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch) const final {
        co_return tag();
    }
    bool is_leader() const final { return _id == ct_id; }

    ss::future<std::error_code> linearizable_barrier() final {
        ++barrier_calls;
        co_return std::error_code{};
    }
    ss::future<error_code>
    prefix_truncate(model::offset o, ss::lowres_clock::time_point) final {
        ++prefix_truncate_calls;
        last_prefix_truncate = o;
        co_return error_code::none;
    }
    ss::future<storage::translating_reader>
    make_reader(kafka::log_reader_config cfg) final {
        ++make_reader_calls;
        last_reader_cfg = cfg;
        co_return storage::translating_reader{
          model::make_empty_record_batch_reader()};
    }
    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config) final {
        ++timequery_calls;
        co_return std::nullopt;
    }
    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final {
        ++aborted_calls;
        last_aborted = std::make_pair(base, last);
        co_return std::vector<model::tx_range>{};
    }
    ss::future<error_code> validate_fetch_offset(
      model::offset o, bool, model::timeout_clock::time_point) final {
        ++validate_calls;
        last_validated = o;
        co_return error_code::none;
    }
    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch>, raft::replicate_options) final {
        ++replicate_calls;
        co_return tag();
    }
    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch,
      raft::replicate_options) final {
        ++replicate_stages_calls;
        return raft::replicate_stages{raft::errc::success};
    }
    std::unique_ptr<exact_offset_replicator> make_exact_offset_replicator()
      && final {
        return std::make_unique<fake_eor>(_id);
    }
    result<partition_info> get_partition_info() const final {
        partition_info info;
        info.leader = model::node_id(_id);
        return info;
    }
    size_t estimate_size_between(kafka::offset, kafka::offset) const final {
        return static_cast<size_t>(_id);
    }
    cluster::partition_probe& probe() final { return _probe; }
    size_t local_size_bytes() const final { return static_cast<size_t>(_id); }
    ss::future<std::optional<size_t>> cloud_size_bytes() const final {
        co_return static_cast<size_t>(_id);
    }
    model::offset offset_lag() const final { return tag(); }
    ss::future<cluster::partition_cloud_storage_status>
    get_cloud_storage_status() const final {
        cluster::partition_cloud_storage_status status{};
        status.local_log_size_bytes = static_cast<size_t>(_id);
        co_return status;
    }

private:
    int _id;
    model::ntp _ntp;
    cluster::partition_probe _probe;
};

constexpr kafka::offset boundary{100};

// Build a migrated_partition over two fakes and hand back raw pointers for
// inspection after the impls are moved into the composite. A null
// cluster::partition is fine: it is only dereferenced by the TS-range
// cloud-abort path, which these unit tests do not drive.
migrated_partition make_migrated(fake_impl** ts_out, fake_impl** ct_out) {
    auto ts = std::make_unique<fake_impl>(ts_id);
    auto ct = std::make_unique<fake_impl>(ct_id);
    *ts_out = ts.get();
    *ct_out = ct.get();
    return migrated_partition{
      ss::lw_shared_ptr<cluster::partition>{},
      boundary,
      std::move(ts),
      std::move(ct)};
}

} // namespace

// ---- metadata / offset getters: all delegate to the CT child ----

TEST_CORO(migrated_partition_test, ntp_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    ASSERT_EQ_CORO(mp.ntp(), ct->ntp());
    ASSERT_TRUE_CORO(ct->ntp() != ts->ntp());
}

TEST_CORO(migrated_partition_test, sync_effective_start_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    auto r = co_await mp.sync_effective_start(std::chrono::seconds(1));
    ASSERT_TRUE_CORO(r.has_value());
    ASSERT_EQ_CORO(r.value(), ct->tag());
}

TEST_CORO(migrated_partition_test, local_start_offset_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    ASSERT_EQ_CORO(mp.local_start_offset(), ct->tag());
    co_return;
}

TEST_CORO(migrated_partition_test, start_offset_is_min_of_both) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    // Make ct the smaller of the two to prove it is min(), not "always _ts".
    ts->start_offset_val = model::offset{5};
    ct->start_offset_val = model::offset{2};
    ASSERT_EQ_CORO(mp.start_offset(), model::offset{2});
    // And the other orientation.
    ts->start_offset_val = model::offset{1};
    ct->start_offset_val = model::offset{9};
    ASSERT_EQ_CORO(mp.start_offset(), model::offset{1});
    co_return;
}

TEST_CORO(migrated_partition_test, high_watermark_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    ASSERT_EQ_CORO(mp.high_watermark(), ct->tag());
    co_return;
}

TEST_CORO(migrated_partition_test, last_stable_offset_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    auto r = mp.last_stable_offset();
    ASSERT_TRUE_CORO(r.has_value());
    ASSERT_EQ_CORO(r.value(), ct->tag());
    co_return;
}

TEST_CORO(migrated_partition_test, leader_epoch_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    ASSERT_EQ_CORO(mp.leader_epoch(), kafka::leader_epoch(ct_id));
    co_return;
}

TEST_CORO(migrated_partition_test, get_leader_epoch_last_offset_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    auto r = co_await mp.get_leader_epoch_last_offset(kafka::leader_epoch(1));
    ASSERT_TRUE_CORO(r.has_value());
    ASSERT_EQ_CORO(r.value(), ct->tag());
}

TEST_CORO(migrated_partition_test, is_leader_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    // ct returns true, ts returns false; the composite must reflect ct.
    ASSERT_TRUE_CORO(mp.is_leader());
    co_return;
}

TEST_CORO(migrated_partition_test, get_partition_info_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    auto r = mp.get_partition_info();
    ASSERT_TRUE_CORO(r.has_value());
    ASSERT_EQ_CORO(r.value().leader, model::node_id(ct_id));
    co_return;
}

TEST_CORO(migrated_partition_test, estimate_size_between_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    ASSERT_EQ_CORO(
      mp.estimate_size_between(kafka::offset{0}, kafka::offset{1}),
      static_cast<size_t>(ct_id));
    co_return;
}

TEST_CORO(migrated_partition_test, probe_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    // Identity, by address, against the ct child's probe.
    ASSERT_TRUE_CORO(&mp.probe() == &ct->probe());
    ASSERT_TRUE_CORO(&mp.probe() != &ts->probe());
    co_return;
}

TEST_CORO(migrated_partition_test, local_size_bytes_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    ASSERT_EQ_CORO(mp.local_size_bytes(), static_cast<size_t>(ct_id));
    co_return;
}

TEST_CORO(migrated_partition_test, cloud_size_bytes_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    auto r = co_await mp.cloud_size_bytes();
    ASSERT_TRUE_CORO(r.has_value());
    ASSERT_EQ_CORO(r.value(), static_cast<size_t>(ct_id));
}

TEST_CORO(migrated_partition_test, offset_lag_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    ASSERT_EQ_CORO(mp.offset_lag(), ct->tag());
    co_return;
}

TEST_CORO(migrated_partition_test, get_cloud_storage_status_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    auto status = co_await mp.get_cloud_storage_status();
    ASSERT_EQ_CORO(status.local_log_size_bytes, static_cast<size_t>(ct_id));
}

// ---- side-effecting methods that delegate to the CT child ----

TEST_CORO(migrated_partition_test, linearizable_barrier_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    co_await mp.linearizable_barrier();
    ASSERT_EQ_CORO(ct->barrier_calls, 1);
    ASSERT_EQ_CORO(ts->barrier_calls, 0);
}

TEST_CORO(migrated_partition_test, prefix_truncate_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    co_await mp.prefix_truncate(model::offset{42}, ss::lowres_clock::now());
    ASSERT_EQ_CORO(ct->prefix_truncate_calls, 1);
    ASSERT_EQ_CORO(ts->prefix_truncate_calls, 0);
    ASSERT_EQ_CORO(ct->last_prefix_truncate, model::offset{42});
}

TEST_CORO(migrated_partition_test, timequery_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    co_await mp.timequery(
      storage::timequery_config{
        model::offset{0},
        model::timestamp::now(),
        model::offset{100},
        model::record_batch_type::raft_data});
    ASSERT_EQ_CORO(ct->timequery_calls, 1);
    ASSERT_EQ_CORO(ts->timequery_calls, 0);
}

TEST_CORO(migrated_partition_test, replicate_vector_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    chunked_vector<model::record_batch> batches;
    [[maybe_unused]] auto res = co_await mp.replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});
    ASSERT_EQ_CORO(ct->replicate_calls, 1);
    ASSERT_EQ_CORO(ts->replicate_calls, 0);
}

TEST_CORO(migrated_partition_test, replicate_batch_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    storage::record_batch_builder bb(
      model::record_batch_type::raft_data, model::offset{0});
    bb.add_raw_kv(iobuf{}, iobuf{});
    auto stages = mp.replicate(
      model::batch_identity{},
      std::move(bb).build(),
      raft::replicate_options{raft::consistency_level::quorum_ack});
    co_await std::move(stages.replicate_finished).discard_result();
    ASSERT_EQ_CORO(ct->replicate_stages_calls, 1);
    ASSERT_EQ_CORO(ts->replicate_stages_calls, 0);
}

TEST_CORO(migrated_partition_test, exact_offset_replicator_delegates_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    auto eor = std::move(mp).make_exact_offset_replicator();
    ASSERT_TRUE_CORO(eor != nullptr);
    auto* fe = dynamic_cast<fake_eor*>(eor.get());
    ASSERT_TRUE_CORO(fe != nullptr);
    ASSERT_EQ_CORO(fe->id, ct_id);
}

// ---- offset-routed methods (the core of the composite) ----

TEST_CORO(migrated_partition_test, read_below_boundary_routes_to_ts) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);

    // start at 30 (<= boundary 100), max well above the boundary.
    kafka::log_reader_config cfg{kafka::offset{30}, kafka::offset{500}};
    co_await mp.make_reader(cfg);

    ASSERT_EQ_CORO(ts->make_reader_calls, 1);
    ASSERT_EQ_CORO(ct->make_reader_calls, 0);
    ASSERT_TRUE_CORO(ts->last_reader_cfg.has_value());
    ASSERT_EQ_CORO(ts->last_reader_cfg->start_offset, kafka::offset{30});
    // max_offset must be capped at the boundary.
    ASSERT_EQ_CORO(ts->last_reader_cfg->max_offset, boundary);
}

TEST_CORO(migrated_partition_test, read_at_boundary_routes_to_ts) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    kafka::log_reader_config cfg{boundary, kafka::offset{500}};
    co_await mp.make_reader(cfg);
    ASSERT_EQ_CORO(ts->make_reader_calls, 1);
    ASSERT_EQ_CORO(ct->make_reader_calls, 0);
}

TEST_CORO(migrated_partition_test, read_above_boundary_routes_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);
    kafka::log_reader_config cfg{kafka::offset{101}, kafka::offset{500}};
    co_await mp.make_reader(cfg);
    ASSERT_EQ_CORO(ct->make_reader_calls, 1);
    ASSERT_EQ_CORO(ts->make_reader_calls, 0);
    // No cap on the CT path.
    ASSERT_EQ_CORO(ct->last_reader_cfg->max_offset, kafka::offset{500});
}

TEST_CORO(migrated_partition_test, validate_fetch_offset_routed_by_boundary) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);

    co_await mp.validate_fetch_offset(
      model::offset{50}, false, model::no_timeout);
    ASSERT_EQ_CORO(ts->validate_calls, 1);
    ASSERT_EQ_CORO(ct->validate_calls, 0);

    co_await mp.validate_fetch_offset(
      model::offset{200}, false, model::no_timeout);
    ASSERT_EQ_CORO(ct->validate_calls, 1);
    ASSERT_EQ_CORO(ts->validate_calls, 1);
}

TEST_CORO(migrated_partition_test, aborted_transactions_above_boundary_to_ct) {
    fake_impl* ts = nullptr;
    fake_impl* ct = nullptr;
    auto mp = make_migrated(&ts, &ct);

    // base above boundary -> CT path, unchanged range.
    co_await mp.aborted_transactions(
      model::offset{200}, model::offset{500}, nullptr);
    ASSERT_EQ_CORO(ct->aborted_calls, 1);
    ASSERT_EQ_CORO(ts->aborted_calls, 0);
    ASSERT_EQ_CORO(ct->last_aborted->second, model::offset{500});

    // base at/below boundary with a null translator -> empty (L1 read, aborts
    // already filtered); touches neither child nor the (null) partition. The
    // TS-range cloud-abort translation is covered by the ducktape transactions
    // test (it needs a real cluster::partition).
    auto below = co_await mp.aborted_transactions(
      model::offset{10}, model::offset{50}, nullptr);
    ASSERT_TRUE_CORO(below.empty());
    ASSERT_EQ_CORO(ts->aborted_calls, 0);
    ASSERT_EQ_CORO(ct->aborted_calls, 1);
}

} // namespace kafka
