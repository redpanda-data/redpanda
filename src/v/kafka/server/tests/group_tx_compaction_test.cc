// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/group_manager.h"
#include "kafka/server/group_tx_tracker_stm.h"
#include "kafka/server/rm_group_frontend.h"
#include "model/record_batch_types.h"
#include "model/tests/randoms.h"
#include "redpanda/tests/fixture.h"
#include "ssx/semaphore.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

ss::logger logger("group_tx_compaction_test");

static model::ntp offsets_ntp{
  model::kafka_consumer_offsets_nt.ns,
  model::kafka_consumer_offsets_nt.tp,
  model::partition_id{0}};

struct custom_configs_fixture {
    custom_configs_fixture() {
        // compaction is run manually in this test.
        cfg.get("log_disable_housekeeping_for_tests").set_value(true);
        cfg.get("group_initial_rebalance_delay").set_value(0ms);
    }
    scoped_config cfg;
};

struct group_manager_fixture
  : public custom_configs_fixture
  , public redpanda_thread_fixture
  , public seastar_test {
    ss::future<> SetUpAsync() override {
        test_cfg.get("group_topic_partitions").set_value(1);
        co_await app.group_initializer.local().assure_topic_exists();
        co_await wait_for_leader(offsets_ntp);
        // Wait until the partitions are registered.
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this] {
            auto& gm = app._group_manager;
            auto [ec, _] = gm.local().list_groups();
            return ec == kafka::error_code::none
                   && gm.local().attached_partitions_count() == 1;
        });
        co_await wait_for_version_fence();
    }

    auto join_group(kafka::join_group_request request) {
        return app._group_manager.local().join_group(std::move(request)).result;
    }

    auto sync_group(kafka::sync_group_request request) {
        return app._group_manager.local().sync_group(std::move(request)).result;
    }

    auto describe_group(kafka::group_id& group) {
        return app._group_manager.local().describe_group(offsets_ntp, group);
    }

    auto begin_tx(cluster::begin_group_tx_request request) {
        return app._group_manager.local().begin_tx(std::move(request));
    }

    auto commit_tx(cluster::commit_group_tx_request request) {
        return app._group_manager.local().commit_tx(std::move(request));
    }

    auto abort_tx(cluster::abort_group_tx_request request) {
        return app._group_manager.local().abort_tx(std::move(request));
    }

    auto tx_offset_commit(kafka::txn_offset_commit_request request) {
        return app._group_manager.local().txn_offset_commit(std::move(request));
    }

    ss::shared_ptr<storage::log> consumer_offsets_log() {
        return app.storage.local().log_mgr().get(offsets_ntp);
    }

    ss::future<> force_roll_group_partition_log() {
        auto log = app.storage.local().log_mgr().get(offsets_ntp);
        co_await consumer_offsets_log()->force_roll();
    }

    auto group_tx_stm() {
        auto log = consumer_offsets_log();
        return dynamic_pointer_cast<kafka::group_tx_tracker_stm>(
          log->stm_hookset()->transactional_stm());
    }

    ss::future<> wait_for_version_fence() {
        // wait for the version fence batch to be written
        // This batch is asynchronously applied by the group manager
        // and may happen in the middle of test runs conflicting with
        // offset expectations, so wait and until that is finished.
        auto log = consumer_offsets_log();
        struct version_fence_finder {
            ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
                if (
                  b.header().type == model::record_batch_type::version_fence) {
                    _found = true;
                    co_return ss::stop_iteration::yes;
                }
                co_return ss::stop_iteration::no;
            }

            bool end_of_stream() { return _found; }
            bool _found = false;
        };

        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [log] {
            auto lstats = log->offsets();
            auto cfg = storage::local_log_reader_config(
              lstats.start_offset, lstats.committed_offset);
            return log->make_reader(std::move(cfg)).then([](auto reader) {
                return std::move(reader).for_each_ref(
                  version_fence_finder{}, model::no_timeout);
            });
        });
    }

    scoped_config test_cfg;
};

struct executable_op {
    virtual ~executable_op() = default;
    virtual ss::future<> execute(group_manager_fixture*) = 0;
};

struct join_group_op : public executable_op {
    explicit join_group_op(kafka::group_id group)
      : group(std::move(group)) {}

    ss::future<> execute(group_manager_fixture* fixture) override {
        vlog(logger.trace, "Executing join_group_op: {}", group);

        kafka::join_group_request request;
        request.data = kafka::join_group_request_data{
          .group_id = group,
          .session_timeout_ms = 300s,
          .member_id = kafka::unknown_member_id,
          .protocol_type = kafka::protocol_type{"test"},
          .protocols = chunked_vector<kafka::join_group_request_protocol>{
            {kafka::protocol_name("test"), bytes()}}};
        request.ntp = offsets_ntp;
        auto result = co_await fixture->join_group(std::move(request));
        ASSERT_EQ_CORO(result.data.error_code, kafka::error_code::none);

        kafka::sync_group_request sync_request;
        sync_request.ntp = offsets_ntp;
        sync_request.data = kafka::sync_group_request_data{
          .group_id = group,
          .generation_id = result.data.generation_id,
          .member_id = result.data.member_id,
        };

        auto sync_result = co_await fixture->sync_group(
          std::move(sync_request));
        ASSERT_EQ_CORO(sync_result.data.error_code, kafka::error_code::none);
    }
    kafka::group_id group;
};

struct begin_tx_op : public executable_op {
    explicit begin_tx_op(cluster::begin_group_tx_request begin_req)
      : req(std::move(begin_req)) {}

    ss::future<> execute(group_manager_fixture* fixture) override {
        vlog(logger.trace, "Executing begin_tx_op: {}", req);
        auto result = co_await fixture->begin_tx(req);
        ASSERT_EQ_CORO(result.ec, cluster::tx::errc::none);
    }
    cluster::begin_group_tx_request req;
};

struct commit_tx_op : executable_op {
    explicit commit_tx_op(cluster::commit_group_tx_request commit_req)
      : req(std::move(commit_req)) {}
    ss::future<> execute(group_manager_fixture* fixture) override {
        vlog(logger.trace, "Executing commit_tx_op: {}", req);
        auto result = co_await fixture->commit_tx(req);
        ASSERT_EQ_CORO(result.ec, cluster::tx::errc::none);
    }
    cluster::commit_group_tx_request req;
};

struct abort_tx_op : executable_op {
    explicit abort_tx_op(cluster::abort_group_tx_request abort_req)
      : req(std::move(abort_req)) {}
    ss::future<> execute(group_manager_fixture* fixture) override {
        vlog(logger.trace, "Executing abort_tx_op: {}", req);
        auto result = co_await fixture->abort_tx(req);
        ASSERT_EQ_CORO(result.ec, cluster::tx::errc::none);
    }
    cluster::abort_group_tx_request req;
};

struct offset_commit_op : executable_op {
    explicit offset_commit_op(
      kafka::txn_offset_commit_request offset_commit_req)
      : req(std::move(offset_commit_req)) {}

    ss::future<> execute(group_manager_fixture* fixture) override {
        vlog(logger.trace, "Executing offset_commit_op: {}", req);
        auto result = co_await fixture->tx_offset_commit(std::move(req));
        ASSERT_FALSE_CORO(result.data.errored());
    }
    kafka::txn_offset_commit_request req;
};

struct roll_op : executable_op {
    ss::future<> execute(group_manager_fixture* fixture) override {
        vlog(logger.trace, "Executing log roll");
        return fixture->force_roll_group_partition_log();
    }
};

using executable_op_ptr = ss::shared_ptr<executable_op>;
using random_ops = std::vector<executable_op_ptr>;

static model::ntp test_ntp = model::ntp{
  model::kafka_namespace, model::topic{"test"}, model::partition_id{0}};
constexpr static const auto no_timeout = std::chrono::milliseconds(
  std::numeric_limits<int32_t>::max());

struct workload_parameters {
    enum tx_types { commit_only, abort_only, mixed };
    int num_groups;
    int num_tx_per_group;
    // To create multi segment transactions
    int num_rolls;
    tx_types tx_workload_type;

    friend std::ostream&
    operator<<(std::ostream& os, const workload_parameters& params) {
        fmt::print(
          os,
          "{{groups: {}, tx_per_group: {}, rolls: {}, tx_workload_type: {}}}",
          params.num_groups,
          params.num_tx_per_group,
          params.num_rolls,
          static_cast<int>(params.tx_workload_type));
        return os;
    }
};

random_ops generate_workload(workload_parameters params) {
    int group_id_counter = 0;
    auto next_group_id = [&] {
        return kafka::group_id{std::to_string(group_id_counter++)};
    };

    size_t op_count = 0;
    std::vector<std::queue<executable_op_ptr>> all_ops;
    all_ops.reserve(params.num_groups);

    for (int i = 0; i < params.num_groups; i++) {
        std::queue<executable_op_ptr> group_ops;
        kafka::group_id id = next_group_id();
        group_ops.emplace(ss::make_shared<join_group_op>(id));
        auto pid = model::random_producer_identity();
        for (int j = 0; j < params.num_tx_per_group; j++) {
            auto seq = model::tx_seq{j};
            // group begin
            cluster::begin_group_tx_request begin_req{
              offsets_ntp, id, pid, seq, no_timeout, model::partition_id{0}};
            group_ops.emplace(ss::make_shared<begin_tx_op>(begin_req));
            // offset commit
            kafka::txn_offset_commit_request offset_req;
            offset_req.ntp = offsets_ntp;
            offset_req.data.transactional_id = "tx.id";
            offset_req.data.group_id = id;
            offset_req.data.producer_id = kafka::producer_id{pid.id};
            offset_req.data.producer_epoch = pid.epoch;
            kafka::txn_offset_commit_request_topic topic_data;
            topic_data.name = test_ntp.tp.topic;
            topic_data.partitions.push_back(
              {.partition_index = model::partition_id{0},
               .committed_offset = model::offset{j}});
            offset_req.data.topics.push_back(std::move(topic_data));
            group_ops.emplace(
              ss::make_shared<offset_commit_op>(std::move(offset_req)));

            auto commit_group_tx
              = params.tx_workload_type == workload_parameters::commit_only
                || (params.tx_workload_type != workload_parameters::abort_only && tests::random_bool());
            // group end
            if (commit_group_tx) {
                cluster::commit_group_tx_request commit_req{
                  offsets_ntp, pid, seq, id, no_timeout};
                group_ops.emplace(ss::make_shared<commit_tx_op>(commit_req));
            } else {
                cluster::abort_group_tx_request abort_req{
                  offsets_ntp, id, pid, seq, no_timeout};
                group_ops.emplace(ss::make_shared<abort_tx_op>(abort_req));
            }
        }
        op_count += group_ops.size();
        all_ops.push_back(std::move(group_ops));
    }

    std::queue<executable_op_ptr> roll_ops;
    for (int i = 0; i < params.num_rolls; i++) {
        roll_ops.emplace(ss::make_shared<roll_op>());
    }
    op_count += roll_ops.size();
    if (roll_ops.size() > 0) {
        all_ops.push_back(std::move(roll_ops));
    }

    // Shuffle all the ops for randomness, this results in random
    // interleavings of the transactions while still preserving ordering
    // within a single transaction. Log rolls are randomly placed among
    // transactions.
    random_ops result;
    result.reserve(op_count);
    while (!all_ops.empty()) {
        // pick a random group / roll and pop it.
        auto index = random_generators::get_int<long>(all_ops.size() - 1);
        auto it = all_ops.begin() + index;
        result.push_back(it->front());
        it->pop();
        if (it->empty()) {
            all_ops.erase(it);
        }
    }
    return result;
}

ss::future<> run_workload(
  workload_parameters params,
  group_manager_fixture* fixture,
  ss::shared_ptr<storage::log> log) {
    auto ops = generate_workload(params);
    vlog(logger.info, "Starting workload: {}", params);
    ss::abort_source dummy_as;
    auto housekeeping = [&]() {
        if (!log) {
            return ss::make_ready_future<>();
        }
        return log->apply_segment_ms().then([&] {
            auto collect_offset
              = log->stm_hookset()->max_removable_local_log_offset();
            auto cfg = storage::housekeeping_config::make_config(
              model::timestamp::max(),
              std::nullopt,
              collect_offset,
              collect_offset,
              collect_offset,
              std::nullopt,
              std::chrono::milliseconds{0},
              std::chrono::milliseconds{0},
              dummy_as);
            return log->housekeeping(std::move(cfg))
              .handle_exception_type(
                [](const storage::segment_closed_exception&) {});
        });
    };
    for (auto& op : ops) {
        // Dispatch a housekeeping fiber that runs compaction
        // in the background as we append group metadata to the
        // log.
        auto housekeeping_f = housekeeping();
        co_await op->execute(fixture);
        co_await std::move(housekeeping_f);
    }
    vlog(logger.info, "Finished workload, validating... {}", params);
    // Wait until all segments are compacted and only two remain
    co_await log->flush();
    co_await log->force_roll();
    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&]() {
        return housekeeping().then(
          [log]() { return log->segment_count() == 2; });
    });

    struct batch_validator {
        bool do_validate_batch(const model::record_batch& b) {
            auto batch_type = b.header().type;
            if (
              batch_type == model::record_batch_type::group_fence_tx
              || batch_type == model::record_batch_type::group_prepare_tx
              || batch_type == model::record_batch_type::group_commit_tx
              || batch_type == model::record_batch_type::group_abort_tx
              || batch_type == model::record_batch_type::tx_fence) {
                return true;
            }
            if (b.header().attrs.is_control()) {
                return true;
            }

            return false;
        }

        void validate_batch(const model::record_batch& b) {
            ASSERT_FALSE(do_validate_batch(b)) << fmt::format(
              "Unexpected batch encountered after compaction {}", b.header());
        }
        ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
            validate_batch(b);
            co_return ss::stop_iteration::no;
        }
        void end_of_stream() {}
    };

    auto lstats = log->offsets();
    auto cfg = storage::local_log_reader_config(
      lstats.start_offset, lstats.committed_offset);
    auto reader = co_await log->make_reader(cfg);
    co_await reader.for_each_ref(batch_validator{}, model::no_timeout);
}

struct group_basic_workload_fixture
  : public group_manager_fixture
  , public ::testing::WithParamInterface<workload_parameters> {};

// Test that validates the stm is tracking open transactions correctly.
TEST_P_CORO(group_basic_workload_fixture, test_group_tx_stm_tracking) {
    auto stm = group_tx_stm();
    auto log = consumer_offsets_log();
    // Generate a commit transaction.
    auto ops = generate_workload(GetParam());
    ASSERT_EQ_CORO(ops.size(), 4);

    auto wait_until_stm_apply = [&] {
        return tests::cooperative_spin_wait_with_timeout(5s, [log, stm] {
            return log->offsets().dirty_offset == stm->last_applied_offset();
        });
    };

    auto execute_op = [&](int idx) {
        return ops[idx]->execute(this).discard_result().then(
          [&]() { return wait_until_stm_apply(); });
    };

    // prep the group
    co_await execute_op(0);

    co_await wait_until_stm_apply();
    // no transactions in flight
    ASSERT_EQ_CORO(
      stm->max_removable_local_log_offset(), log->offsets().committed_offset);
    auto before = stm->max_removable_local_log_offset();
    // begin transaction.
    co_await execute_op(1);
    ASSERT_EQ_CORO(stm->max_removable_local_log_offset(), before);
    // tx offset commit
    co_await execute_op(2);
    ASSERT_EQ_CORO(stm->max_removable_local_log_offset(), before);
    // end transaction
    co_await execute_op(3);
    // ensure max removable offset moved.
    ASSERT_GT_CORO(stm->max_removable_local_log_offset(), before);
    ASSERT_EQ_CORO(
      stm->max_removable_local_log_offset(), log->offsets().committed_offset);
}

INSTANTIATE_TEST_SUITE_P(
  group_tx_basic_test,
  group_basic_workload_fixture,
  testing::Values(
    workload_parameters{
      .num_groups = 1,
      .num_tx_per_group = 1,
      .num_rolls = 0,
      .tx_workload_type = workload_parameters::commit_only},
    workload_parameters{
      .num_groups = 1,
      .num_tx_per_group = 1,
      .num_rolls = 0,
      .tx_workload_type = workload_parameters::abort_only}));

struct group_tx_random_workload_fixture
  : public group_manager_fixture
  , public ::testing::WithParamInterface<workload_parameters> {};

TEST_P_CORO(
  group_tx_random_workload_fixture, test_compaction_with_group_transactions) {
    co_await run_workload(GetParam(), this, consumer_offsets_log());
}

#ifdef NDEBUG
static constexpr size_t num_groups = 100;
static constexpr size_t num_tx_per_group = 50;
static constexpr size_t num_rolls = 30;
#else
static constexpr size_t num_groups = 10;
static constexpr size_t num_tx_per_group = 5;
static constexpr size_t num_rolls = 3;
#endif

INSTANTIATE_TEST_SUITE_P(
  group_tx_combinations,
  group_tx_random_workload_fixture,
  testing::Values(
    workload_parameters{
      .num_groups = num_groups,
      .num_tx_per_group = num_tx_per_group,
      .num_rolls = num_rolls,
      .tx_workload_type = workload_parameters::commit_only},
    workload_parameters{
      .num_groups = num_groups,
      .num_tx_per_group = num_tx_per_group,
      .num_rolls = num_rolls,
      .tx_workload_type = workload_parameters::abort_only},
    workload_parameters{
      .num_groups = num_groups,
      .num_tx_per_group = num_tx_per_group,
      .num_rolls = num_rolls,
      .tx_workload_type = workload_parameters::mixed}));

// Regression test for a bug where compaction removes a commit batch that was
// written after a local STM snapshot was taken while a transaction was open.
//
// The sequence that triggers the bug:
// 1. Transaction begins (fence at offset F), STM records open tx
// 2. Local snapshot is taken at offset >= F but < commit offset C
//    (snapshot captures the open tx state)
// 3. Transaction commits (commit at offset C)
// 4. max_removable_offset advances past C (all txs closed)
// 5. Compaction runs with the advanced max_removable_offset, removing the
//    commit batch at C from the log segment
// 6. On restart, STM loads the snapshot (tx at F is open), replays the
//    compacted log where the commit at C is gone
// 7. The open tx at F is never resolved -> max_removable_offset stuck at
//    prev(F) permanently
//
// The fix limits the local snapshot offset to max_removable_local_log_offset -
// the offset before the earliest open transaction - so the snapshot never
// captures open tx state.
//
// Instead of a full node restart, we simulate recovery by: taking the snapshot,
// compacting, then re-applying the snapshot to the STM and replaying batches
// from the compacted log. This is exactly what the STM does on startup.
TEST_F_CORO(
  group_manager_fixture, test_stale_open_tx_after_snapshot_and_compaction) {
    auto stm = group_tx_stm();
    auto log = consumer_offsets_log();

    auto wait_until_stm_apply = [&] {
        return tests::cooperative_spin_wait_with_timeout(5s, [&] {
            return consumer_offsets_log()->offsets().dirty_offset
                   == group_tx_stm()->last_applied_offset();
        });
    };

    // Step 1: Join a group so the STM starts tracking it.
    kafka::group_id gid{"snapshot-compaction-test"};
    {
        kafka::join_group_request jreq;
        jreq.data = kafka::join_group_request_data{
          .group_id = gid,
          .session_timeout_ms = 300s,
          .member_id = kafka::unknown_member_id,
          .protocol_type = kafka::protocol_type{"test"},
          .protocols = chunked_vector<kafka::join_group_request_protocol>{
            {kafka::protocol_name("test"), bytes()}}};
        jreq.ntp = offsets_ntp;
        auto jresult = co_await join_group(std::move(jreq));
        ASSERT_EQ_CORO(jresult.data.error_code, kafka::error_code::none);

        kafka::sync_group_request sreq;
        sreq.ntp = offsets_ntp;
        sreq.data = kafka::sync_group_request_data{
          .group_id = gid,
          .generation_id = jresult.data.generation_id,
          .member_id = jresult.data.member_id,
        };
        auto sresult = co_await sync_group(std::move(sreq));
        ASSERT_EQ_CORO(sresult.data.error_code, kafka::error_code::none);
    }
    co_await wait_until_stm_apply();

    auto pid = model::producer_identity{42, 0};

    // Step 2: Run several committed transactions to build up log data so that
    // compaction has material to work with.
    for (int i = 0; i < 20; i++) {
        auto seq = model::tx_seq{i};
        auto bresult = co_await begin_tx(
          cluster::begin_group_tx_request{
            offsets_ntp, gid, pid, seq, no_timeout, model::partition_id{0}});
        ASSERT_EQ_CORO(bresult.ec, cluster::tx::errc::none);

        kafka::txn_offset_commit_request oreq;
        oreq.ntp = offsets_ntp;
        oreq.data.transactional_id = "tx.id";
        oreq.data.group_id = gid;
        oreq.data.producer_id = kafka::producer_id{pid.id};
        oreq.data.producer_epoch = pid.epoch;
        kafka::txn_offset_commit_request_topic tdata;
        tdata.name = test_ntp.tp.topic;
        tdata.partitions.push_back(
          {.partition_index = model::partition_id{0},
           .committed_offset = model::offset{i}});
        oreq.data.topics.push_back(std::move(tdata));
        auto oresult = co_await tx_offset_commit(std::move(oreq));
        ASSERT_FALSE_CORO(oresult.data.errored());

        auto cresult = co_await commit_tx(
          cluster::commit_group_tx_request{
            offsets_ntp, pid, seq, gid, no_timeout});
        ASSERT_EQ_CORO(cresult.ec, cluster::tx::errc::none);
    }

    co_await wait_until_stm_apply();
    // All txs committed - mrlo should be at committed_offset.
    ASSERT_EQ_CORO(
      stm->max_removable_local_log_offset(), log->offsets().committed_offset);

    // Step 3: Roll the log to create a segment boundary, then begin a new
    // transaction. The fence batch pins max_removable_offset.
    co_await log->force_roll();

    auto open_tx_seq = model::tx_seq{20};
    {
        auto bresult = co_await begin_tx(
          cluster::begin_group_tx_request{
            offsets_ntp,
            gid,
            pid,
            open_tx_seq,
            no_timeout,
            model::partition_id{0}});
        ASSERT_EQ_CORO(bresult.ec, cluster::tx::errc::none);
    }
    co_await wait_until_stm_apply();

    // mrlo is now pinned at the offset before the open tx's fence.
    auto mrlo_during_open_tx = stm->max_removable_local_log_offset();
    ASSERT_LT_CORO(mrlo_during_open_tx, log->offsets().committed_offset);

    // Step 4: Capture and persist a local snapshot while the tx is open.
    // We save the snapshot data so we can re-persist it just before restart —
    // the force_roll in step 6 triggers a background snapshot that would
    // otherwise overwrite this one with a clean snapshot (all txs committed).
    auto stale_snapshot = co_await stm->take_local_snapshot(
      ssx::semaphore_units{});
    co_await stm->write_local_snapshot();

    // Step 5: Commit the transaction and write more data so that mrlo advances
    // past the commit offset, allowing compaction to remove it.
    {
        kafka::txn_offset_commit_request oreq;
        oreq.ntp = offsets_ntp;
        oreq.data.transactional_id = "tx.id";
        oreq.data.group_id = gid;
        oreq.data.producer_id = kafka::producer_id{pid.id};
        oreq.data.producer_epoch = pid.epoch;
        kafka::txn_offset_commit_request_topic tdata;
        tdata.name = test_ntp.tp.topic;
        tdata.partitions.push_back(
          {.partition_index = model::partition_id{0},
           .committed_offset = model::offset{20}});
        oreq.data.topics.push_back(std::move(tdata));
        auto oresult = co_await tx_offset_commit(std::move(oreq));
        ASSERT_FALSE_CORO(oresult.data.errored());
    }
    {
        auto cresult = co_await commit_tx(
          cluster::commit_group_tx_request{
            offsets_ntp, pid, open_tx_seq, gid, no_timeout});
        ASSERT_EQ_CORO(cresult.ec, cluster::tx::errc::none);
    }

    // Run a few more committed transactions so compaction has newer data with
    // the same keys, causing the older commit batch to be deduplicated away.
    for (int i = 21; i < 40; i++) {
        auto seq = model::tx_seq{i};
        auto bresult = co_await begin_tx(
          cluster::begin_group_tx_request{
            offsets_ntp, gid, pid, seq, no_timeout, model::partition_id{0}});
        ASSERT_EQ_CORO(bresult.ec, cluster::tx::errc::none);

        kafka::txn_offset_commit_request oreq;
        oreq.ntp = offsets_ntp;
        oreq.data.transactional_id = "tx.id";
        oreq.data.group_id = gid;
        oreq.data.producer_id = kafka::producer_id{pid.id};
        oreq.data.producer_epoch = pid.epoch;
        kafka::txn_offset_commit_request_topic tdata;
        tdata.name = test_ntp.tp.topic;
        tdata.partitions.push_back(
          {.partition_index = model::partition_id{0},
           .committed_offset = model::offset{i}});
        oreq.data.topics.push_back(std::move(tdata));
        auto oresult = co_await tx_offset_commit(std::move(oreq));
        ASSERT_FALSE_CORO(oresult.data.errored());

        auto cresult = co_await commit_tx(
          cluster::commit_group_tx_request{
            offsets_ntp, pid, seq, gid, no_timeout});
        ASSERT_EQ_CORO(cresult.ec, cluster::tx::errc::none);
    }

    co_await wait_until_stm_apply();
    auto mrlo_before_compaction = stm->max_removable_local_log_offset();
    ASSERT_EQ_CORO(mrlo_before_compaction, log->offsets().committed_offset);

    // Step 6: Roll and compact. Compaction is allowed up to
    // max_removable_offset which is now past the commit batch, so the commit
    // batch can be removed.
    co_await log->flush();
    co_await log->force_roll();

    ss::abort_source as;
    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&]() {
        auto collect_offset
          = log->stm_hookset()->max_removable_local_log_offset();
        return log->apply_segment_ms().then([&, collect_offset] {
            auto cfg = storage::housekeeping_config::make_config(
              model::timestamp::max(),
              std::nullopt,
              collect_offset,
              collect_offset,
              collect_offset,
              std::nullopt,
              std::chrono::milliseconds{0},
              std::chrono::milliseconds{0},
              as);
            return log->housekeeping(std::move(cfg))
              .handle_exception_type(
                [](const storage::segment_closed_exception&) {})
              .then([&] {
                  // Compact until the old segments are fully compacted
                  // (2 segments: one compacted + one active).
                  return log->segment_count() == 2;
              });
        });
    });

    // Step 7: Re-persist the snapshot captured in step 4 (while the tx was
    // open). This simulates the real-world scenario where the last snapshot on
    // disk was taken while a transaction was open — background snapshotting
    // during compaction would otherwise overwrite it with a clean one.
    co_await stm->apply_local_snapshot(
      stale_snapshot.header, stale_snapshot.data.copy());
    co_await stm->write_local_snapshot();

    // restart() resets them, avoid UAF.
    stm = nullptr;
    log = nullptr;
    // restart() uses blocking .get() calls, so wrap in ss::async.
    co_await ss::async([this] { restart(should_wipe::no); });
    co_await wait_for_leader(offsets_ntp);
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this] {
        auto& gm = app._group_manager;
        auto [ec, _] = gm.local().list_groups();
        return ec == kafka::error_code::none
               && gm.local().attached_partitions_count() == 1;
    });

    // Refresh references after restart.
    stm = group_tx_stm();
    log = consumer_offsets_log();
    co_await wait_until_stm_apply();

    // Step 9: Verify that max_removable_offset is at committed_offset, not
    // stuck at the pre-commit value from the stale snapshot.
    auto mrlo_after_restart = stm->max_removable_local_log_offset();
    ASSERT_EQ_CORO(mrlo_after_restart, log->offsets().committed_offset)
      << "max_removable_offset is stuck at " << mrlo_after_restart
      << " instead of committed_offset " << log->offsets().committed_offset
      << ". This indicates a stale open transaction from the snapshot was not "
         "resolved after compaction removed the commit batch.";
}
