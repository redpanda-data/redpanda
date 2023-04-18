// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/rm_stm.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"

namespace cluster {

static ss::logger logger{"random_tx_generator"};

class random_tx_generator {
public:
    constexpr static const auto tx_timeout = std::chrono::milliseconds(
      std::numeric_limits<int32_t>::max());

    // Types of tx ops to generate
    enum tx_types { commit_only, abort_only, mixed };

    struct spec {
        int _num_txes = 5;
        int _num_rolls = 2;
        tx_types _types = mixed;
        // Interleave tx_ops if set to true.
        bool _interleave = false;

        friend std::ostream& operator<<(std::ostream& os, const spec& s) {
            fmt::print(
              os,
              "{{ num_txes: {}, num_rolls: {}, type: {}, interleave: {} }}",
              s._num_txes,
              s._num_rolls,
              s._types,
              s._interleave);
            return os;
        }
    };

    void run_workload(
      spec s,
      model::term_id term,
      ss::shared_ptr<cluster::rm_stm> stm,
      storage::disk_log_impl* log) {
        // ---- Step 1: Generate random transaction ops.
        // As we generate random txns, we populate them in a priority queue that
        // orders them by their associated weight, which controls the sequence
        // of transactions.
        std::priority_queue<tx_op_ptr, std::vector<tx_op_ptr>, tx_op_cmp> ops;
        int num_ops = 3 * s._num_txes + s._num_rolls;
        auto rand_weight = [num_ops](int prev) {
            return prev + random_generators::get_int(1, num_ops);
        };
        int idx = 0;
        for (auto i : boost::irange(s._num_txes)) {
            auto pid = model::producer_identity{i, 0};

            // Weight controls the position of the op in the heap.
            // Random weights are assigned if interleaving is enabled.
            // We still need to maintain the order within a specific
            // transaction, so we make sure they have strictly increasing
            // order of weights.
            // For non interleaving specs, we just increment and index to
            // use as weight and the heap gives us the same insertion order.
            int weight0 = s._interleave ? rand_weight(0) : idx++;
            int weight1 = s._interleave ? rand_weight(weight0) : idx++;
            int weight2 = s._interleave ? rand_weight(weight1) : idx++;

            // Every tx has 3 ops, a begin, data, <commit/abort>
            // Last op is controlled by tx_types param in the spec.
            // For mixed type spec, we randomly pick a commit/abort.
            ops.emplace(ss::make_shared(
              begin_op{tx_op_ctx{_data_gen, stm, log, pid, term}, weight0}));
            ops.emplace(ss::make_shared(
              data_op{tx_op_ctx{_data_gen, stm, log, pid, term}, weight1}));

            if (
              s._types == tx_types::commit_only
              || (s._types == tx_types::mixed && tests::random_bool())) {
                _committed_pids.insert(pid);
                ops.emplace(ss::make_shared(commit_op{
                  tx_op_ctx{_data_gen, stm, log, pid, term}, weight2}));
            } else {
                _aborted_pids.insert(pid);
                ops.emplace(ss::make_shared(abort_op{
                  tx_op_ctx{_data_gen, stm, log, pid, term}, weight2}));
            }
        }

        // Sprinkle log rolls randomly.
        for ([[maybe_unused]] auto _ : boost::irange(s._num_rolls)) {
            ops.emplace(ss::make_shared(roll_op{
              tx_op_ctx{_data_gen, stm, log, {}},
              random_generators::get_int(1, num_ops)}));
        }

        //----- Step 2: Execute ops
        while (!ops.empty()) {
            auto op = ops.top();
            op->execute();
            vlog(logger.info, "Executed op: {}", op->debug());
            ops.pop();
        }

        //---- Step 3: Force a roll and compact the log.
        log->flush().get0();
        log->force_roll(ss::default_priority_class()).get0();
        ss::abort_source as{};
        storage::compaction_config ccfg(
          model::timestamp::min(),
          std::nullopt,
          model::offset::max(),
          ss::default_priority_class(),
          as);
        // Compacts until a single sealed segment remains, other than the
        // currently active one.
        tests::cooperative_spin_wait_with_timeout(30s, [log, ccfg]() {
            return log->compact(ccfg).then(
              [log]() { return log->segment_count() == 2; });
        }).get();

        //--- Step 4: Read the log and validate the batches.

        // 1. There are no aborted keys (tracked in _aborted_xxx)
        // 2. There are no tx control markers
        // 3. Only tx control data batches allowed is fence type.
        auto lstats = log->offsets();
        storage::log_reader_config cfg(
          lstats.start_offset,
          lstats.committed_offset,
          ss::default_priority_class());
        auto reader = log->make_reader(cfg).get0();
        auto batches = copy_to_mem(reader).get0();

        int fence_batch_count = 0;
        for (auto& batch : batches) {
            auto type = batch.header().type;
            BOOST_REQUIRE_NE(type, model::record_batch_type::tx_prepare);
            if (batch.header().attrs.is_transactional()) {
                model::producer_identity pid{
                  batch.header().producer_id, batch.header().producer_epoch};
                // no control (commit/abort) batches.
                BOOST_REQUIRE(!batch.header().attrs.is_control());
                BOOST_REQUIRE_EQUAL(type, model::record_batch_type::raft_data);
                BOOST_REQUIRE(_committed_pids.contains(pid));
                BOOST_REQUIRE(!_aborted_pids.contains(pid));
            }
            if (type == model::record_batch_type::tx_fence) {
                fence_batch_count++;
            }
        }
        BOOST_REQUIRE_EQUAL(fence_batch_count, s._num_txes);
    }

private:
    enum tx_op_type : int {
        begin,
        commit,
        abort,
        data,
        roll,
    };

    struct tx_op_ctx {
        ss::shared_ptr<linear_int_kv_batch_generator> _data_gen;
        ss::shared_ptr<rm_stm> _stm;
        storage::disk_log_impl* _log;
        model::producer_identity _pid;
        model::term_id _term;
    };

    struct tx_op {
    public:
        explicit tx_op(tx_op_ctx&& ctx, int weight)
          : _ctx(std::move(ctx))
          , _weight(weight) {}
        virtual ~tx_op() noexcept = default;
        virtual void execute() = 0;
        virtual ss::sstring debug() = 0;
        virtual tx_op_type type() = 0;
        int weight() const { return _weight; }

    protected:
        tx_op_ctx _ctx;
        int _weight;
    };

    using tx_op_ptr = ss::shared_ptr<tx_op>;
    struct tx_op_cmp {
        bool operator()(tx_op_ptr l, tx_op_ptr r) {
            return l->weight() > r->weight();
        }
    };

    struct begin_op final : tx_op {
    public:
        explicit begin_op(tx_op_ctx&& ctx, int weight)
          : tx_op(std::move(ctx), weight) {}

        void execute() override {
            BOOST_REQUIRE(_ctx._stm
                            ->begin_tx(
                              _ctx._pid,
                              model::tx_seq{0},
                              tx_timeout,
                              model::partition_id(0))
                            .get0());
        }

        tx_op_type type() override { return tx_op_type::begin; }
        ss::sstring debug() override {
            return fmt::format("begin {}", _ctx._pid);
        }
    };

    struct commit_op final : tx_op {
    public:
        explicit commit_op(tx_op_ctx&& ctx, int weight)
          : tx_op(std::move(ctx), weight) {}

        void execute() override {
            BOOST_REQUIRE_EQUAL(
              _ctx._stm
                ->prepare_tx(
                  _ctx._term,
                  model::partition_id(0),
                  _ctx._pid,
                  model::tx_seq{0},
                  tx_timeout)
                .get0(),
              cluster::tx_errc::none);
            BOOST_REQUIRE_EQUAL(
              _ctx._stm->commit_tx(_ctx._pid, model::tx_seq{0}, tx_timeout)
                .get0(),
              cluster::tx_errc::none);
        }
        tx_op_type type() override { return tx_op_type::commit; }
        ss::sstring debug() override {
            return fmt::format("commit {}", _ctx._pid);
        }
    };

    struct abort_op final : tx_op {
    public:
        explicit abort_op(tx_op_ctx&& ctx, int weight)
          : tx_op(std::move(ctx), weight) {}
        void execute() override {
            BOOST_REQUIRE_EQUAL(
              _ctx._stm->abort_tx(_ctx._pid, model::tx_seq{0}, tx_timeout)
                .get0(),
              cluster::tx_errc::none);
        }
        tx_op_type type() override { return tx_op_type::abort; }
        ss::sstring debug() override {
            return fmt::format("abort {}", _ctx._pid);
        }
    };

    struct data_op final : tx_op {
    public:
        explicit data_op(tx_op_ctx&& ctx, int weight)
          : tx_op(std::move(ctx), weight) {}

        void execute() override {
            model::test::record_batch_spec spec;
            spec.producer_id = _ctx._pid.id;
            spec.producer_epoch = _ctx._pid.epoch;
            spec.is_transactional = true;
            spec.count = 5;
            auto batches = _ctx._data_gen->operator()(spec, 1);
            _data_idx = _ctx._data_gen->_idx - 1;
            BOOST_REQUIRE_EQUAL(batches.size(), 1);
            model::batch_identity bid{
              .pid = _ctx._pid,
              .first_seq = 0,
              .last_seq = spec.count - 1,
              .record_count = spec.count,
              .is_transactional = true};
            auto reader = model::make_memory_record_batch_reader(
              std::move(batches[0]));
            auto result = _ctx._stm
                            ->replicate(
                              bid,
                              std::move(reader),
                              raft::replicate_options(
                                raft::consistency_level::quorum_ack))
                            .get0();
            if (!result.has_value()) {
                vlog(logger.error, "Error {}", result.error());
            }
            BOOST_REQUIRE((bool)result);
        }

        tx_op_type type() override { return tx_op_type::data; }
        ss::sstring debug() override {
            return fmt::format("data idx: {} pid: {}", _data_idx, _ctx._pid);
        }

    private:
        int _data_idx{};
    };

    struct roll_op final : tx_op {
    public:
        explicit roll_op(tx_op_ctx&& ctx, int weight)
          : tx_op(std::move(ctx), weight) {}
        void execute() override {
            _ctx._log->force_roll(ss::default_priority_class()).get0();
        }
        tx_op_type type() override { return tx_op_type::roll; }
        ss::sstring debug() override { return "roll log"; }
    };

    ss::future<ss::circular_buffer<model::record_batch>>
    copy_to_mem(model::record_batch_reader& reader) {
        using data_t = ss::circular_buffer<model::record_batch>;
        class memory_batch_consumer {
        public:
            ss::future<ss::stop_iteration> operator()(model::record_batch b) {
                _result.push_back(std::move(b));
                return ss::make_ready_future<ss::stop_iteration>(
                  ss::stop_iteration::no);
            }
            data_t end_of_stream() { return std::move(_result); }

        private:
            data_t _result;
        };

        return reader.consume(memory_batch_consumer{}, model::no_timeout);
    }

    ss::shared_ptr<linear_int_kv_batch_generator> _data_gen
      = ss::make_shared<linear_int_kv_batch_generator>();
    // We track the commited/aborted pids as we build the tx workload
    // and tally them with the batches in the final compacted segment file.
    std::set<model::producer_identity> _aborted_pids;
    std::set<model::producer_identity> _committed_pids;
    std::set<int> _aborted_keys;
};

}; // namespace cluster
