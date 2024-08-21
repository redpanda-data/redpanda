/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "cluster/fwd.h"
#include "cluster/producer_state.h"
#include "cluster/state_machine_registry.h"
#include "cluster/topic_table.h"
#include "cluster/tx_utils.h"
#include "cluster/types.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "features/feature_table.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine.h"
#include "storage/snapshot.h"
#include "utils/available_promise.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"
#include "utils/tracking_allocator.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <string_view>
#include <system_error>

namespace mt = util::mem_tracked;

struct rm_stm_test_fixture;

namespace cluster {

/**
 * RM stands for resource manager. There is a RM for each partition. It's a
 * part of the transactional sub system.
 *
 * Resource manager:
 *   - tracks all transactions affecting a partition
 *   - maintains the last stable offset
 *   - keeps a list of the aborted transactions
 *   - enforces monotonicity of the sequential numbers
 *   - fences against old epochs
 */
class rm_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "rm_stm";
    using clock_type = ss::lowres_clock;
    using time_point_type = clock_type::time_point;
    using duration_type = clock_type::duration;

    using producers_t = mt::
      map_t<absl::btree_map, model::producer_identity, cluster::producer_ptr>;

    static constexpr const int8_t abort_snapshot_version = 0;
    using tx_range = model::tx_range;

    struct abort_index {
        model::offset first;
        model::offset last;

        bool operator==(const abort_index&) const = default;
    };

    struct prepare_marker {
        // partition of the transaction manager
        // reposible for curent transaction
        model::partition_id tm_partition;
        // tx_seq identifies a transaction within a session
        model::tx_seq tx_seq;
        model::producer_identity pid;

        bool operator==(const prepare_marker&) const = default;
    };

    struct abort_snapshot {
        model::offset first;
        model::offset last;
        fragmented_vector<tx_range> aborted;

        bool match(abort_index idx) {
            return idx.first == first && idx.last == last;
        }
        friend std::ostream& operator<<(std::ostream&, const abort_snapshot&);

        bool operator==(const abort_snapshot&) const = default;
    };

    static constexpr int8_t prepare_control_record_version{0};
    static constexpr int8_t fence_control_record_v0_version{0};
    static constexpr int8_t fence_control_record_v1_version{1};
    static constexpr int8_t fence_control_record_version{2};

    explicit rm_stm(
      ss::logger&,
      raft::consensus*,
      ss::sharded<cluster::tx_gateway_frontend>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<producer_state_manager>&,
      std::optional<model::vcluster_id>);

    ss::future<checked<model::term_id, tx_errc>> begin_tx(
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<tx_errc> commit_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<tx_errc> abort_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    /**
     * Returns the next after the last one decided. If there are no ongoing
     * transactions this will return next offset to be applied to the the stm.
     */
    model::offset last_stable_offset();
    ss::future<fragmented_vector<rm_stm::tx_range>>
      aborted_transactions(model::offset, model::offset);

    /**
     * Returns highest producer ID of any batch that has been applied to this
     * partition. Note that the corresponding transactions may or may not have
     * been committed or aborted; the only certainty of this ID is that it has
     * been used.
     *
     * Callers should be wary to either ensure that the stm is synced before
     * calling, or ensure that the producer_id doesn't need to reflect batches
     * later than the max_collectible_offset.
     */
    model::producer_id highest_producer_id() const;

    model::offset max_collectible_offset() override {
        const auto lso = last_stable_offset();
        if (lso < model::offset{0}) {
            return model::offset{};
        }
        /**
         * Since the LSO may be equal to `_next` we must return offset which has
         * already been decided and applied hence we subtract one from the last
         * stable offset.
         */
        return model::prev_offset(lso);
    }

    storage::stm_type type() override {
        return storage::stm_type::transactional;
    }

    ss::future<fragmented_vector<model::tx_range>>
    aborted_tx_ranges(model::offset from, model::offset to) override {
        return aborted_transactions(from, to);
    }

    model::control_record_type
    parse_tx_control_batch(const model::record_batch&) override;

    kafka_stages replicate_in_stages(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options);

    ss::future<result<kafka_result>> replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options);

    ss::future<ss::basic_rwlock<>::holder> prepare_transfer_leadership();

    ss::future<> stop() override;

    ss::future<> start() override;

    void testing_only_disable_auto_abort() { _is_autoabort_enabled = false; }

    struct expiration_info {
        duration_type timeout;
        time_point_type last_update;
        bool is_expiration_requested;

        time_point_type deadline() const { return last_update + timeout; }

        bool is_expired(time_point_type now) const {
            return is_expiration_requested || deadline() <= now;
        }
    };

    struct tx_data {
        model::tx_seq tx_seq;
        model::partition_id tm_partition;
    };

    struct transaction_info {
        enum class status_t { ongoing, preparing, prepared, initiating };
        status_t status;

        model::offset lso_bound;
        std::optional<expiration_info> info;

        std::optional<int32_t> seq;

        std::string_view get_status() const {
            switch (status) {
            case status_t::ongoing:
                return "ongoing";
            case status_t::prepared:
                return "prepared";
            case status_t::preparing:
                return "preparing";
            case status_t::initiating:
                return "initiating";
            }
        }

        bool is_expired() const {
            return !info.has_value()
                   || info.value().deadline() <= clock_type::now();
        }

        std::optional<duration_type> get_staleness() const {
            if (is_expired()) {
                return std::nullopt;
            }

            auto now = ss::lowres_clock::now();
            return now - info->last_update;
        }

        std::optional<duration_type> get_timeout() const {
            if (is_expired()) {
                return std::nullopt;
            }

            return info->timeout;
        }
    };

    using transaction_set
      = absl::btree_map<model::producer_identity, rm_stm::transaction_info>;
    ss::future<result<transaction_set>> get_transactions();

    ss::future<std::error_code> mark_expired(model::producer_identity pid);

    ss::future<> remove_persistent_state() override;

    uint64_t get_local_snapshot_size() const override;

    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

    const producers_t& get_producers() const { return _producers; }

protected:
    ss::future<> apply_raft_snapshot(const iobuf&) final;

private:
    void setup_metrics();
    ss::future<> do_remove_persistent_state();
    ss::future<fragmented_vector<rm_stm::tx_range>>
      do_aborted_transactions(model::offset, model::offset);
    // Tells whether the producer is already known or is created
    // for the first time from the incoming request.
    using producer_previously_known
      = ss::bool_class<struct new_producer_created_tag>;
    std::pair<producer_ptr, producer_previously_known>
      maybe_create_producer(model::producer_identity);
    void cleanup_producer_state(model::producer_identity);
    ss::future<> reset_producers();
    model::record_batch make_fence_batch(
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<checked<model::term_id, tx_errc>> do_begin_tx(
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<tx_errc> do_commit_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<tx_errc> do_abort_tx(
      model::producer_identity,
      std::optional<model::tx_seq>,
      model::timeout_clock::duration);
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot> take_local_snapshot() override;
    ss::future<raft::stm_snapshot> do_take_local_snapshot(uint8_t version);
    ss::future<std::optional<abort_snapshot>> load_abort_snapshot(abort_index);
    ss::future<> save_abort_snapshot(abort_snapshot);

    ss::future<result<kafka_result>> do_replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<result<kafka_result>> transactional_replicate(
      model::batch_identity, model::record_batch_reader);

    ss::future<result<kafka_result>> transactional_replicate(
      model::term_id,
      producer_ptr,
      model::batch_identity,
      model::record_batch_reader,
      ssx::semaphore_units);

    ss::future<result<kafka_result>> do_transactional_replicate(
      model::term_id,
      producer_ptr,
      model::batch_identity,
      model::record_batch_reader);

    ss::future<result<kafka_result>> idempotent_replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<result<kafka_result>> idempotent_replicate(
      model::term_id,
      producer_ptr,
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>,
      ssx::semaphore_units,
      producer_previously_known);

    ss::future<result<kafka_result>> do_idempotent_replicate(
      model::term_id,
      producer_ptr,
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>,
      ssx::semaphore_units&,
      producer_previously_known);

    ss::future<result<kafka_result>> replicate_msg(
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<bool> sync(model::timeout_clock::duration);
    constexpr bool check_tx_permitted() { return true; }

    void track_tx(model::producer_identity, std::chrono::milliseconds);
    void abort_old_txes();
    ss::future<> do_abort_old_txes();
    ss::future<> try_abort_old_tx(model::producer_identity);
    ss::future<tx_errc> do_try_abort_old_tx(model::producer_identity);
    void try_arm(time_point_type);

    ss::future<std::error_code> do_mark_expired(model::producer_identity pid);

    absl::btree_set<model::producer_identity> get_expired_producers() const;

    bool is_known_session(model::producer_identity pid) const {
        auto is_known = false;
        is_known |= _mem_state.estimated.contains(pid);
        is_known |= _log_state.ongoing_map.contains(pid);
        is_known |= _log_state.current_txes.contains(pid);
        return is_known;
    }

    abort_origin
    get_abort_origin(const model::producer_identity&, model::tx_seq) const;

    ss::future<> apply(const model::record_batch&) override;
    void apply_fence(model::record_batch&&);
    void apply_control(model::producer_identity, model::control_record_type);
    void apply_data(model::batch_identity, const model::record_batch_header&);

    ss::future<> reduce_aborted_list();
    ss::future<> offload_aborted_txns();

    std::optional<model::tx_seq>
    get_tx_seq(model::producer_identity pid) const {
        auto log_it = _log_state.current_txes.find(pid);
        if (log_it != _log_state.current_txes.end()) {
            return log_it->second.tx_seq;
        }

        return std::nullopt;
    }

    /**
     * Return when the committed offset has been established when STM starts.
     */
    ss::future<model::offset> bootstrap_committed_offset();

    util::mem_tracker _tx_root_tracker{"tx-mem-root"};
    // The state of this state machine maybe change via two paths
    //
    //   - by reading the already replicated commands from raft and
    //     applying them in sequence (the classic RSM approach)
    //
    //   - by applying a command before replicating it accepting the
    //     risk that the replication may fail
    //
    // It's error prone to let this two stream of changes to modify
    // the same state e.g. in that case the pre-replicated command
    // may override legit state, fail and cause an anomaly.
    //
    // We use a segregated state to avoid this problem and reconcile the
    // different part of the state when it's needed. log_state is used
    // to replay replicated commands and mem_state to keep the effect of
    // not replicated yet commands.

    template<class T>
    using allocator = util::tracking_allocator<T>;
    struct log_state {
        explicit log_state(util::mem_tracker& parent)
          : _tracker(parent.create_child("log-state"))
          , fence_pid_epoch(mt::map<
                            absl::flat_hash_map,
                            model::producer_id,
                            model::producer_epoch>(_tracker))
          , ongoing_map(
              mt::map<absl::flat_hash_map, model::producer_identity, tx_range>(
                _tracker))
          , ongoing_set(mt::set<absl::btree_set, model::offset>(_tracker))
          , current_txes(
              mt::map<absl::flat_hash_map, model::producer_identity, tx_data>(
                _tracker))
          , expiration(mt::map<
                       absl::flat_hash_map,
                       model::producer_identity,
                       expiration_info>(_tracker)) {}

        log_state(log_state&) noexcept = delete;
        log_state(log_state&&) noexcept = delete;
        log_state& operator=(log_state&) noexcept = delete;
        log_state& operator=(log_state&&) noexcept = delete;
        ~log_state() noexcept { reset(); }

        ss::shared_ptr<util::mem_tracker> _tracker;
        // we enforce monotonicity of epochs related to the same producer_id
        // and fence off out of order requests
        mt::unordered_map_t<
          absl::flat_hash_map,
          model::producer_id,
          model::producer_epoch>
          fence_pid_epoch;
        // a map from session id (aka producer_identity) to its current tx
        mt::unordered_map_t<
          absl::flat_hash_map,
          model::producer_identity,
          tx_range>
          ongoing_map;
        // a heap of the first offsets of the ongoing transactions
        mt::set_t<absl::btree_set, model::offset> ongoing_set;
        fragmented_vector<tx_range> aborted;
        fragmented_vector<abort_index> abort_indexes;
        abort_snapshot last_abort_snapshot{.last = model::offset(-1)};
        mt::unordered_map_t<
          absl::flat_hash_map,
          model::producer_identity,
          tx_data>
          current_txes;
        mt::unordered_map_t<
          absl::flat_hash_map,
          model::producer_identity,
          expiration_info>
          expiration;

        void forget(const model::producer_identity& pid);
        void reset();
    };

    struct mem_state {
        explicit mem_state(util::mem_tracker& parent)
          : _tracker(parent.create_child("mem-state"))
          , estimated(mt::map<
                      absl::flat_hash_map,
                      model::producer_identity,
                      model::offset>(_tracker)) {}

        ss::shared_ptr<util::mem_tracker> _tracker;
        // once raft's term has passed mem_state::term we wipe mem_state
        // and wait until log_state catches up with current committed index.
        // with this approach a combination of mem_state and log_state is
        // always up to date
        model::term_id term;
        // before we replicate the first batch of a transaction we don't know
        // its offset but we must prevent read_comitted fetch from getting it
        // so we use last seen offset to estimate it
        mt::unordered_map_t<
          absl::flat_hash_map,
          model::producer_identity,
          model::offset>
          estimated;

        // depending on the inflight state we may use last_applied or
        // committed_index as LSO; the alternation between them may
        // violate LSO monotonicity so we need to explicitly maintain
        // it with last_lso
        model::offset last_lso{-1};

        void forget(model::producer_identity pid) { estimated.erase(pid); }
    };

    ss::lw_shared_ptr<mutex> get_tx_lock(model::producer_id pid) {
        auto lock_it = _tx_locks.find(pid);
        if (lock_it == _tx_locks.end()) {
            auto [new_it, _] = _tx_locks.try_emplace(
              pid, ss::make_lw_shared<mutex>("get_tx_lock"));
            lock_it = new_it;
        }
        return lock_it->second;
    }

    kafka::offset from_log_offset(model::offset old_offset) const;
    model::offset to_log_offset(kafka::offset new_offset) const;

    std::optional<expiration_info>
    get_expiration_info(model::producer_identity pid) const;
    std::optional<int32_t> get_seq_number(model::producer_identity pid) const;

    uint8_t active_snapshot_version();

    template<class T>
    void fill_snapshot_wo_seqs(T&);

    bool is_transaction_partitioning() const {
        return _feature_table.local().is_active(
          features::feature::transaction_partitioning);
    }

    friend std::ostream& operator<<(std::ostream&, const mem_state&);
    friend std::ostream& operator<<(std::ostream&, const log_state&);
    ss::future<> maybe_log_tx_stats();
    void log_tx_stats();

    // Defines the commit offset range for the stm bootstrap.
    // Set on first apply upcall and used to identify if the
    // stm is still replaying the log.
    std::optional<model::offset> _bootstrap_committed_offset;
    ss::basic_rwlock<> _state_lock;
    bool _is_abort_idx_reduction_requested{false};
    mt::unordered_map_t<
      absl::flat_hash_map,
      model::producer_id,
      ss::lw_shared_ptr<mutex>>
      _tx_locks;
    log_state _log_state;
    mem_state _mem_state;
    ss::timer<clock_type> auto_abort_timer;
    std::chrono::milliseconds _sync_timeout;
    std::chrono::milliseconds _tx_timeout_delay;
    std::chrono::milliseconds _abort_interval_ms;
    uint32_t _abort_index_segment_size;
    bool _is_autoabort_enabled{true};
    bool _is_autoabort_active{false};
    bool _is_tx_enabled{false};
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    storage::snapshot_manager _abort_snapshot_mgr;
    absl::flat_hash_map<std::pair<model::offset, model::offset>, uint64_t>
      _abort_snapshot_sizes{};
    ss::sharded<features::feature_table>& _feature_table;
    config::binding<std::chrono::seconds> _log_stats_interval_s;
    ss::timer<clock_type> _log_stats_timer;
    prefix_logger _ctx_log;
    ss::sharded<producer_state_manager>& _producer_state_manager;
    std::optional<model::vcluster_id> _vcluster_id;

    producers_t _producers;
    metrics::internal_metric_groups _metrics;
    ss::abort_source _as;
    ss::gate _gate;
    // Highest producer ID applied to this stm.
    model::producer_id _highest_producer_id;

    friend struct ::rm_stm_test_fixture;
};

struct fence_batch_data {
    model::batch_identity bid;
    std::optional<model::tx_seq> tx_seq;
    std::optional<std::chrono::milliseconds> transaction_timeout_ms;
    model::partition_id tm;
};

class rm_stm_factory : public state_machine_factory {
public:
    rm_stm_factory(
      bool enable_transactions,
      bool enable_idempotence,
      ss::sharded<tx_gateway_frontend>&,
      ss::sharded<cluster::producer_state_manager>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::topic_table>&);
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;

private:
    bool _enable_transactions;
    bool _enable_idempotence;
    ss::sharded<tx_gateway_frontend>& _tx_gateway_frontend;
    ss::sharded<cluster::producer_state_manager>& _producer_state_manager;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<topic_table>& _topics;
};

model::record_batch make_fence_batch_v1(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms);

model::record_batch make_fence_batch_v2(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm);

fence_batch_data read_fence_batch(model::record_batch&& b);

} // namespace cluster
