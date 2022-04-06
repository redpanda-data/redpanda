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

#include "cluster/persisted_stm.h"
#include "cluster/tx_utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "raft/types.h"
#include "storage/snapshot.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

#include <system_error>

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
class rm_stm final : public persisted_stm {
public:
    using clock_type = ss::lowres_clock;
    using time_point_type = clock_type::time_point;
    using duration_type = clock_type::duration;

    static constexpr const int8_t abort_snapshot_version = 0;
    struct tx_range {
        model::producer_identity pid;
        model::offset first;
        model::offset last;
    };

    struct abort_index {
        model::offset first;
        model::offset last;
    };

    struct prepare_marker {
        // partition of the transaction manager
        // reposible for curent transaction
        model::partition_id tm_partition;
        // tx_seq identifies a transaction within a session
        model::tx_seq tx_seq;
        model::producer_identity pid;
    };

    struct seq_cache_entry {
        int32_t seq{-1};
        model::offset offset;
    };

    struct seq_entry {
        static const int seq_cache_size = 5;
        model::producer_identity pid;
        int32_t seq{-1};
        model::offset last_offset{-1};
        ss::circular_buffer<seq_cache_entry> seq_cache;
        model::timestamp::type last_write_timestamp;

        seq_entry copy() const {
            seq_entry ret;
            ret.pid = pid;
            ret.seq = seq;
            ret.last_offset = last_offset;
            ret.seq_cache.reserve(seq_cache.size());
            std::copy(
              seq_cache.cbegin(),
              seq_cache.cend(),
              std::back_inserter(ret.seq_cache));
            ret.last_write_timestamp = last_write_timestamp;
            return ret;
        }

        void update(int32_t new_seq, model::offset new_offset) {
            if (new_seq < seq) {
                return;
            }

            if (seq == new_seq) {
                last_offset = new_offset;
                return;
            }

            if (seq >= 0 && last_offset >= model::offset{0}) {
                auto entry = seq_cache_entry{.seq = seq, .offset = last_offset};
                seq_cache.push_back(entry);
                while (seq_cache.size() >= seq_entry::seq_cache_size) {
                    seq_cache.pop_front();
                }
            }

            seq = new_seq;
            last_offset = new_offset;
        }
    };

    struct tx_snapshot {
        static constexpr uint8_t version = 1;

        std::vector<model::producer_identity> fenced;
        std::vector<tx_range> ongoing;
        std::vector<prepare_marker> prepared;
        std::vector<tx_range> aborted;
        std::vector<abort_index> abort_indexes;
        model::offset offset;
        std::vector<seq_entry> seqs;
    };

    struct abort_snapshot {
        model::offset first;
        model::offset last;
        std::vector<tx_range> aborted;

        bool match(abort_index idx) {
            return idx.first == first && idx.last == last;
        }
    };

    static constexpr int8_t prepare_control_record_version{0};
    static constexpr int8_t fence_control_record_version{0};

    explicit rm_stm(
      ss::logger&,
      raft::consensus*,
      ss::sharded<cluster::tx_gateway_frontend>&);

    ss::future<checked<model::term_id, tx_errc>> begin_tx(
      model::producer_identity, model::tx_seq, std::chrono::milliseconds);
    ss::future<tx_errc> prepare_tx(
      model::term_id,
      model::partition_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<tx_errc> commit_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<tx_errc> abort_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);

    model::offset last_stable_offset();
    ss::future<std::vector<rm_stm::tx_range>>
      aborted_transactions(model::offset, model::offset);

    ss::future<result<raft::replicate_result>> replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options);

    ss::future<> stop() override;

    void testing_only_disable_auto_abort() { _is_autoabort_enabled = false; }

    void testing_only_enable_transactions() { _is_tx_enabled = true; }

    struct expiration_info {
        duration_type timeout;
        time_point_type last_update;

        time_point_type deadline() const { return last_update + timeout; }
    };

    struct transaction_info {
        enum class status_t { ongoing, preparing, prepared, initiating };
        status_t status;

        model::offset lso_bound;
        std::optional<expiration_info> info;

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

protected:
    ss::future<> handle_eviction() override;

private:
    ss::future<checked<model::term_id, tx_errc>> do_begin_tx(
      model::producer_identity, model::tx_seq, std::chrono::milliseconds);
    ss::future<tx_errc> do_prepare_tx(
      model::term_id,
      model::partition_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<tx_errc> do_commit_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<tx_errc> do_abort_tx(
      model::producer_identity,
      std::optional<model::tx_seq>,
      model::timeout_clock::duration);
    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;
    ss::future<std::optional<abort_snapshot>> load_abort_snapshot(abort_index);
    ss::future<> save_abort_snapshot(abort_snapshot);

    bool check_seq(model::batch_identity);
    std::optional<model::offset> known_seq(model::batch_identity) const;
    void set_seq(model::batch_identity, model::offset);

    ss::future<result<raft::replicate_result>>
      replicate_tx(model::batch_identity, model::record_batch_reader);

    ss::future<result<raft::replicate_result>> replicate_seq(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options);

    void compact_snapshot();

    ss::future<bool> sync(model::timeout_clock::duration);
    bool check_tx_permitted();

    void track_tx(model::producer_identity, std::chrono::milliseconds);
    void abort_old_txes();
    ss::future<> do_abort_old_txes();
    ss::future<> try_abort_old_tx(model::producer_identity);
    ss::future<> do_try_abort_old_tx(model::producer_identity);
    void try_arm(time_point_type);

    ss::future<std::error_code> do_mark_expired(model::producer_identity pid);

    bool is_known_session(model::producer_identity pid) const {
        auto is_known = false;
        is_known |= _mem_state.estimated.contains(pid);
        is_known |= _mem_state.tx_start.contains(pid);
        is_known |= _log_state.ongoing_map.contains(pid);
        return is_known;
    }

    abort_origin
    get_abort_origin(const model::producer_identity&, model::tx_seq) const;

    ss::future<> apply(model::record_batch) override;
    void apply_fence(model::record_batch&&);
    void apply_prepare(rm_stm::prepare_marker);
    ss::future<>
      apply_control(model::producer_identity, model::control_record_type);
    void apply_data(model::batch_identity, model::offset);

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
    struct log_state {
        // we enforce monotonicity of epochs related to the same producer_id
        // and fence off out of order requests
        absl::flat_hash_map<model::producer_id, model::producer_epoch>
          fence_pid_epoch;
        // a map from session id (aka producer_identity) to its current tx
        absl::flat_hash_map<model::producer_identity, tx_range> ongoing_map;
        // a heap of the first offsets of the ongoing transactions
        absl::btree_set<model::offset> ongoing_set;
        absl::flat_hash_map<model::producer_identity, prepare_marker> prepared;
        std::vector<tx_range> aborted;
        std::vector<abort_index> abort_indexes;
        abort_snapshot last_abort_snapshot{.last = model::offset(-1)};
        // the only piece of data which we update on replay and before
        // replicating the command. we use the highest seq number to resolve
        // conflicts. if the replication fails we reject a command but clients
        // by spec should be ready for thier commands being rejected so it's
        // ok by design to have false rejects
        absl::flat_hash_map<model::producer_identity, seq_entry> seq_table;
    };

    struct mem_state {
        // once raft's term has passed mem_state::term we wipe mem_state
        // and wait until log_state catches up with current committed index.
        // with this approach a combination of mem_state and log_state is
        // always up to date
        model::term_id term;
        // a map from producer_identity (a session) to the first offset of
        // the current transaction in this session
        absl::flat_hash_map<model::producer_identity, model::offset> tx_start;
        // a heap of the first offsets of all ongoing transactions
        absl::btree_set<model::offset> tx_starts;
        // before we replicate the first batch of a transaction we don't know
        // its offset but we must prevent read_comitted fetch from getting it
        // so we use last seen offset to estimate it
        absl::flat_hash_map<model::producer_identity, model::offset> estimated;
        // a set of ongoing sessions. we use it  to prevent some client protocol
        // errors like the transactional writes outside of a transaction
        absl::flat_hash_map<model::producer_identity, model::tx_seq> expected;
        // `preparing` helps to identify failed prepare requests and use them to
        // filter out stale abort requests
        absl::flat_hash_map<model::producer_identity, prepare_marker> preparing;
        absl::flat_hash_map<model::producer_identity, expiration_info>
          expiration;
        model::offset last_end_tx{-1};

        void forget(model::producer_identity pid) {
            expected.erase(pid);
            estimated.erase(pid);
            preparing.erase(pid);
            expiration.erase(pid);
            auto tx_start_it = tx_start.find(pid);
            if (tx_start_it != tx_start.end()) {
                tx_starts.erase(tx_start_it->second);
                tx_start.erase(pid);
            }
        }
    };

    ss::lw_shared_ptr<mutex> get_tx_lock(model::producer_id pid) {
        auto lock_it = _tx_locks.find(pid);
        if (lock_it == _tx_locks.end()) {
            auto [new_it, _] = _tx_locks.try_emplace(
              pid, ss::make_lw_shared<mutex>());
            lock_it = new_it;
        }
        return lock_it->second;
    }

    transaction_info::status_t
    get_tx_status(model::producer_identity pid) const;
    std::optional<expiration_info>
    get_expiration_info(model::producer_identity pid) const;

    ss::basic_rwlock<> _state_lock;
    absl::flat_hash_map<model::producer_id, ss::lw_shared_ptr<mutex>> _tx_locks;
    log_state _log_state;
    mem_state _mem_state;
    ss::timer<clock_type> auto_abort_timer;
    model::timestamp _oldest_session;
    std::chrono::milliseconds _sync_timeout;
    std::chrono::milliseconds _tx_timeout_delay;
    std::chrono::milliseconds _abort_interval_ms;
    uint32_t _abort_index_segment_size;
    uint32_t _seq_table_min_size;
    model::violation_recovery_policy _recovery_policy;
    std::chrono::milliseconds _transactional_id_expiration;
    bool _is_autoabort_enabled{true};
    bool _is_autoabort_active{false};
    bool _is_tx_enabled{false};
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    storage::snapshot_manager _abort_snapshot_mgr;
};

} // namespace cluster
