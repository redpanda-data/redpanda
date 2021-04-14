/*
 * Copyright 2020 Vectorized, Inc.
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
#include "kafka/protocol/errors.h"
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

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

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
    static constexpr const int8_t tx_snapshot_version = 0;
    struct tx_range {
        model::producer_identity pid;
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

    struct seq_entry {
        model::producer_identity pid;
        int32_t seq;
        model::timestamp::type last_write_timestamp;
    };

    struct tx_snapshot {
        std::vector<model::producer_identity> fenced;
        std::vector<tx_range> ongoing;
        std::vector<prepare_marker> prepared;
        std::vector<tx_range> aborted;
        model::offset offset;
        std::vector<seq_entry> seqs;
    };

    static constexpr model::control_record_version
      prepare_control_record_version{0};
    static constexpr model::control_record_version fence_control_record_version{
      0};

    explicit rm_stm(ss::logger&, raft::consensus*);

    ss::future<checked<model::term_id, tx_errc>>
      begin_tx(model::producer_identity);
    ss::future<tx_errc> prepare_tx(
      model::term_id,
      model::partition_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<tx_errc> commit_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<tx_errc>
      abort_tx(model::producer_identity, model::timeout_clock::duration);

    model::offset last_stable_offset();
    ss::future<std::vector<rm_stm::tx_range>>
      aborted_transactions(model::offset, model::offset);

    ss::future<checked<raft::replicate_result, kafka::error_code>> replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options);

private:
    void load_snapshot(stm_snapshot_header, iobuf&&) override;
    stm_snapshot take_snapshot() override;

    bool check_seq(model::batch_identity);

    ss::future<checked<raft::replicate_result, kafka::error_code>>
      replicate_tx(model::batch_identity, model::record_batch_reader);

    ss::future<checked<raft::replicate_result, kafka::error_code>>
      replicate_seq(
        model::batch_identity,
        model::record_batch_reader,
        raft::replicate_options);

    void compact_snapshot();

    ss::future<> apply(model::record_batch) override;
    void apply_prepare(rm_stm::prepare_marker);
    void apply_control(model::producer_identity, model::control_record_type);
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
        absl::flat_hash_map<model::producer_identity, tx_range> aborted;
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
        absl::btree_set<model::producer_identity> expected;
        // 'prepare' command may be replicated but reject during the apply phase
        // because of the fencing and race with abort. we use this field to
        // let a 'prepare' initator know the status of the 'prepare' command
        // once state_machine::apply catches up with the prepare's offset
        absl::flat_hash_map<model::producer_identity, bool> has_prepare_applied;

        void forget(model::producer_identity pid) {
            expected.erase(pid);
            estimated.erase(pid);
            has_prepare_applied.erase(pid);
            auto tx_start_it = tx_start.find(pid);
            if (tx_start_it != tx_start.end()) {
                tx_starts.erase(tx_start_it->second);
                tx_start.erase(pid);
            }
        }
    };

    log_state _log_state;
    mem_state _mem_state;
    model::timestamp _oldest_session;
    std::chrono::milliseconds _sync_timeout;
    model::violation_recovery_policy _recovery_policy;
    std::chrono::milliseconds _transactional_id_expiration;
};

} // namespace cluster
