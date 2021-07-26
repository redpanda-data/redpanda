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
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/types.h"
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

#include <absl/container/flat_hash_map.h>

#include <compare>

namespace cluster {

struct tm_transaction {
    static constexpr uint8_t version = 0;

    enum tx_status {
        ongoing,
        preparing,
        prepared,
        aborting, // abort is initiated by a client
        killed,   // abort is initiated by a timeout
        ready,
    };

    struct tx_partition {
        model::ntp ntp;
        model::term_id etag;
    };

    struct tx_group {
        kafka::group_id group_id;
        model::term_id etag;
    };

    // id of an application executing a transaction. in the early
    // drafts of Kafka protocol transactional_id used to be named
    // application_id
    kafka::transactional_id id;
    // another misnomer in fact producer_identity identifies a
    // session of transactional_id'ed application as any given
    // moment there maybe only one session per tx.id
    model::producer_identity pid;
    // tx_seq identifues a transactions within a session so a
    // triple (transactional_id, producer_identity, tx_seq) uniquely
    // identidies a transaction
    model::tx_seq tx_seq;
    // term of a transaction coordinated started a transaction.
    // transactions can't span cross term to prevent loss of information stored
    // only in memory (partitions and groups).
    model::term_id etag;
    tx_status status;
    std::chrono::milliseconds timeout_ms;
    std::vector<tx_partition> partitions;
    std::vector<tx_group> groups;

    friend std::ostream& operator<<(std::ostream&, const tm_transaction&);
};

struct tm_snapshot {
    model::offset offset;
    std::vector<tm_transaction> transactions;
};

/**
 * TM stands for transaction manager (2PC slang for a transaction
 * coordinator). It's a state machine which maintains stats of the
 * ongoing and executed transactions and maps tx.id to its latest
 * session (producer_identity)
 */
class tm_stm final : public persisted_stm {
public:
    static constexpr const int8_t supported_version = 0;

    enum op_status {
        success,
        not_found,
        conflict,
        unknown,
    };

    explicit tm_stm(ss::logger&, raft::consensus*);

    std::optional<tm_transaction> get_tx(kafka::transactional_id);
    checked<tm_transaction, tm_stm::op_status>
      mark_tx_ongoing(kafka::transactional_id);
    bool add_partitions(
      kafka::transactional_id, std::vector<tm_transaction::tx_partition>);
    bool add_group(kafka::transactional_id, kafka::group_id, model::term_id);
    bool is_actual_term(model::term_id term) { return _insync_term == term; }
    std::optional<kafka::transactional_id>
    get_id_by_pid(model::producer_identity pid) {
        auto tx_it = _pid_tx_id.find(pid);
        std::optional<kafka::transactional_id> r;
        if (tx_it != _pid_tx_id.end()) {
            r = tx_it->second;
        }
        return r;
    }

    ss::future<bool> barrier();

    ss::future<std::optional<tm_transaction>>
      get_actual_tx(kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      mark_tx_ready(kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      mark_tx_ready(kafka::transactional_id, model::term_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      try_change_status(kafka::transactional_id, tm_transaction::tx_status);
    ss::future<tm_stm::op_status> re_register_producer(
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity);
    ss::future<tm_stm::op_status> register_new_producer(
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity);

    // before calling a tm_stm modifying operation a caller should
    // take get_tx_lock mutex
    ss::lw_shared_ptr<mutex> get_tx_lock(kafka::transactional_id tid) {
        auto [lock_it, inserted] = _tx_locks.try_emplace(tid, nullptr);
        if (inserted) {
            lock_it->second = ss::make_lw_shared<mutex>();
        }
        return lock_it->second;
    }

private:
    ss::future<> load_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;

    void expire_old_txs();

    std::chrono::milliseconds _sync_timeout;
    model::violation_recovery_policy _recovery_policy;
    absl::flat_hash_map<kafka::transactional_id, tm_transaction> _tx_table;
    absl::flat_hash_map<model::producer_identity, kafka::transactional_id>
      _pid_tx_id;
    absl::flat_hash_map<kafka::transactional_id, ss::lw_shared_ptr<mutex>>
      _tx_locks;
    ss::future<> apply(model::record_batch b) override;

    ss::future<checked<tm_transaction, tm_stm::op_status>>
      update_tx(tm_transaction, model::term_id);
    ss::future<result<raft::replicate_result>>
    replicate_quorum_ack(model::term_id term, model::record_batch&& batch) {
        return _c->replicate(
          term,
          model::make_memory_record_batch_reader(std::move(batch)),
          raft::replicate_options{raft::consistency_level::quorum_ack});
    }
};

} // namespace cluster
