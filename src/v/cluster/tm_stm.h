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
#include "cluster/tm_stm_cache.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "raft/types.h"
#include "storage/snapshot.h"
#include "utils/expiring_promise.h"
#include "utils/fragmented_vector.h"
#include "utils/mutex.h"

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

#include <compare>

namespace cluster {

using use_tx_version_with_last_pid_bool
  = ss::bool_class<struct use_tx_version_with_last_pid_tag>;

struct tm_snapshot {
    model::offset offset;
    fragmented_vector<tm_transaction> transactions;
};

/**
 * TM stands for transaction manager (2PC slang for a transaction
 * coordinator). It's a state machine which maintains stats of the
 * ongoing and executed transactions and maps tx.id to its latest
 * session (producer_identity)
 */
class tm_stm final : public persisted_stm {
public:
    using clock_type = ss::lowres_system_clock;
    static constexpr const int8_t supported_version = 0;

    enum op_status {
        success,
        not_found,
        conflict,
        unknown,
        not_leader,
        partition_not_found,
        timeout
    };

    explicit tm_stm(
      ss::logger&,
      raft::consensus*,
      ss::sharded<features::feature_table>&,
      ss::lw_shared_ptr<cluster::tm_stm_cache>);

    void try_rm_lock(kafka::transactional_id tid) {
        auto tx_opt = _cache->find_mem(tid);
        if (tx_opt) {
            return;
        }
        tx_opt = _cache->find_log(tid);
        if (tx_opt) {
            return;
        }
        if (_tx_locks.contains(tid)) {
            auto lock = _tx_locks[tid];
            if (lock->ready()) {
                _tx_locks.erase(tid);
            }
        }
    };

    ss::gate& gate() { return _gate; }

    ss::future<> start() override;

    ss::future<checked<tm_transaction, tm_stm::op_status>>
      get_tx(kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      mark_tx_ongoing(model::term_id, kafka::transactional_id);
    ss::future<tm_stm::op_status> add_partitions(
      model::term_id,
      kafka::transactional_id,
      std::vector<tm_transaction::tx_partition>);
    ss::future<tm_stm::op_status> add_group(
      model::term_id, kafka::transactional_id, kafka::group_id, model::term_id);
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

    ss::future<checked<model::term_id, tm_stm::op_status>> barrier();
    ss::future<checked<model::term_id, tm_stm::op_status>>
      sync(model::timeout_clock::duration);
    ss::future<checked<model::term_id, tm_stm::op_status>> sync() {
        return sync(_sync_timeout);
    }

    ss::future<ss::basic_rwlock<>::holder> read_lock() {
        return _cache->read_lock();
    }

    ss::future<> checkpoint_ongoing_txs();

    ss::future<ss::basic_rwlock<>::holder> prepare_transfer_leadership();

    ss::future<checked<tm_transaction, tm_stm::op_status>>
      reset_transferring(model::term_id, kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      mark_tx_preparing(model::term_id, kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      mark_tx_aborting(model::term_id, kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      mark_tx_prepared(model::term_id, kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      mark_tx_killed(model::term_id, kafka::transactional_id);
    ss::future<tm_stm::op_status> re_register_producer(
      model::term_id,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity,
      model::producer_identity);
    ss::future<tm_stm::op_status> register_new_producer(
      model::term_id,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity);
    ss::future<tm_stm::op_status>
      expire_tx(model::term_id, kafka::transactional_id);

    bool is_expired(const tm_transaction&);

    // before calling a tm_stm modifying operation a caller should
    // take get_tx_lock mutex
    ss::lw_shared_ptr<mutex> get_tx_lock(kafka::transactional_id tid) {
        auto [lock_it, inserted] = _tx_locks.try_emplace(tid, nullptr);
        if (inserted) {
            lock_it->second = ss::make_lw_shared<mutex>();
        }
        return lock_it->second;
    }

    absl::btree_set<kafka::transactional_id> get_expired_txs();

    using get_txs_result
      = checked<fragmented_vector<tm_transaction>, tm_stm::op_status>;
    ss::future<get_txs_result> get_all_transactions();

    ss::future<checked<tm_transaction, tm_stm::op_status>>
    delete_partition_from_tx(
      model::term_id term,
      kafka::transactional_id tid,
      tm_transaction::tx_partition ntp);

    ss::future<checked<tm_transaction, tm_stm::op_status>>
      update_tx(tm_transaction, model::term_id);

    model::partition_id get_partition() const {
        return _raft->ntp().tp.partition;
    }

protected:
    ss::future<> handle_eviction() override;

private:
    std::optional<tm_transaction> find_tx(kafka::transactional_id);
    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;

    std::chrono::milliseconds _sync_timeout;
    std::chrono::milliseconds _transactional_id_expiration;
    absl::flat_hash_map<model::producer_identity, kafka::transactional_id>
      _pid_tx_id;
    absl::flat_hash_map<kafka::transactional_id, ss::lw_shared_ptr<mutex>>
      _tx_locks;
    ss::sharded<features::feature_table>& _feature_table;
    ss::lw_shared_ptr<cluster::tm_stm_cache> _cache;

    ss::future<> apply(model::record_batch b) override;

    ss::future<checked<model::term_id, tm_stm::op_status>> do_barrier();
    ss::future<checked<model::term_id, tm_stm::op_status>>
      do_sync(model::timeout_clock::duration);
    ss::future<checked<tm_transaction, tm_stm::op_status>>
      do_update_tx(tm_transaction, model::term_id);
    ss::future<tm_stm::op_status> do_register_new_producer(
      model::term_id,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity);
    ss::future<stm_snapshot> do_take_snapshot();

    ss::future<result<raft::replicate_result>>
    replicate_quorum_ack(model::term_id term, model::record_batch&& batch) {
        return _c->replicate(
          term,
          model::make_memory_record_batch_reader(std::move(batch)),
          raft::replicate_options{raft::consistency_level::quorum_ack});
    }

    bool is_transaction_ga() {
        return _feature_table.local().is_active(
          features::feature::transaction_ga);
    }

    use_tx_version_with_last_pid_bool use_new_tx_version() {
        return is_transaction_ga() ? use_tx_version_with_last_pid_bool::yes
                                   : use_tx_version_with_last_pid_bool::no;
    }

    model::record_batch serialize_tx(tm_transaction tx);
};

} // namespace cluster
