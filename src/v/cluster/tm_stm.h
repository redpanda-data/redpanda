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

#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/state_machine_registry.h"
#include "cluster/tm_stm_cache.h"
#include "cluster/tx_hash_ranges.h"
#include "container/fragmented_vector.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/fwd.h"
#include "raft/persisted_stm.h"
#include "storage/ntp_config.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <string_view>

namespace cluster {

class tm_stm;

class txlock_unit {
    tm_stm* _stm;
    kafka::transactional_id _id;
    std::string_view _name;
    bool _is_locked;
    ssx::semaphore_units _units;

    txlock_unit(
      tm_stm* stm,
      ssx::semaphore_units&& units,
      kafka::transactional_id id,
      std::string_view name) noexcept
      : _stm(stm)
      , _id(id)
      , _name(name) {
        _is_locked = (bool)units;
        _units = std::move(units);
        vassert(_is_locked, "units must be initialized");
        vlog(txlog.trace, "got_lock name:{}, tx_id:{}", _name, _id);
    }

    friend class tm_stm;

public:
    txlock_unit(txlock_unit&& token) noexcept
      : _stm(token._stm)
      , _id(token._id)
      , _name(token._name)
      , _is_locked(token._is_locked)
      , _units(std::move(token._units)) {
        token._stm = nullptr;
        token._is_locked = false;
    }
    txlock_unit(const txlock_unit&) = delete;
    txlock_unit(const txlock_unit&&) = delete;
    txlock_unit() = delete;

    bool return_all() {
        if (_is_locked) {
            _units.return_all();
            _is_locked = false;
            vlog(txlog.trace, "released_lock name:{}, tx_id:{}", _name, _id);
            return true;
        }
        return false;
    }

    txlock_unit& operator=(txlock_unit&& other) noexcept {
        if (this != &other) {
            _stm = other._stm;
            _units = std::move(other._units);
            _id = other._id;
            _name = other._name;
            other._stm = nullptr;
            other._is_locked = false;
        }
        return *this;
    }

    ~txlock_unit() noexcept;
};

/**
 * TM stands for transaction manager (2PC slang for a transaction
 * coordinator). It's a state machine which maintains stats of the
 * ongoing and executed transactions and maps tx.id to its latest
 * session (producer_identity)
 */
class tm_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "tm_stm";
    using clock_type = ss::lowres_system_clock;

    enum op_status {
        success,
        not_found,
        conflict,
        unknown,
        not_leader,
        partition_not_found,
        timeout
    };

    struct draining_txs
      : serde::
          envelope<draining_txs, serde::version<0>, serde::compat_version<0>> {
        repartitioning_id id;
        tx_hash_ranges_set ranges{};
        absl::btree_set<kafka::transactional_id> transactions{};

        draining_txs() = default;

        draining_txs(
          repartitioning_id id,
          tx_hash_ranges_set ranges,
          absl::btree_set<kafka::transactional_id> txs)
          : id(id)
          , ranges(std::move(ranges))
          , transactions(std::move(txs)) {}

        friend bool operator==(const draining_txs&, const draining_txs&)
          = default;

        auto serde_fields() { return std::tie(id, ranges, transactions); }

        friend std::ostream&
        operator<<(std::ostream& o, const draining_txs& txes) {
            fmt::print(
              o,
              "{{ id: {}, ranges: {}, transactions: {} }}",
              txes.id,
              txes.ranges,
              txes.transactions.size());
            return o;
        }
    };

    // this struct is basicly the same as other hosted_txs but we can't
    // unify them in minor release because locally_hosted_txs already is
    // being persisted to disk and `inited` doesn't make sense in other
    // contexts (tx_registry & rpc types)
    struct locally_hosted_txs
      : serde::envelope<
          locally_hosted_txs,
          serde::version<0>,
          serde::compat_version<0>> {
        bool inited{false};
        tx_hash_ranges_set hash_ranges{};
        absl::btree_set<kafka::transactional_id> excluded_transactions{};
        absl::btree_set<kafka::transactional_id> included_transactions{};
        draining_txs draining{};

        locally_hosted_txs() = default;

        locally_hosted_txs(
          bool inited,
          tx_hash_ranges_set hr,
          absl::btree_set<kafka::transactional_id> et,
          absl::btree_set<kafka::transactional_id> it,
          draining_txs dr)
          : inited(inited)
          , hash_ranges(std::move(hr))
          , excluded_transactions(std::move(et))
          , included_transactions(std::move(it))
          , draining(std::move(dr)) {}

        friend bool
        operator==(const locally_hosted_txs&, const locally_hosted_txs&)
          = default;

        auto serde_fields() {
            return std::tie(
              inited,
              hash_ranges,
              excluded_transactions,
              included_transactions,
              draining);
        }

        friend std::ostream&
        operator<<(std::ostream& o, const locally_hosted_txs& txes) {
            fmt::print(
              o,
              "{{ inited: {}, hash ranges: {}, excluded: {}, included: {}, "
              "draining: {} }}",
              txes.inited,
              txes.hash_ranges,
              txes.excluded_transactions.size(),
              txes.included_transactions.size(),
              txes.draining);
            return o;
        }
    };

    struct tm_snapshot_v0 {
        static constexpr uint8_t version = 0;

        model::offset offset;
        fragmented_vector<tx_metadata> transactions;
    };

    struct tm_snapshot {
        static constexpr uint8_t version = 1;

        model::offset offset;
        fragmented_vector<tx_metadata> transactions;
        locally_hosted_txs hash_ranges;
    };

    explicit tm_stm(
      ss::logger&,
      raft::consensus*,
      ss::sharded<features::feature_table>&,
      ss::lw_shared_ptr<cluster::tm_stm_cache>);

    void try_rm_lock(const kafka::transactional_id& tid) {
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

    ss::future<checked<tx_metadata, tm_stm::op_status>>
      get_tx(kafka::transactional_id);
    ss::future<checked<tx_metadata, tm_stm::op_status>>
      mark_tx_ongoing(model::term_id, kafka::transactional_id);
    ss::future<tm_stm::op_status> add_partitions(
      model::term_id,
      kafka::transactional_id,
      std::vector<tx_metadata::tx_partition>);
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
    uint8_t active_snapshot_version();

    ss::future<> checkpoint_ongoing_txs();

    ss::future<ss::basic_rwlock<>::holder> prepare_transfer_leadership();

    ss::future<checked<tx_metadata, tm_stm::op_status>>
      reset_transferring(model::term_id, kafka::transactional_id);
    ss::future<checked<tx_metadata, tm_stm::op_status>>
      mark_tx_aborting(model::term_id, kafka::transactional_id);
    ss::future<checked<tx_metadata, tm_stm::op_status>>
      mark_tx_prepared(model::term_id, kafka::transactional_id);
    ss::future<checked<tx_metadata, tm_stm::op_status>>
      mark_tx_killed(model::term_id, kafka::transactional_id);
    // todo: cleanup last_pid and rolled_pid. It seems like they are doing
    // the same thing but in practice they are not. last_pid is not updated
    // in all cases whereas rolled_pid is need to cleanup all the state
    // from previous epochs.
    ss::future<tm_stm::op_status> re_register_producer(
      model::term_id,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity pid_to_register,
      model::producer_identity last_pid,
      model::producer_identity rolled_pid);
    ss::future<tm_stm::op_status> register_new_producer(
      model::term_id,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity);
    ss::future<tm_stm::op_status>
      expire_tx(model::term_id, kafka::transactional_id);

    bool is_expired(const tx_metadata&);

    // before calling a tm_stm modifying operation a caller should
    // take get_tx_lock mutex
    ss::lw_shared_ptr<mutex> get_tx_lock(kafka::transactional_id);

    ss::future<txlock_unit> lock_tx(kafka::transactional_id, std::string_view);

    std::optional<txlock_unit>
      try_lock_tx(kafka::transactional_id, std::string_view);

    absl::btree_set<kafka::transactional_id> get_expired_txs();

    using get_txs_result
      = checked<fragmented_vector<tx_metadata>, tm_stm::op_status>;
    ss::future<get_txs_result> get_all_transactions();

    ss::future<checked<tx_metadata, tm_stm::op_status>>
    delete_partition_from_tx(
      model::term_id term,
      kafka::transactional_id tid,
      tx_metadata::tx_partition ntp);

    ss::future<checked<tx_metadata, tm_stm::op_status>>
      update_tx(tx_metadata, model::term_id);

    model::partition_id get_partition() const {
        return _raft->ntp().tp.partition;
    }

    mutex& get_tx_thrashing_lock() { return _tx_thrashing_lock; }

    size_t tx_cache_size() const;

    std::optional<tx_metadata> oldest_tx() const;
    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

protected:
    ss::future<> apply_raft_snapshot(const iobuf&) final;

private:
    std::optional<tx_metadata> find_tx(const kafka::transactional_id&);
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot> take_local_snapshot() override;

    ss::future<> apply(const model::record_batch& b) final;

    ss::future<>
    apply_tm_update(model::record_batch_header hdr, model::record_batch b);

    ss::future<checked<model::term_id, tm_stm::op_status>> do_barrier();
    ss::future<checked<model::term_id, tm_stm::op_status>>
      do_sync(model::timeout_clock::duration);
    ss::future<checked<tx_metadata, tm_stm::op_status>>
      do_update_tx(tx_metadata, model::term_id);

    ss::future<tm_stm::op_status> do_register_new_producer(
      model::term_id,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::producer_identity);
    ss::future<raft::stm_snapshot> do_take_snapshot();
    ss::future<result<raft::replicate_result>>
      quorum_write_empty_batch(model::timeout_clock::time_point);

    ss::future<result<raft::replicate_result>>
    replicate_quorum_ack(model::term_id term, model::record_batch&& batch);

    model::record_batch serialize_tx(tx_metadata tx);

private:
    std::chrono::milliseconds _sync_timeout;
    config::binding<std::chrono::milliseconds> _transactional_id_expiration;
    absl::flat_hash_map<model::producer_identity, kafka::transactional_id>
      _pid_tx_id;
    absl::flat_hash_map<kafka::transactional_id, ss::lw_shared_ptr<mutex>>
      _tx_locks;
    ss::sharded<features::feature_table>& _feature_table;
    ss::lw_shared_ptr<cluster::tm_stm_cache> _cache;

    mutex _tx_thrashing_lock{"tm_stm::tx_thrashing_lock"};
    prefix_logger _ctx_log;
};

inline txlock_unit::~txlock_unit() noexcept {
    return_all();
    if (_stm) {
        _stm->try_rm_lock(_id);
        _stm = nullptr;
    }
}

class tm_stm_factory : public state_machine_factory {
public:
    tm_stm_factory(
      ss::sharded<tm_stm_cache_manager>&,
      ss::sharded<features::feature_table>&);
    bool is_applicable_for(const storage::ntp_config& raft) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;

private:
    ss::sharded<tm_stm_cache_manager>& _tm_stm_cache_manager;
    ss::sharded<features::feature_table>& _feature_table;
};

} // namespace cluster

namespace reflection {
template<>
struct adl<cluster::tm_stm::draining_txs> {
    void to(iobuf& out, cluster::tm_stm::draining_txs&& dr) {
        reflection::serialize(out, dr.id, dr.ranges, dr.transactions);
    }
    cluster::tm_stm::draining_txs from(iobuf_parser& in) {
        auto id = reflection::adl<cluster::repartitioning_id>{}.from(in);
        auto ranges
          = reflection::adl<std::vector<cluster::tx_hash_range>>{}.from(in);
        auto txs = reflection::adl<absl::btree_set<kafka::transactional_id>>{}
                     .from(in);
        return {id, std::move(ranges), std::move(txs)};
    }
};

template<>
struct adl<cluster::tm_stm::locally_hosted_txs> {
    void to(iobuf& out, cluster::tm_stm::locally_hosted_txs&& hr) {
        reflection::serialize(
          out,
          hr.inited,
          hr.hash_ranges,
          hr.excluded_transactions,
          hr.included_transactions,
          hr.draining);
    }
    cluster::tm_stm::locally_hosted_txs from(iobuf_parser& in) {
        auto inited = reflection::adl<bool>{}.from(in);

        auto hash_ranges_set
          = reflection::adl<cluster::tx_hash_ranges_set>{}.from(in);
        auto included_transactions
          = reflection::adl<absl::btree_set<kafka::transactional_id>>{}.from(
            in);
        auto excluded_transactions
          = reflection::adl<absl::btree_set<kafka::transactional_id>>{}.from(
            in);
        auto draining = reflection::adl<cluster::tm_stm::draining_txs>{}.from(
          in);
        return {
          inited,
          std::move(hash_ranges_set),
          std::move(included_transactions),
          std::move(excluded_transactions),
          std::move(draining)};
    };
};

} // namespace reflection
