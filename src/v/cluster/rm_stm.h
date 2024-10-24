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
#include "cluster/rm_stm_types.h"
#include "cluster/state_machine_registry.h"
#include "cluster/topic_table.h"
#include "cluster/tx_utils.h"
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

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <string_view>
#include <system_error>

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
 *
 * There are 3 types of producer requests this state machine handles
 *
 * 1. Regular produce requests (non idempotent, non transactional) - These are
 * just delegated to the underlying raft instance.
 *
 * 2. Idempotent requests - Provides single session idempotency guarantees. Each
 * idempotent produce request is associated with a producer_identity
 * (producer_id + epoch=N). Idempotency is implemented by tracking sequence
 * numbers of last 5 inflight/processed requests and ensuring that the new
 * requests maintain the sequence order. Client stamps the record batches with
 * sequence numbers. Idempotent producer may choose to increment epoch on the
 * client side to reset the sequence tracking when it deems safe
 * (check kip-360). The state machine detects such situations and resets the
 * tracked sequence number state.
 *
 * 3. Transactional requests - Provides EOS semantics across multiple sessions
 * by implementing fencing as defined in the Kafka protocol. Transactional
 * requests are also idempotent by design and are associated with a transaction
 * (application) id in the client config. Transaction id is mapped to a
 * producer_identity with epoch getting bumped every time a new instance of
 * transactional client registers. This ensures that only one active
 * transactional producer is active, the one with the latest epoch.
 *
 * Both idempotent and transactional producers are associated with a
 * producer_state instance and the state machine tracks the list of all active
 * producer_states producing to this partition.
 *
 * Notes on locking in this stm:
 *
 * There are two main locks in use here.
 * The locks are always grabbed in that order.
 * 1. (stm)_state_lock : This is a state machine wide (global) lock acquired by
 * all operations like producers and snapshots. Producers acquire it in read
 * (shared) mode while operations like snapshots that require a consistent view
 * of the state machine acquire it in write (exclusive) mode.
 *
 * 2. producer_state_lock: This is transparently grabbed in the invocations of
 * run_with_lock method of the producer but the scope of the lock is different
 * for idempotent and transactional producers.

 * - Idempotent requests hold the lock until the requested cannot be reordered
 * at the raft layer (so the sequence is not violated in the physical log) and
 * released soon after to let concurrent idempotent requests make progress.
 *
 * - Transactional requests hold the lock for the duration of the request and
 * until the changes are reflected in the state machine via apply. So the scope
 * of the lock is larger.
 *
 * Notes on producer_state eviction:
 *
 * Kafka protocol has no notion of session termination. For example, an
 * idempotent producer may just stop producing and the broker wouldn't know
 * whether it is ever going to come back again. This forces the implementation
 * to have an eviction policy for producers so we do not just accumulate
 * them forever. This is currently based on a configurable count
 * (max_concurrent_producer_ids), enforced by the producer_state_manager
 * at the shard level. While this state machine owns the producer_state
 * instances, the producer_state_manager manges a shard wide LRU
 * list of producers and notifies the stm when the limits are breached. See
 * producer_state class for details on when a producer can be evicted.
 *
 * Notes on transaction expiry:

 * This stm periodically checks if there is any pending transaction for
 * expiration. The expiration kicks in the transaction is not committed/aborted
 * within the user set transaction timeout. A producer with an active
 * transaction cannot be evicted, so exipration ensures that with timely
 * expiration of open transactions, the producer states are candidates for
 * eviction.
 */
class rm_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "rm_stm";

    using producers_t = chunked_hash_map<model::producer_id, tx::producer_ptr>;

    explicit rm_stm(
      ss::logger&,
      raft::consensus*,
      ss::sharded<cluster::tx_gateway_frontend>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<tx::producer_state_manager>&,
      std::optional<model::vcluster_id>);

    ss::future<checked<model::term_id, tx::errc>> begin_tx(
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<tx::errc> commit_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<tx::errc> abort_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    /**
     * Returns the next after the last one decided. If there are no ongoing
     * transactions this will return next offset to be applied to the the stm.
     */
    model::offset last_stable_offset();
    ss::future<fragmented_vector<tx::tx_range>>
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
        return storage::stm_type::user_topic_transactional;
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

    ss::future<result<tx::partition_transactions>> get_transactions();

    ss::future<tx::errc> mark_expired(model::producer_identity pid);

    ss::future<> remove_persistent_state() override;

    uint64_t get_local_snapshot_size() const override;

    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

    const producers_t& get_producers() const { return _producers; }

    ss::future<tx::errc> abort_all_txes();

protected:
    ss::future<> apply_raft_snapshot(const iobuf&) final;

private:
    void setup_metrics();
    ss::future<> do_remove_persistent_state();
    ss::future<fragmented_vector<tx::tx_range>>
      do_aborted_transactions(model::offset, model::offset);

    // Tells whether the producer is already known or is created
    // for the first time from the incoming request.
    using producer_previously_known
      = ss::bool_class<struct new_producer_created_tag>;
    std::pair<tx::producer_ptr, producer_previously_known>
      maybe_create_producer(model::producer_identity);
    void cleanup_producer_state(model::producer_identity);
    ss::future<> reset_producers();
    ss::future<checked<model::term_id, tx::errc>> do_begin_tx(
      model::term_id,
      model::producer_identity pid,
      tx::producer_ptr,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<tx::errc> do_commit_tx(
      model::term_id synced_term,
      tx::producer_ptr,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<tx::errc> do_abort_tx(
      model::term_id,
      tx::producer_ptr,
      std::optional<model::tx_seq>,
      model::timeout_clock::duration);
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) override;
    ss::future<raft::stm_snapshot>
    do_take_local_snapshot(uint8_t version, ssx::semaphore_units apply_units);
    ss::future<std::optional<tx::abort_snapshot>>
      load_abort_snapshot(tx::abort_index);
    ss::future<> save_abort_snapshot(tx::abort_snapshot);

    ss::future<result<kafka_result>> do_replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<result<kafka_result>> transactional_replicate(
      model::batch_identity, model::record_batch_reader);

    ss::future<result<kafka_result>> transactional_replicate(
      model::term_id,
      tx::producer_ptr,
      model::batch_identity,
      model::record_batch_reader);

    ss::future<result<kafka_result>> do_transactional_replicate(
      model::term_id,
      tx::producer_ptr,
      model::batch_identity,
      model::record_batch_reader);

    ss::future<result<kafka_result>> idempotent_replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<result<kafka_result>> do_idempotent_replicate(
      model::term_id,
      tx::producer_ptr,
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>,
      ssx::semaphore_units&,
      producer_previously_known);

    ss::future<result<kafka_result>> idempotent_replicate(
      model::term_id,
      tx::producer_ptr,
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>,
      ssx::semaphore_units,
      producer_previously_known);

    ss::future<result<kafka_result>> replicate_msg(
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<bool> sync(model::timeout_clock::duration);
    constexpr bool check_tx_permitted() { return true; }

    void abort_old_txes();
    ss::future<std::chrono::milliseconds> do_abort_old_txes();
    ss::future<tx::errc> try_abort_old_tx(tx::producer_ptr);
    ss::future<tx::errc> do_try_abort_old_tx(tx::producer_ptr);
    void maybe_rearm_autoabort_timer(tx::time_point_type);

    abort_origin get_abort_origin(tx::producer_ptr, model::tx_seq) const;

    ss::future<> do_apply(const model::record_batch&) override;
    void apply_fence(model::producer_identity, model::record_batch);
    void apply_control(model::producer_identity, model::control_record_type);
    void apply_data(model::batch_identity, const model::record_batch_header&);

    ss::future<> reduce_aborted_list();

    /**
     * Return when the committed offset has been established when STM starts.
     */
    ss::future<model::offset> bootstrap_committed_offset();

    // Populated from state machine up calls.
    struct aborted_tx_state {
        fragmented_vector<tx::tx_range> aborted;
        fragmented_vector<tx::abort_index> abort_indexes;
        tx::abort_snapshot last_abort_snapshot{.last = model::offset(-1)};
    };

    kafka::offset from_log_offset(model::offset old_offset) const;
    model::offset to_log_offset(kafka::offset new_offset) const;

    std::optional<int32_t> get_seq_number(model::producer_identity pid) const;

    // Returns a list of expired producers and the next smallest pending
    // expiration duration or ms::max() if no transaction exists. The latter
    // is used to rearm the expiration timer accordingly.
    std::pair<chunked_vector<tx::producer_ptr>, std::chrono::milliseconds>
    get_expired_producers() const;

    uint8_t active_snapshot_version();

    friend std::ostream& operator<<(std::ostream&, const aborted_tx_state&);

    // Defines the commit offset range for the stm bootstrap.
    // Set on first apply upcall and used to identify if the
    // stm is still replaying the log.
    std::optional<model::offset> _bootstrap_committed_offset;
    ss::basic_rwlock<> _state_lock;
    bool _is_abort_idx_reduction_requested{false};
    aborted_tx_state _aborted_tx_state;
    ss::timer<tx::clock_type> auto_abort_timer;
    config::binding<std::chrono::milliseconds> _sync_timeout;
    std::chrono::milliseconds _tx_timeout_delay;
    std::chrono::milliseconds _abort_interval_ms;
    uint32_t _abort_index_segment_size;
    bool _is_autoabort_enabled{true};
    bool _is_tx_enabled{false};
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    storage::snapshot_manager _abort_snapshot_mgr;
    absl::flat_hash_map<std::pair<model::offset, model::offset>, uint64_t>
      _abort_snapshot_sizes{};
    ss::sharded<features::feature_table>& _feature_table;
    prefix_logger _ctx_log;
    ss::sharded<tx::producer_state_manager>& _producer_state_manager;
    std::optional<model::vcluster_id> _vcluster_id;

    // All the producers producing to this transaction.
    // There are two types of producers we track here
    // 1. idempotent
    // 2. transactions
    // All other produce requests do not have an associated producer_id.
    // A producer is uniquely identified by the tuple  <producer_id, epoch>.
    // For idempotent producers epoch is never used and is always 0, so a
    // producer_id uniquely identifies the producer instance.
    //
    // for transactional producers, epoch is used to fence older instances of
    // of the the same producer_id. A transactional client maps the client
    // side property transactional.id to it's corresponding producer_id using
    // init_producer_id request and the coordinator ensures fencing by only
    // retaining the latest epoch for a given producer_id.
    //
    // A list of open transactions is maintained below for convenience.
    //
    // The map maints only latest epoch for a given producer_id.
    // This helps implement fencing which ensures that only the latest epoch for
    // a given producer_id remains active. This also works for idempotent
    // producers because epoch is unused.
    producers_t _producers;

    // All the producers with open transactions in this partition.
    // The list is sorted by the open transaction begin offset, so
    // the first entry in the list is the earliest open transaction
    // on the partition. This property is used to efficiently compute
    // the LSO without having traverse the whole list.
    using active_transactional_producers_t = counted_intrusive_list<
      tx::producer_state,
      &tx::producer_state::_active_transaction_hook>;
    active_transactional_producers_t _active_tx_producers;

    metrics::internal_metric_groups _metrics;
    ss::abort_source _as;
    ss::gate _gate;
    // Highest producer ID applied to this stm.
    model::producer_id _highest_producer_id;
    // for monotonicity of computed LSO.
    model::offset _last_known_lso{-1};

    friend struct ::rm_stm_test_fixture;
};

class rm_stm_factory : public state_machine_factory {
public:
    rm_stm_factory(
      bool enable_transactions,
      bool enable_idempotence,
      ss::sharded<tx_gateway_frontend>&,
      ss::sharded<cluster::tx::producer_state_manager>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::topic_table>&);
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;

private:
    bool _enable_transactions;
    bool _enable_idempotence;
    ss::sharded<tx_gateway_frontend>& _tx_gateway_frontend;
    ss::sharded<cluster::tx::producer_state_manager>& _producer_state_manager;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<topic_table>& _topics;
};

} // namespace cluster
