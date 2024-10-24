/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cloud_storage/fwd.h"
#include "cloud_storage/topic_mount_handler.h"
#include "cluster/data_migration_table.h"
#include "cluster/shard_table.h"
#include "container/chunked_hash_map.h"
#include "data_migration_types.h"
#include "fwd.h"
#include "ssx/semaphore.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

namespace cluster::data_migrations {

/*
 * Cluster-wide coordinator for migrations,
 * as well as node coordinator for local partition-specific actions
 */
class backend {
public:
    backend(
      migrations_table& table,
      frontend& frontend,
      ss::sharded<worker>& worker,
      partition_leaders_table& leaders_table,
      topics_frontend& topics_frontend,
      topic_table& topic_table,
      shard_table& shard_table,
      std::optional<std::reference_wrapper<cloud_storage::remote>>
        _cloud_storage_api,
      ss::abort_source& as);

    ss::future<> start();
    ss::future<> stop();

private:
    struct work_scope {
        std::optional<state> sought_state;
        bool partition_work_needed = false;
        bool topic_work_needed = false;
    };
    struct topic_reconciliation_state {
        size_t idx_in_migration;
        chunked_hash_map<model::partition_id, std::vector<model::node_id>>
          outstanding_partitions; // for partition scoped ops
        bool topic_scoped_work_needed;
        bool topic_scoped_work_done;
        void clear();
    };
    using topic_map_t
      = chunked_hash_map<model::topic_namespace, topic_reconciliation_state>;
    struct migration_reconciliation_state {
        explicit migration_reconciliation_state(work_scope scope)
          : scope(scope) {}
        work_scope scope;
        topic_map_t outstanding_topics;
    };
    using migration_reconciliation_states_t
      = absl::flat_hash_map<id, migration_reconciliation_state>;

    struct replica_work_state {
        id migration_id;
        state sought_state;
        // shard may only be assigned if replica_status is can_run
        std::optional<seastar::shard_id> shard;
        migrated_replica_status status
          = migrated_replica_status::waiting_for_rpc;
        replica_work_state(id migration_id, state sought_state)
          : migration_id(migration_id)
          , sought_state(sought_state) {}
    };
    struct topic_work_result {
        model::topic_namespace nt;
        id migration;
        state sought_state;
        errc ec;
    };
    class topic_scoped_work_state {
        using self = topic_scoped_work_state;
        ss::abort_source _as;
        retry_chain_node _rcn;
        ss::shared_promise<errc> _promise;

    public:
        topic_scoped_work_state();
        ~topic_scoped_work_state();
        topic_scoped_work_state(const self&) = delete;
        topic_scoped_work_state(self&&) = delete;
        self& operator=(const self&) = delete;
        self& operator=(self&&) = delete;

        retry_chain_node& rcn();
        void set_value(errc ec);
        ss::future<errc> future();
    };
    using tsws_lwptr_t = ss::lw_shared_ptr<topic_scoped_work_state>;

private:
    /* loop management */
    ss::future<> loop_once();
    ss::future<> work_once();
    void wakeup();

    /* event handlers outside main loop */
    ss::future<> handle_raft0_leadership_update();
    ss::future<> handle_migration_update(id id);
    void handle_shard_update(
      const model::ntp& ntp, raft::group_id, std::optional<ss::shard_id> shard);

    /* RPC and raft0 actions */
    ss::future<> send_rpc(model::node_id node_id);
    ss::future<check_ntp_states_reply>
    check_ntp_states_locally(check_ntp_states_request&& req);
    void
    to_advance_if_done(migration_reconciliation_states_t::const_iterator it);
    ss::future<> advance(id migration_id, state sought_state);
    void spawn_advances();

    /* topic work */
    ss::future<> schedule_topic_work(model::topic_namespace nt);
    ss::future<topic_work_result>
    // also resulting future cannot throw when co_awaited
    do_topic_work(model::topic_namespace nt, topic_work tw) noexcept;
    ss::future<errc> do_topic_work(
      const model::topic_namespace& nt,
      state sought_state,
      const inbound_topic_work_info& itwi,
      tsws_lwptr_t tsws);
    ss::future<errc> do_topic_work(
      const model::topic_namespace& nt,
      state sought_state,
      const outbound_topic_work_info& otwi,
      tsws_lwptr_t tsws);
    ss::future<> abort_all_topic_work();
    /* topic work helpers */
    ss::future<errc> create_topic(
      const model::topic_namespace& local_nt,
      const std::optional<model::topic_namespace>& original_nt,
      const std::optional<cloud_storage_location>& storage_location,
      retry_chain_node& rcn);
    ss::future<errc> prepare_mount_topic(
      const model::topic_namespace& nt, retry_chain_node& rcn);
    ss::future<errc> confirm_mount_topic(
      const model::topic_namespace& nt, retry_chain_node& rcn);
    ss::future<errc>
    delete_topic(const model::topic_namespace& nt, retry_chain_node& rcn);
    ss::future<errc>
    unmount_topic(const model::topic_namespace& nt, retry_chain_node& rcn);
    ss::future<errc>
    do_unmount_topic(const model::topic_namespace& nt, retry_chain_node& rcn);

    /* communication with partition workers */
    void start_partition_work(
      const model::ntp& ntp, const replica_work_state& rwstate);
    void stop_partition_work(
      model::topic_namespace_view nt,
      model::partition_id partition_id,
      const replica_work_state& rwstate);
    void
    on_partition_work_completed(model::ntp&& ntp, id migration, state state);

    /* deferred event handlers */
    // call only with _mutex lock grabbed
    ss::future<> process_delta(cluster::topic_table_ntp_delta&& delta);

    /* helpers */
    std::optional<backend::migration_reconciliation_states_t::iterator>
    get_rstate(id migration, state expected_sought_state);

    void update_partition_shard(
      const model::ntp& ntp,
      replica_work_state& rwstate,
      std::optional<ss::shard_id> new_shard);
    void mark_migration_step_done_for_ntp(
      migration_reconciliation_state& rs, const model::ntp& ntp);
    void mark_migration_step_done_for_nt(
      migration_reconciliation_state& rs, const model::topic_namespace& nt);
    ss::future<> drop_migration_reconciliation_rstate(
      migration_reconciliation_states_t::const_iterator rs_it);
    ss::future<> clear_tstate(const topic_map_t::value_type& topic_map_entry);
    void clear_tstate_belongings(
      const model::topic_namespace& nt,
      const topic_reconciliation_state& tstate);
    void erase_tstate_if_done(
      migration_reconciliation_state& mrstate, topic_map_t::iterator it);

    // call only with _mutex lock grabbed
    ss::future<> reconcile_migration(
      migration_reconciliation_state& mrstate,
      const migration_metadata& metadata);

    // call only with _mutex lock grabbed
    ss::future<> reconcile_topic(
      const id migration_id,
      size_t idx_in_migration,
      const model::topic_namespace& nt,
      migration_reconciliation_state& mrstate);

    // call only with _mutex lock grabbed
    ss::future<> reconcile_existing_topic(
      const model::topic_namespace& nt,
      topic_reconciliation_state& tstate,
      id migration,
      work_scope scope,
      bool schedule_local_partition_work);

    std::optional<std::reference_wrapper<replica_work_state>>
    get_replica_work_state(const model::ntp& ntp);
    bool has_local_replica(const model::ntp& ntp);

    inbound_partition_work_info get_partition_work_info(
      const model::ntp& ntp, const inbound_migration& im, id migration_id);
    outbound_partition_work_info get_partition_work_info(
      const model::ntp& ntp, const outbound_migration& om, id migration_id);
    partition_work_info get_partition_work_info(
      const model::ntp& ntp, const migration_metadata& metadata);

    inbound_topic_work_info get_topic_work_info(
      const model::topic_namespace& nt,
      const inbound_migration& im,
      id migration_id);
    outbound_topic_work_info get_topic_work_info(
      const model::topic_namespace& nt,
      const outbound_migration& om,
      id migration_id);
    topic_work_info get_topic_work_info(
      const model::topic_namespace& nt, const migration_metadata& metadata);

    template<class M>
    struct migration_direction_tag {};
    static work_scope get_work_scope(const migration_metadata& metadata);
    static work_scope get_work_scope(
      migration_direction_tag<inbound_migration>,
      const migration_metadata& metadata);
    static work_scope get_work_scope(
      migration_direction_tag<outbound_migration>,
      const migration_metadata& metadata);
    /*
     * Reconciliation-related data.
     *
     * When we are not the coordinator, _migration_states stores sought states
     * and topics only, but no partititons, _node_states, _nodes_to_retry and
     * _topic_work_to_retry are empty. The same applies to the migration states
     * with topic scoped work only needed.
     *
     * The following invariants can only be violated between tasks by a fiber
     * that has the lock.
     *
     * When we are the coordinator and need partition scoped work:
     * - _migration_states and _node_states store the same set of migration-ntp
     * combinations.
     * - _migration_states and _topic_migration_map store the same set of
     * migration-nt combinations.
     * - For each node there is no more than one RPC in flight at a time.
     * - For each topic there is no more than one topic work in flight.
     * - Nodes in _node_states = nodes in _nodes_to_retry ⊔ nodes of in-flight
     * RPCs.
     * - topics in _mrstates = topics in _topic_work_to_retry ⊔ topic work
     * in-flight
     *
     * - _advance_requests is only modified by the work cycle
     * - _migration_states, _node_states, _nodes_to_retry, _topic_migration_map
     * and _topic_work_to_retry are only modified under lock
     *
     * - _work_states only contains topics present in _mrstates
     */
    migration_reconciliation_states_t _migration_states;
    // reverse map for topics in mrstates
    using topic_migration_map_t = chunked_hash_map<model::topic_namespace, id>;
    topic_migration_map_t _topic_migration_map;
    using node_state = chunked_hash_map<model::ntp, id>;
    chunked_hash_map<model::node_id, node_state> _node_states;
    using deadline_t = model::timeout_clock::time_point;
    chunked_hash_map<model::node_id, deadline_t> _nodes_to_retry;
    chunked_hash_map<model::topic_namespace, deadline_t> _topic_work_to_retry;
    struct advance_info {
        state sought_state;
        bool sent = false;
        explicit advance_info(state sought_state)
          : sought_state(sought_state) {}
    };
    absl::flat_hash_map<id, advance_info> _advance_requests;
    chunked_vector<topic_table_ntp_delta> _unprocessed_deltas;
    chunked_hash_map<model::node_id, check_ntp_states_reply> _rpc_responses;

    /* Node-local data for partition-scoped work */
    using topic_work_state_t
      = chunked_hash_map<model::partition_id, replica_work_state>;
    chunked_hash_map<model::topic_namespace, topic_work_state_t>
      _local_work_states;
    /*
     * Topic-scoped work states for starting/stopping and disallowing concurrent
     * work on the same topic: similar to data_migrations::worker
     */
    chunked_vector<topic_work_result> _topic_work_results;
    chunked_hash_map<model::topic_namespace, tsws_lwptr_t>
      _active_topic_work_states; // no null pointers on scheduling points

    /* Refs to services etc */
    model::node_id _self;
    migrations_table& _table;
    frontend& _frontend;
    ss::sharded<worker>& _worker;
    partition_leaders_table& _leaders_table;
    topics_frontend& _topics_frontend;
    topic_table& _topic_table;
    shard_table& _shard_table;
    std::optional<std::reference_wrapper<cloud_storage::remote>>
      _cloud_storage_api;
    ss::abort_source& _as;

    std::unique_ptr<cloud_storage::topic_mount_handler> _topic_mount_handler;

    ss::gate _gate;
    ssx::semaphore _sem{0, "c/data-migration-be"};
    mutex _mutex{"c/data-migration-be::lock"};
    ss::timer<ss::lowres_clock> _timer{[this]() { wakeup(); }};

    bool _is_raft0_leader;
    bool _is_coordinator;
    migrations_table::notification_id _table_notification_id;
    cluster::notification_id_type _plt_raft0_leadership_notification_id;
    cluster::notification_id_type _topic_table_notification_id;
    cluster::notification_id_type _shard_notification_id;

    friend irpc_frontend;
};
} // namespace cluster::data_migrations
