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
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_mount_handler.h"
#include "cluster/data_migration_group_proxy.h"
#include "cluster/data_migration_router.h"
#include "cluster/data_migration_table.h"
#include "cluster/errc.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "container/chunked_hash_map.h"
#include "data_migration_types.h"
#include "fwd.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "ssx/mutex.h"
#include "ssx/semaphore.h"

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
      router& router,
      ss::sharded<worker>& worker,
      partition_leaders_table& leaders_table,
      topics_frontend& topics_frontend,
      topic_table& topic_table,
      shard_table& shard_table,
      ss::shared_ptr<group_proxy> group_proxy,
      std::optional<std::reference_wrapper<cloud_storage::remote>>
        _cloud_storage_api,
      std::optional<std::reference_wrapper<cloud_storage::topic_mount_handler>>
        _topic_mount_handler,
      ss::abort_source& as);

    ss::future<> start();
    ss::future<> stop();

    ss::future<result<entities_status, errc>>
    get_entities_status(id migration_id);

    ss::future<errc>
    set_entities_status(id migration_id, entities_status status);

private:
    struct work_scope {
        std::optional<state> sought_state;
        bool data_partition_work_needed = false;
        bool co_partition_work_needed = false;
        bool topic_work_needed = false;
        // true if the work requires entity state to be updated before
        // proceeding to the next step
        bool needs_entity_state_update = false;

        // true if the work requires waiting for all partition work to finish
        // before proceeding to the next step
        bool wait_for_partition_work_to_finish = false;

        bool any_partition_work_needed() const {
            return data_partition_work_needed || co_partition_work_needed;
        };
        bool partition_work_needed(const model::topic_namespace& nt) const {
            return nt == model::kafka_consumer_offsets_nt
                     ? co_partition_work_needed
                     : data_partition_work_needed;
        };
    };
    struct topic_reconciliation_state {
        // will be invalid for consumer offsets topic
        // as it was not explicitly requested in the migration as a topic
        size_t idx_in_migration;
        chunked_hash_map<model::partition_id, std::vector<model::node_id>>
          outstanding_partitions; // for partition scoped ops
        bool topic_scoped_work_needed;
        bool topic_scoped_work_done;
        bool all_partitions_ready() const {
            return outstanding_partitions.empty();
        }
        void clear();
    };
    using topic_map_t = chunked_hash_map<
      model::topic_namespace,
      topic_reconciliation_state,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;
    using partition_consumer_group_map_t
      = chunked_hash_map<model::partition_id, chunked_vector<consumer_group>>;
    struct migration_reconciliation_state {
        explicit migration_reconciliation_state(
          work_scope scope, model::revision_id revision_id)
          : scope(scope)
          , revision_id(revision_id)
          , entities_ready(!scope.needs_entity_state_update) {}
        work_scope scope;
        // comes from the same raft0 record as `scope`
        model::revision_id revision_id;
        topic_map_t outstanding_topics;
        bool entities_ready;
        // may not stay unfilled between scheduling points
        std::optional<partition_consumer_group_map_t> partition_group_map;
    };
    using migration_reconciliation_states_t
      = absl::flat_hash_map<id, migration_reconciliation_state>;
    using mrstate_it_t = migration_reconciliation_states_t::iterator;
    using mrstate_cit_t = migration_reconciliation_states_t::const_iterator;

    struct replica_work_state {
        state sought_state;
        // shard may only be assigned if replica_status is `can_run`
        std::optional<seastar::shard_id> shard;
        migrated_replica_status status;
        // empty if status is `waiting_for_rpc`; otherwise it has a value
        // which is in sync with `sought_state`, i.e. they come from the same
        // raft0 record creating or updating the migration
        std::optional<model::revision_id> revision_id;

        replica_work_state(
          state sought_state,
          std::optional<model::revision_id> revision_id,
          migrated_replica_status status)
          : sought_state(sought_state)
          , status(status)
          , revision_id(revision_id) {
            vassert(
              (status != migrated_replica_status::waiting_for_controller_update)
                == this->revision_id.has_value(),
              "Inconsistent replica work state: sought_state {}, status {}, "
              "revision_id {}",
              sought_state,
              status,
              this->revision_id);
        }

        model::revision_id get_revision_id() const {
            vassert(
              status == migrated_replica_status::can_run
                && revision_id.has_value(),
              "Cannot get revision_id for replica work state in status {}",
              status);
            return *revision_id;
        }
    };

    friend std::ostream& operator<<(std::ostream&, const replica_work_state&);

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
        std::optional<cloud_storage::topic_manifest> _cached_topic_manifest;

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

        void cache_topic_manifest(cloud_storage::topic_manifest m) {
            _cached_topic_manifest = std::move(m);
        }

        const std::optional<cloud_storage::topic_manifest>&
        cached_topic_manifest() const {
            return _cached_topic_manifest;
        }

        std::optional<cloud_storage::topic_manifest>&&
        release_cached_topic_manifest() && {
            return std::move(_cached_topic_manifest);
        }
    };
    using tsws_lwptr_t = ss::lw_shared_ptr<topic_scoped_work_state>;

private:
    using partition_work_state_t = chunked_hash_map<id, replica_work_state>;
    using rwstate_entry = partition_work_state_t::value_type;
    struct topic_namespace_migration {
        model::topic_namespace nt;
        id migration;

        bool operator==(const topic_namespace_migration& other) const;

        template<class H>
        friend H AbslHashValue(H h, const topic_namespace_migration& tnm) {
            return H::combine(std::move(h), tnm.nt, tnm.migration);
        }
    };

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
    check_ntp_states_locally(check_ntp_states_request req);
    void to_advance_if_done(mrstate_cit_t it);
    ss::future<> advance(id migration_id, state sought_state);
    void spawn_advances();

    /* topic work */
    void schedule_topic_work_if_partitions_ready(
      const model::topic_namespace& nt, mrstate_cit_t rs_it);
    void schedule_topic_work(topic_namespace_migration tnm);
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
    ss::future<
      result<std::reference_wrapper<const cloud_storage::topic_manifest>, errc>>
    maybe_download_topic_manifest(
      const model::topic_namespace& nt,
      const std::optional<model::topic_namespace>& original_nt,
      const std::optional<cloud_storage_location>& storage_location,
      tsws_lwptr_t tsws);

    ss::future<errc> create_topic(
      const model::topic_namespace& local_nt,
      const std::optional<model::topic_namespace>& original_nt,
      const cloud_storage::topic_manifest& manifest,
      retry_chain_node& rcn);

    ss::future<errc> prepare_mount_topic(
      const model::topic_namespace& nt,
      const cloud_storage::topic_manifest& manifest,
      retry_chain_node& rcn);

    ss::future<errc> confirm_mount_topic(
      const model::topic_namespace& nt,
      const cloud_storage::topic_manifest& manifest,
      retry_chain_node& rcn);
    ss::future<errc>
    delete_topic(const model::topic_namespace& nt, retry_chain_node& rcn);

    ss::future<errc> unmount_not_existing_topic(
      const model::topic_namespace& nt,
      const cloud_storage::topic_manifest& manifest,
      retry_chain_node& rcn);

    ss::future<errc> do_unmount_not_existing_topic(
      const model::topic_namespace& nt,
      const cloud_storage::topic_manifest& manifest,
      retry_chain_node& rcn);

    ss::future<errc>
    unmount_topic(const model::topic_namespace& nt, retry_chain_node& rcn);
    ss::future<errc>
    do_unmount_topic(const model::topic_namespace& nt, retry_chain_node& rcn);

    /* communication with partition workers */
    void
    start_partition_work(const model::ntp& ntp, const rwstate_entry& rwstate);
    void
    stop_partition_work(const model::ntp ntp, const rwstate_entry& rwstate);
    void
    on_partition_work_completed(model::ntp&& ntp, id migration, state state);

    /* deferred event handlers */
    // call only with _mutex lock grabbed
    ss::future<> process_delta(cluster::topic_table_ntp_delta&& delta);

    /* helpers */
    std::optional<mrstate_it_t>
    get_rstate(id migration, state expected_sought_state);

    void update_partition_shard(
      const model::ntp& ntp,
      rwstate_entry& rwstate,
      std::optional<ss::shard_id> new_shard);
    void
    mark_migration_step_done_for_ntp(mrstate_it_t rs_it, const model::ntp& ntp);
    void mark_migration_step_done_for_nt(
      mrstate_it_t rs_it, const model::topic_namespace& nt);
    ss::future<> drop_migration_reconciliation_rstate(mrstate_cit_t rs_it);
    ss::future<> clear_tstate(
      id migration_id, const topic_map_t::value_type& topic_map_entry);
    void clear_tstate_belongings(
      const topic_namespace_migration& tnm,
      const topic_reconciliation_state& tstate);
    void remove_from_topic_migration_map(
      const model::topic_namespace& nt, id migration);
    void erase_tstate_if_done(mrstate_it_t rs_it, topic_map_t::iterator it);
    result<partition_consumer_group_map_t, errc>
    build_migration_group_map(const migration_metadata& metadata) const;

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
      model::revision_id revision_id,
      bool schedule_local_partition_work);

    std::optional<std::reference_wrapper<partition_work_state_t>>
    get_replica_work_states(const model::ntp& ntp);
    bool has_local_replica(const model::ntp& ntp);
    const inbound_topic& get_inbound_topic(
      const model::topic_namespace_view& nt,
      const inbound_migration& im,
      id migration_id) const;

    inbound_partition_work_info get_partition_work_info(
      const model::ntp& ntp,
      const inbound_migration& im,
      id migration_id) const;
    outbound_partition_work_info get_partition_work_info(
      const model::ntp& ntp,
      const outbound_migration& om,
      id migration_id) const;
    partition_work_info get_partition_work_info(
      const model::ntp& ntp, const migration_metadata& metadata) const;

    inbound_topic_work_info get_topic_work_info(
      const model::topic_namespace& nt,
      const inbound_migration& im,
      id migration_id) const;
    outbound_topic_work_info get_topic_work_info(
      const model::topic_namespace& nt,
      const outbound_migration& om,
      id migration_id) const;
    topic_work_info get_topic_work_info(
      const model::topic_namespace& nt,
      const migration_metadata& metadata) const;

    template<class M>
    struct migration_direction_tag {};
    static work_scope get_work_scope(const migration_metadata& metadata);
    static work_scope get_work_scope(
      migration_direction_tag<inbound_migration>,
      const migration_metadata& metadata);
    static work_scope get_work_scope(
      migration_direction_tag<outbound_migration>,
      const migration_metadata& metadata);

    // for a normal topic get all assignments,
    // for __consumer_groups get only those holding migrated groups
    chunked_vector<partition_assignment>
    get_topic_assignments(const model::topic_namespace& nt, const id id);

    /*
     * Reconciliation-related data.
     *
     * When we are not the coordinator, _migration_states stores sought states
     * and topics only, but no partitions, _node_states, _nodes_to_retry and
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
    using topic_migration_map_t
      = chunked_hash_map<model::topic_namespace, chunked_hash_set<id>>;
    topic_migration_map_t _topic_migration_map;

    struct ntp_migration {
        model::ntp ntp;
        id migration;

        bool operator==(const ntp_migration& other) const;

        template<class H>
        friend H AbslHashValue(H h, const ntp_migration& nm) {
            return H::combine(std::move(h), nm.ntp, nm.migration);
        };
    };

    using node_state = chunked_hash_set<ntp_migration>;
    chunked_hash_map<model::node_id, node_state> _node_states;
    using deadline_t = model::timeout_clock::time_point;
    chunked_hash_map<model::node_id, deadline_t> _nodes_to_retry;
    chunked_hash_map<topic_namespace_migration, deadline_t>
      _topic_work_to_retry;
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
      = chunked_hash_map<model::partition_id, partition_work_state_t>;
    chunked_hash_map<
      model::topic_namespace,
      topic_work_state_t,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
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
    router& _router;
    partition_leaders_table& _leaders_table;
    topics_frontend& _topics_frontend;
    topic_table& _topic_table;
    shard_table& _shard_table;
    ss::shared_ptr<group_proxy> _group_proxy;
    std::optional<std::reference_wrapper<cloud_storage::remote>>
      _cloud_storage_api;
    std::optional<std::reference_wrapper<cloud_storage::topic_mount_handler>>
      _topic_mount_handler;
    ss::abort_source& _as;

    ss::gate _gate;
    ssx::semaphore _sem{0, "c/data-migration-be"};
    ssx::mutex _mutex{"c/data-migration-be::lock"};
    ss::timer<ss::lowres_clock> _timer{[this]() { wakeup(); }};

    std::optional<model::term_id> _raft0_leader_term;
    std::optional<model::term_id> _coordinator_term;
    migrations_table::notification_id _table_notification_id;
    cluster::notification_id_type _plt_raft0_leadership_notification_id;
    cluster::notification_id_type _topic_table_notification_id;
    cluster::notification_id_type _shard_notification_id;

    friend irpc_frontend;
};
} // namespace cluster::data_migrations
