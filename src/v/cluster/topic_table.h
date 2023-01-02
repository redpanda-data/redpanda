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

#include "cluster/commands.h"
#include "cluster/non_replicable_topics_frontend.h"
#include "cluster/topic_table_probe.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/limits.h"
#include "model/metadata.h"
#include "utils/expiring_promise.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <span>
#include <type_traits>

namespace cluster {
/**
 * ## Overview
 *
 * Topic table represent all topics configuration and partition assignments.
 * The topic table is copied on each core to minimize cross core communication
 * when requesting topic information. The topics table provides an API for
 * Kafka requests and delta API for controller backend. The delta API allows
 * backend to wait for changes in topics table. Topics table is update directly
 * from controller_stm. The table is always updated before any actions related
 * with topic creation or deletion are executed.
 *
 * Topic table is updated through topic_updates_dispatcher.
 *
 *
 * ## Partition movement state machine
 *
 * When partition is being moved from replica set A to replica set B it may
 * either be finished or cancelled. The cancellation on the other hand may be
 * reverted. The partition movement state transitions are explained in the
 * following diagram:
 *
 * A - replica set A
 * B - replica set B
 *
 * ┌──────────┐   move           ┌──────────┐       finish         ┌──────────┐
 * │  static  │                  │  moving  │                      │  static  │
 * │          ├─────────────────►│          ├─────────────────────►│          │
 * │    A     │                  │  A -> B  │                      │    B     │
 * └──────────┘                  └────┬─────┘                      └──────────┘
 *      ▲                             │                                  ▲
 *      │                             │                                  │
 *      │                             │ cancel/force_cancel              │
 *      │                             │                                  │
 *      │                             │                                  │
 *      │                             ▼                                  │
 *      │                        ┌──────────┐                            │
 *      │        finish          │  moving  │     revert_cancel          │
 *      └────────────────────────┤          ├────────────────────────────┘
 *                               │  B -> A  │
 *                               └──────────┴─────┐
 *                                    ▲           │
 *                                    │           │
 *                                    │           │
 *                                    │           │
 *                                    └───────────┘
 *                                        force_cancel
 */

class topic_table {
public:
    enum class topic_state { exists, not_exists, indeterminate };

    // Guide to various partition revisions (i.e. offsets of the corresponding
    // commands in the controller log), presented in the chronological order:
    // * topic creation revision
    // * revision of the command (topic creation or partition movement) that
    //   caused a partition replica to appear on a given node (note that the
    //   partition can move away from a node and come back again and each time
    //   revision will be different). This revision appears in:
    //   * replicas_revision_map
    //   * partition::get_ntp_config().get_revision()
    //   * the partition directory name
    // * revision of the last applied command that caused raft reconfiguration
    //   (can be reconfiguration itself or cancellation). This revision appears
    //   in:
    //   * in_progress_update::get_last_cmd_revision()
    //   * partition::get_revision_id()
    //   * raft::group_configuration::revision_id()

    class in_progress_update {
    public:
        explicit in_progress_update(
          std::vector<model::broker_shard> previous_replicas,
          std::vector<model::broker_shard> target_replicas,
          reconfiguration_state state,
          model::revision_id update_revision,
          topic_table_probe& probe)
          : _previous_replicas(std::move(previous_replicas))
          , _target_replicas(std::move(target_replicas))
          , _state(state)
          , _update_revision(update_revision)
          , _last_cmd_revision(update_revision)
          , _probe(probe) {
            _probe.handle_update(_previous_replicas, _target_replicas);
        }

        ~in_progress_update() {
            _probe.handle_update_finish(_previous_replicas, _target_replicas);
            if (
              _state == reconfiguration_state::cancelled
              || _state == reconfiguration_state::force_cancelled) {
                _probe.handle_update_cancel_finish(
                  _previous_replicas, _target_replicas);
                ;
            }
        }

        in_progress_update(const in_progress_update&) = delete;
        in_progress_update(in_progress_update&&) = default;
        in_progress_update& operator=(const in_progress_update&) = delete;
        in_progress_update& operator=(in_progress_update&&) = delete;

        const reconfiguration_state& get_state() const { return _state; }

        void set_state(reconfiguration_state state, model::revision_id rev) {
            if (
              _state == reconfiguration_state::in_progress
              && (state == reconfiguration_state::cancelled || state == reconfiguration_state::force_cancelled)) {
                _probe.handle_update_cancel(
                  _previous_replicas, _target_replicas);
            }
            _state = state;
            _last_cmd_revision = rev;
        }

        const std::vector<model::broker_shard>& get_previous_replicas() const {
            return _previous_replicas;
        }
        const std::vector<model::broker_shard>& get_target_replicas() const {
            return _target_replicas;
        }

        const model::revision_id& get_update_revision() const {
            return _update_revision;
        }

        const model::revision_id& get_last_cmd_revision() const {
            return _last_cmd_revision;
        }

    private:
        std::vector<model::broker_shard> _previous_replicas;
        std::vector<model::broker_shard> _target_replicas;
        reconfiguration_state _state;
        model::revision_id _update_revision;
        model::revision_id _last_cmd_revision;
        topic_table_probe& _probe;
    };

    struct partition_meta {
        /// Replica revision map reflecting *only the finished partition
        /// updates* (i.e. it only gets updated when the update_finished command
        /// is processed).
        replicas_revision_map replicas_revisions;
        /// Revision id of the last applied update_finished controller command
        /// (or of addition command if none)
        model::revision_id last_update_finished_revision;
    };

    struct topic_metadata_item {
        topic_metadata metadata;
        absl::node_hash_map<model::partition_id, partition_meta> partitions;

        bool is_topic_replicable() const {
            return metadata.is_topic_replicable();
        }

        assignments_set& get_assignments() {
            return metadata.get_assignments();
        }

        const assignments_set& get_assignments() const {
            return metadata.get_assignments();
        }
        model::revision_id get_revision() const {
            return metadata.get_revision();
        }
        std::optional<model::initial_revision_id> get_remote_revision() const {
            return metadata.get_remote_revision();
        }
        const model::topic& get_source_topic() const {
            return metadata.get_source_topic();
        }

        const topic_configuration& get_configuration() const {
            return metadata.get_configuration();
        }
        topic_configuration& get_configuration() {
            return metadata.get_configuration();
        }

        replication_factor get_replication_factor() const {
            return metadata.get_replication_factor();
        }
    };

    using delta = topic_table_delta;

    using underlying_t = absl::node_hash_map<
      model::topic_namespace,
      topic_metadata_item,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;
    using hierarchy_t = absl::node_hash_map<
      model::topic_namespace,
      absl::flat_hash_set<model::topic_namespace>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    using delta_range_t
      = boost::iterator_range<fragmented_vector<delta>::const_iterator>;
    using delta_cb_t = ss::noncopyable_function<void(delta_range_t)>;

    explicit topic_table()
      : _probe(*this){};

    cluster::notification_id_type register_delta_notification(delta_cb_t cb) {
        auto id = _notification_id++;
        _notifications.emplace_back(id, std::move(cb));
        return id;
    }

    void unregister_delta_notification(cluster::notification_id_type id) {
        std::erase_if(
          _notifications,
          [id](const std::pair<cluster::notification_id_type, delta_cb_t>& n) {
              return n.first == id;
          });
    }

    using updates_t = absl::node_hash_map<model::ntp, in_progress_update>;

    bool is_batch_applicable(const model::record_batch& b) const {
        return b.header().type
               == model::record_batch_type::topic_management_cmd;
    }

    // list of commands that this table is able to apply, the list is used to
    // automatically deserialize batch into command
    static constexpr auto accepted_commands = make_commands_list<
      create_topic_cmd,
      delete_topic_cmd,
      move_partition_replicas_cmd,
      finish_moving_partition_replicas_cmd,
      update_topic_properties_cmd,
      create_partition_cmd,
      create_non_replicable_topic_cmd>{};

    /// State machine applies
    ss::future<std::error_code> apply(create_topic_cmd, model::offset);
    ss::future<std::error_code> apply(delete_topic_cmd, model::offset);
    ss::future<std::error_code>
      apply(move_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(finish_moving_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(update_topic_properties_cmd, model::offset);
    ss::future<std::error_code> apply(create_partition_cmd, model::offset);
    ss::future<std::error_code>
      apply(create_non_replicable_topic_cmd, model::offset);
    ss::future<std::error_code>
      apply(cancel_moving_partition_replicas_cmd, model::offset);
    ss::future<std::error_code> apply(move_topic_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(revert_cancel_partition_move_cmd, model::offset);

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<>
    apply_snapshot(model::offset snapshot_offset, const controller_snapshot&);

    ss::future<> stop();

    /// Delta API
    /// NOTE: This API should only be consumed by a single entity, unless
    /// careful consideration is taken. This is because once notifications are
    /// fired, all events are consumed, and if both waiters aren't enqueued in
    /// the \ref _waiters collection by the time the notify occurs, only one
    /// waiter will recieve the updates, leaving the second one to observe
    /// skipped events upon recieving its subsequent notification.
    ss::future<fragmented_vector<delta>> wait_for_changes(ss::abort_source&);

    bool has_pending_changes() const { return !_pending_deltas.empty(); }

    /// Query API

    /// Returns list of all topics that exists in the cluster.
    std::vector<model::topic_namespace> all_topics() const;

    // Returns the number of topics that exist in the cluster.
    size_t all_topics_count() const;

    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_metadata>
      get_topic_metadata(model::topic_namespace_view) const;

    ///\brief Returns reference to metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    ///
    /// IMPORTANT: remember not to use the reference when its lifetime has to
    /// span across multiple scheduling point. Reference returning method is
    /// provided not to copy metadata object when used by synchronous parts of
    /// code
    std::optional<std::reference_wrapper<const topic_metadata>>
      get_topic_metadata_ref(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_configuration>
      get_topic_cfg(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<replication_factor>
      get_topic_replication_factor(model::topic_namespace_view) const;

    ///\brief Returns partition assignments of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<assignments_set>
      get_topic_assignments(model::topic_namespace_view) const;

    ///\brief Returns topics timestamp type
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::timestamp_type>
      get_topic_timestamp_type(model::topic_namespace_view) const;

    /// Returns metadata of all topics.
    const underlying_t& all_topics_metadata() const;

    /// Checks if it has given partition
    bool contains(model::topic_namespace_view, model::partition_id) const;
    /// Checks if it has given topic
    bool contains(model::topic_namespace_view tp) const {
        return _topics.contains(tp);
    }
    /// contains() check with stronger validation on the topic revision/offset.
    /// Just looking up in the cache can yield false negatives if the cache is
    /// not warmed up because controller replay can be in progress. This variant
    /// of contains factors in such false negatives by incorporating a check on
    /// the topic revision. Topic revision is the topic creation command offset
    /// in
    //  the controller log.
    ///
    /// There are 3 possibilities.
    /// 1. id > last applied offset, not enough information
    /// 2. id <= last_applied offset && not in cache
    /// 3. id <= last_applied offset && in cache.
    ///
    /// Callers must note that false positives are still possible because of any
    /// inflight pending deltas (yet to be applied) that delete the topic.
    topic_state
    get_topic_state(model::topic_namespace_view, model::revision_id id) const;

    std::optional<partition_assignment>
    get_partition_assignment(const model::ntp&) const;

    const underlying_t& topics_map() const { return _topics; }

    const hierarchy_t& hierarchy_map() const { return _topics_hierarchy; }

    bool is_update_in_progress(const model::ntp&) const;

    bool has_updates_in_progress() const {
        return !_updates_in_progress.empty();
    }

    const updates_t& updates_in_progress() const {
        return _updates_in_progress;
    }

    ///\brief Returns initial revision id of the topic
    std::optional<model::initial_revision_id>
    get_initial_revision(model::topic_namespace_view tp) const;

    ///\brief Returns initial revision id of the partition
    std::optional<model::initial_revision_id>
    get_initial_revision(const model::ntp& ntp) const;

    /**
     * returns previous replica set of partition if partition is currently being
     * reconfigured. For reconfiguration from [1,2,3] to [2,3,4] this method
     * will return [1,2,3].
     */
    std::optional<std::vector<model::broker_shard>>
    get_previous_replica_set(const model::ntp&) const;
    /**
     * returns target replica set of partition if partition is currently being
     * reconfigured. For reconfiguration from [1,2,3] to [2,3,4] this method
     * will return [2,3,4].
     */
    std::optional<std::vector<model::broker_shard>>
    get_target_replica_set(const model::ntp&) const;

    /**
     * Lists all NTPs that replicas are being move to a node
     */
    std::vector<model::ntp> ntps_moving_to_node(model::node_id) const;

    /**
     * Lists all NTPs that replicas are being move from a node
     */
    std::vector<model::ntp> ntps_moving_from_node(model::node_id) const;

    /**
     * Lists all ntps moving either from or to a node
     */
    std::vector<model::ntp> all_ntps_moving_per_node(model::node_id) const;

    std::vector<model::ntp> all_updates_in_progress() const;

    model::revision_id last_applied_revision() const {
        return _last_applied_revision_id;
    }

    // does not include non-replicable partitions
    size_t partition_count() const { return _partition_count; }

    /**
     * Returns number of partitions allocated on given node
     */
    size_t get_node_partition_count(model::node_id) const;

private:
    friend topic_table_probe;

    struct waiter {
        explicit waiter(uint64_t id)
          : id(id) {}
        ss::promise<fragmented_vector<delta>> promise;
        ss::abort_source::subscription sub;
        uint64_t id;
    };

    void notify_waiters();

    template<typename Func>
    std::vector<std::invoke_result_t<Func, const topic_metadata_item&>>
    transform_topics(Func&&) const;

    void change_partition_replicas(
      model::ntp ntp,
      const std::vector<model::broker_shard>& new_assignment,
      topic_metadata_item& metadata,
      partition_assignment& current_assignment,
      model::offset o);

    class snapshot_applier;

    underlying_t _topics;
    hierarchy_t _topics_hierarchy;
    size_t _partition_count{0};

    updates_t _updates_in_progress;
    model::revision_id _last_applied_revision_id;

    fragmented_vector<delta> _pending_deltas;
    std::vector<std::unique_ptr<waiter>> _waiters;
    cluster::notification_id_type _notification_id{0};
    std::vector<std::pair<cluster::notification_id_type, delta_cb_t>>
      _notifications;
    uint64_t _waiter_id{0};
    std::vector<delta>::difference_type _last_consumed_by_notifier_offset{0};
    topic_table_probe _probe;

    friend class topic_table_partition_generator;
};

} // namespace cluster
