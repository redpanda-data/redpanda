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

namespace cluster {

/// Topic table represent all topics configuration and partition assignments.
/// The topic table is copied on each core to minimize cross core communication
/// when requesting topic information. The topics table provides an API for
/// Kafka requests and delta API for controller backend. The delta API allows
/// backend to wait for changes in topics table. Topics table is update directly
/// from controller_stm. The table is always updated before any actions related
/// with topic creation or deletion are executed. Topic table is also
/// responsible for commiting or removing pending allocations
///

class topic_table {
public:
    enum class in_progress_state {
        update_requested,
        cancel_requested,
        force_cancel_requested
    };
    /**
     * Replicas revision map is used to track revision of brokers in a replica
     * set. When a node is added into replica set its gets the revision assigned
     */
    using replicas_revision_map
      = absl::flat_hash_map<model::node_id, model::revision_id>;

    class in_progress_update {
    public:
        explicit in_progress_update(
          std::vector<model::broker_shard> previous_replicas,
          std::vector<model::broker_shard> result_replicas,
          in_progress_state state,
          model::revision_id update_revision,
          replicas_revision_map replicas_revisions,
          topic_table_probe& probe)
          : _previous_replicas(std::move(previous_replicas))
          , _result_replicas(std::move(result_replicas))
          , _state(state)
          , _update_revision(update_revision)
          , _replicas_revisions(std::move(replicas_revisions))
          , _probe(probe) {
            _probe.handle_update(_previous_replicas, _result_replicas);
        }

        ~in_progress_update() {
            _probe.handle_update_finish(_previous_replicas, _result_replicas);
            if (
              _state == in_progress_state::cancel_requested
              || _state == in_progress_state::force_cancel_requested) {
                _probe.handle_update_cancel_finish(
                  _previous_replicas, _result_replicas);
                ;
            }
        }

        in_progress_update(const in_progress_update&) = delete;
        in_progress_update(in_progress_update&&) = default;
        in_progress_update& operator=(const in_progress_update&) = delete;
        in_progress_update& operator=(in_progress_update&&) = delete;

        const in_progress_state& get_state() const { return _state; }

        void set_state(in_progress_state state) {
            if (
              _state == in_progress_state::update_requested
              && (state == in_progress_state::cancel_requested || state == in_progress_state::force_cancel_requested)) {
                _probe.handle_update_cancel(
                  _previous_replicas, _result_replicas);
            }
            _state = state;
        }

        const std::vector<model::broker_shard>& get_previous_replicas() const {
            return _previous_replicas;
        }

        const replicas_revision_map& get_replicas_revisions() const {
            return _replicas_revisions;
        }

        const model::revision_id& get_update_revision() const {
            return _update_revision;
        }

    private:
        std::vector<model::broker_shard> _previous_replicas;
        std::vector<model::broker_shard> _result_replicas;
        in_progress_state _state;
        model::revision_id _update_revision;
        replicas_revision_map _replicas_revisions;
        topic_table_probe& _probe;
    };

    struct topic_metadata_item {
        topic_metadata metadata;
        // replicas revisions for each partition
        absl::node_hash_map<model::partition_id, replicas_revision_map>
          replica_revisions;

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

    using delta_cb_t
      = ss::noncopyable_function<void(const std::vector<delta>&)>;

    explicit topic_table(ss::sharded<partition_manager>&);

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
    ss::future<> stop();

    /// Delta API
    /// NOTE: This API should only be consumed by a single entity, unless
    /// careful consideration is taken. This is because once notifications are
    /// fired, all events are consumed, and if both waiters aren't enqueued in
    /// the \ref _waiters collection by the time the notify occurs, only one
    /// waiter will recieve the updates, leaving the second one to observe
    /// skipped events upon recieving its subsequent notification.
    ss::future<std::vector<delta>> wait_for_changes(ss::abort_source&);

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

    const absl::node_hash_map<model::ntp, in_progress_update>&
    in_progress_updates() const {
        return _updates_in_progress;
    }

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

private:
    friend topic_table_probe;

    struct waiter {
        explicit waiter(uint64_t id)
          : id(id) {}
        ss::promise<std::vector<delta>> promise;
        ss::abort_source::subscription sub;
        uint64_t id;
    };
    void deallocate_topic_partitions(const std::vector<partition_assignment>&);

    void notify_waiters();

    template<typename Func>
    std::vector<std::invoke_result_t<Func, const topic_metadata_item&>>
    transform_topics(Func&&) const;

    underlying_t _topics;
    hierarchy_t _topics_hierarchy;

    updates_t _updates_in_progress;

    std::vector<delta> _pending_deltas;
    std::vector<std::unique_ptr<waiter>> _waiters;
    cluster::notification_id_type _notification_id{0};
    std::vector<std::pair<cluster::notification_id_type, delta_cb_t>>
      _notifications;
    uint64_t _waiter_id{0};
    model::offset _last_consumed_by_notifier{
      model::model_limits<model::offset>::min()};
    topic_table_probe _probe;
    ss::sharded<partition_manager>& _pm;
};

std::ostream& operator<<(std::ostream&, topic_table::in_progress_state);
} // namespace cluster
