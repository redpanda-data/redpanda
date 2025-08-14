/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/flat_hash_map.h"
#include "cluster_link/deps.h"
#include "cluster_link/link.h"
#include "cluster_link/logger.h"
#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "cluster_link/types.h"
#include "container/chunked_vector.h"
#include "kafka/data/rpc/deps.h"
#include "model/fundamental.h"
#include "ssx/work_queue.h"
#include "utils/mutex.h"

namespace cluster_link {

/**
 * @brief Class used to manage cluster links
 *
 * This class will be notified of changes in Redpanda to create, modify, or
 * remove cluster links and to handle leadership changes of partitions.
 */
class manager {
public:
    manager(
      ::model::node_id self,
      std::unique_ptr<kafka::data::rpc::partition_leader_cache>
        partition_leader_cache,
      std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager,
      std::unique_ptr<kafka::data::rpc::topic_metadata_cache>
        topic_metadata_cache,
      std::unique_ptr<link_registry> registry,
      std::unique_ptr<link_factory> link_factory,
      std::unique_ptr<cluster_factory> cluster_factory,
      ss::lowres_clock::duration task_reconciler_interval);
    manager(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(const manager&) = delete;
    manager& operator=(manager&&) = delete;
    virtual ~manager() = default;

    ss::future<> start();
    ss::future<> stop();

    /// Used to notify that a cluster link has been updated
    void on_link_change(model::id_t id);
    /// Used to notify manager in a change of NTP leadership
    void
    handle_partition_state_change(::model::ntp ntp, ntp_leader is_ntp_leader);
    /// Handles creation and start of a link
    ss::future<> handle_on_link_change(model::id_t id);
    /// Handles leadership changes for a given NTP
    ss::future<> handle_on_leadership_change(::model::ntp, ntp_leader);
    /// Used to add a mirror topic to a cluster link
    ss::future<::cluster::cluster_link::errc>
    add_mirror_topic(model::id_t link_id, model::add_mirror_topic_cmd cmd);
    ss::future<::cluster::cluster_link::errc> update_mirror_topic_state(
      model::id_t link_id, model::update_mirror_topic_state_cmd cmd);
    ss::future<::cluster::cluster_link::errc> update_mirror_topic_properties(
      model::id_t link_id, model::update_mirror_topic_properties_cmd cmd);

    std::optional<
      chunked_hash_map<::model::topic, model::mirror_topic_metadata>>
    get_mirror_topics_for_link(model::id_t id) const;

    /// Registers a task factory that will be used to create tasks when links
    /// are created
    template<typename T, typename... Args>
    ss::future<> register_task_factory(Args&&... args) {
        auto fut = co_await ss::coroutine::as_future(
          _link_task_reconciler_mutex.get_units(_as));
        if (fut.failed()) {
            auto ex = fut.get_exception();
            if (ssx::is_shutdown_exception(ex)) {
                vlog(
                  cllog.info,
                  "Task factory registration skipped due to shutdown");
            } else {
                vlog(cllog.error, "Task factory registration failed: {}", ex);
            }
            co_return;
        }
        _task_factories.emplace_back(
          std::make_unique<T>(std::forward<Args>(args)...));
    }

    model::cluster_link_task_status_report get_task_status_report() const;

    kafka::data::rpc::topic_metadata_cache& topic_metadata_cache() noexcept;

    kafka::data::rpc::partition_leader_cache& partition_leader_cache() noexcept;

    const kafka::data::rpc::partition_leader_cache&
    partition_leader_cache() const noexcept;

    kafka::data::rpc::partition_manager& partition_manager() noexcept;

    const kafka::data::rpc::partition_manager&
    partition_manager() const noexcept;

private:
    /// Called periodically to reconcile registered tasks on created links
    ss::future<> link_task_reconciler();

private:
    ::model::node_id _self;
    std::unique_ptr<kafka::data::rpc::partition_leader_cache>
      _partition_leader_cache;
    std::unique_ptr<kafka::data::rpc::partition_manager> _partition_manager;
    std::unique_ptr<kafka::data::rpc::topic_metadata_cache>
      _topic_metadata_cache;
    std::unique_ptr<link_registry> _registry;
    std::unique_ptr<link_factory> _link_factory;
    std::unique_ptr<cluster_factory> _cluster_factory;
    ssx::work_queue _queue;

    chunked_vector<std::unique_ptr<task_factory>> _task_factories;
    absl::flat_hash_map<id_t, std::unique_ptr<link>> _links;

    ss::lowres_clock::duration _task_reconciler_interval;
    mutex _link_task_reconciler_mutex{
      "cluster_link::manager::link_task_reconciler"};
    ss::timer<ss::lowres_clock> _link_task_reconciler_timer;
    ss::abort_source _as;
    ss::gate _g;
};
} // namespace cluster_link
