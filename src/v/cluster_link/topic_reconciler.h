/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#pragma once

#include "cluster_link/deps.h"
#include "kafka/data/rpc/deps.h"

#include <seastar/core/gate.hh>

namespace cluster_link {

/**
 * @brief Responsible for picking up updates to mirror topics and applying them
 */
class topic_reconciler {
public:
    topic_reconciler(
      kafka::data::rpc::topic_creator* topic_creator,
      kafka::data::rpc::topic_metadata_cache* topic_metadata_cache,
      link_registry* link_registry,
      ss::lowres_clock::duration run_interval,
      config::binding<int16_t> default_topic_replication,
      ss::scheduling_group sg);
    topic_reconciler(const topic_reconciler&) = delete;
    topic_reconciler(topic_reconciler&&) = delete;
    topic_reconciler& operator=(const topic_reconciler&) = delete;
    topic_reconciler& operator=(topic_reconciler&&) = delete;
    virtual ~topic_reconciler() = default;

    ss::future<> start();
    ss::future<> stop() noexcept;

    /// Used to trigger the reconciler to run immediately when there's a change
    /// to the link
    void trigger(model::id_t link_id);

private:
    /// Is called either periodically or when a link is changed.  If the link_id
    /// is provided, then it will only reconcile that link, otherwise it will
    /// attempt to reconciler all links.
    ss::future<> execute(std::optional<model::id_t> link_id = std::nullopt);

    ss::future<> do_reconcile_topic(model::id_t link_id);

    ss::future<> maybe_update_existing_topic(
      model::id_t link_id,
      ::model::topic topic,
      model::mirror_topic_metadata mirror_topic_config,
      cluster::topic_configuration topic_configuration);

    ss::future<> maybe_create_mirror_topic(
      ::model::topic topic, model::mirror_topic_metadata mirror_topic_config);

    /// Checks the source topic configuration against the existing mirror topic
    std::optional<cluster::topic_properties_update>
    maybe_create_update_mirror_topic(
      const ::model::topic& topic_name,
      const cluster::topic_configuration& local_topic_config,
      const model::mirror_topic_metadata& mirror_topic_config);

    std::optional<ss::sstring> check_if_source_topic_is_invalid(
      const cluster::topic_configuration& local_topic_config,
      const model::mirror_topic_metadata& mirror_topic_config);

private:
    kafka::data::rpc::topic_creator* _topic_creator;
    kafka::data::rpc::topic_metadata_cache* _topic_metadata_cache;
    link_registry* _link_registry;

    ssx::mutex _reconciler_mutex{"cluster_link::task_reconciler"};
    ss::timer<ss::lowres_clock> _reconciler_timer;
    ss::lowres_clock::duration _run_interval;
    config::binding<int16_t> _default_topic_replication;
    ss::scheduling_group _scheduling_group;
    ss::abort_source _as;
    ss::gate _gate;
};
} // namespace cluster_link
