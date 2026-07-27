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

#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "kafka/client/cluster.h"

namespace cluster_link {
/**
 * @brief Task that synchronizes source topics with the cluster link
 *
 * This task queries the source cluster for available topics, filters them
 * using the configured topic name filter, and validates their accessibility.
 * It then retrieves topic properties from the source cluster and inserts
 * the topic along with its properties into the cluster link table.
 * A separate reconciler task later processes these entries and applies them
 * to the destination cluster.
 */
class source_topic_syncer : public controller_locked_task {
public:
    static constexpr auto task_name = "Source Topic Sync";
    static constexpr kafka::topic_authorized_operations required_permissions
      = kafka::topic_authorized_operations{
        0x508}; // DESCRIBE_CONFIG, DESCRIBE, READ
    source_topic_syncer(link* link, const model::metadata& config);
    source_topic_syncer(const source_topic_syncer&) = delete;
    source_topic_syncer(source_topic_syncer&&) = delete;
    source_topic_syncer& operator=(const source_topic_syncer&) = delete;
    source_topic_syncer& operator=(source_topic_syncer&&) = delete;
    ~source_topic_syncer() override = default;

    void update_config(const model::metadata& config) override;

    model::enabled_t is_enabled() const final;

protected:
    ss::future<state_transition> run_impl(ss::abort_source&) override;

private:
    struct topic_metadata {
        int32_t partition_count;
        std::optional<int16_t> rf;
        std::optional<::model::topic_id> topic_id;
    };

    using reconciler_commands = std::variant<
      model::add_mirror_topic_cmd,
      model::update_mirror_topic_properties_cmd,
      model::update_mirror_topic_status_cmd>;
    using reconciler_commands_vector = chunked_vector<reconciler_commands>;
    // Map of topics who are candidates for mirror topic creation
    using candidate_create_map
      = chunked_hash_map<::model::topic, topic_metadata>;
    // Map of topics who are candidates for updates, include the current
    // mirror_topic_metadata for cached properties
    using candidate_update_map = chunked_hash_map<
      ::model::topic,
      std::pair<topic_metadata, model::mirror_topic_metadata>>;
    using sr_is_empty_t = ss::bool_class<struct sr_is_empty_tag>;
    // Builds a list of topics that are candidates for creation
    candidate_create_map find_candidate_topics_for_creation(
      kafka::client::cluster& cluster, sr_is_empty_t sr_is_empty);
    // Builds a list of topics that are candidates for update
    candidate_update_map
    find_candidate_topics_for_update(kafka::client::cluster& cluster);
    // Enqueue the add mirror topic commands into the list of commands
    void enqueue_create_mirror_topic_commands(
      reconciler_commands_vector& commands,
      const candidate_create_map& candidates,
      const chunked_vector<kafka::describe_configs_result>& describe_results);
    // Enqueue the update mirror topic properties or update mirror topic state
    // commands
    void enqueue_update_mirror_topic_commands(
      reconciler_commands_vector& commands,
      const candidate_update_map& candidates,
      const chunked_vector<kafka::describe_configs_result>& describe_results);
    // Applies the link's storage mode override to `configs` when `topic` is
    // in scope of `storage_mode_override_filters`. Out-of-scope topics are
    // left untouched so they inherit the source's storage mode already
    // present in `configs`.
    void maybe_apply_storage_mode_override(
      ::model::topic_view topic,
      chunked_hash_map<ss::sstring, ss::sstring>& configs) const;
    // Execute the commands
    ss::future<> submit_commands(reconciler_commands_vector commands);
    ss::future<kafka::describe_configs_response> describe_topics(
      kafka::client::cluster& cluster,
      ::model::node_id controller_id,
      kafka::api_version describe_configs_version,
      const chunked_vector<::model::topic>& topics,
      const model::topic_metadata_mirroring_config::properties_set& configs);
    ss::future<sr_is_empty_t> check_if_schema_registry_is_empty();
    int16_t
    maybe_clamp_rf(int16_t source_rf, const ::model::topic& topic) noexcept;

private:
    model::topic_metadata_mirroring_config _config;
    model::schema_registry_sync_config _sr_config;
};

/**
 * @brief Factory used to create the source topic syncer task
 */
class source_topic_syncer_factory : public task_factory {
public:
    /// Returns the name of the task that this factory creates
    std::string_view created_task_name() const noexcept override;

    /// Creates a new task through the factory.  Provides the owning link
    std::unique_ptr<task> create_task(link* link) override;
};
} // namespace cluster_link
