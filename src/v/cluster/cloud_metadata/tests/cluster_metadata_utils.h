/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/commands.h"
#include "cluster/partition.h"
#include "cluster/tests/topic_properties_generator.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "security/acl.h"
#include "storage/types.h"

// Test utilities to facilitate creating metadata.

namespace cluster::cloud_metadata {

inline security::license get_test_license() {
    const char* sample_valid_license = std::getenv("REDPANDA_SAMPLE_LICENSE");
    vassert(
      sample_valid_license != nullptr,
      "Expected REDPANDA_SAMPLE_LICENSE to be set");
    const ss::sstring license_str{sample_valid_license};
    return security::make_license(license_str);
}

inline security::acl_binding binding_for_user(const ss::sstring& user) {
    const security::acl_principal principal{
      security::principal_type::ephemeral_user, user};
    security::acl_entry acl_entry{
      principal,
      security::acl_host::wildcard_host(),
      security::acl_operation::all,
      security::acl_permission::allow};
    auto binding = security::acl_binding{
      security::resource_pattern{
        security::resource_type::topic,
        security::resource_pattern::wildcard,
        security::pattern_type::literal},
      acl_entry};
    return binding;
}

inline topic_properties uploadable_topic_properties() {
    auto props = random_topic_properties();
    if (
      !props.shadow_indexing.has_value()
      || !is_archival_enabled(props.shadow_indexing.value())) {
        props.shadow_indexing.emplace(model::shadow_indexing_mode::full);
    }
    // Remote topic properties should only be set for recovery topics.
    props.remote_topic_properties = std::nullopt;
    props.recovery = false;
    props.read_replica = false;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    return props;
}

inline topic_properties non_remote_topic_properties() {
    auto props = random_topic_properties();
    props.shadow_indexing = model::shadow_indexing_mode::disabled;
    props.recovery = false;
    props.read_replica = false;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    return props;
}

// Reads the given partition, returning the sequence of recovery stages
// recorded in its log.
inline ss::future<std::vector<cluster::recovery_stage>>
read_recovery_stages(cluster::partition& controller_prt) {
    storage::log_reader_config reader_config(
      model::offset(0),
      controller_prt.raft()->committed_offset(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      model::record_batch_type::cluster_recovery_cmd,
      std::nullopt,
      std::nullopt,
      std::nullopt);
    auto reader = controller_prt.make_reader(reader_config).get();
    auto batches = co_await model::consume_reader_to_memory(
      std::move(reader), model::timeout_clock::time_point::max());
    std::vector<cluster::recovery_stage> stages;
    for (auto& b : batches) {
        auto cmd = co_await cluster::deserialize(
          std::move(b),
          cluster::make_commands_list<
            cluster::cluster_recovery_update_cmd,
            cluster::cluster_recovery_init_cmd>());
        ss::visit(
          cmd,
          [&](const cluster::cluster_recovery_update_cmd& cmd) {
              stages.emplace_back(cmd.value.stage);
          },
          [&](const cluster::cluster_recovery_init_cmd&) {
              stages.emplace_back(cluster::recovery_stage::initialized);
          });
    }
    co_return stages;
}

} // namespace cluster::cloud_metadata
