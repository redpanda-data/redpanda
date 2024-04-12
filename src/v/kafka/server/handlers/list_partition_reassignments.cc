
// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/list_partition_reassignments.h"

#include "cluster/metadata_cache.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "config/node_config.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/list_partition_reassignments_request.h"
#include "kafka/protocol/schemata/list_partition_reassignments_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/fwd.h"
#include "model/namespace.h"

#include <algorithm>
#include <vector>

namespace kafka {

bool in_broker_list(
  const model::broker_shard& bshard,
  const std::vector<model::broker_shard>& list) {
    return std::find(list.begin(), list.end(), bshard) != list.end();
}

ongoing_partition_reassignment compare_replica_sets(
  cluster::partition_assignment& current_assignment,
  std::optional<std::vector<model::broker_shard>> previous_replica_set) {
    ongoing_partition_reassignment reassignments;

    std::for_each(
      current_assignment.replicas.cbegin(),
      current_assignment.replicas.cend(),
      [&reassignments,
       &previous_replica_set](const model::broker_shard& bshard) {
          reassignments.replicas.push_back(bshard.node_id);
          if (previous_replica_set.has_value()) {
              if (!in_broker_list(bshard, *previous_replica_set)) {
                  reassignments.adding_replicas.push_back(bshard.node_id);
              }
          }
      });

    if (previous_replica_set.has_value()) {
        std::for_each(
          previous_replica_set->cbegin(),
          previous_replica_set->cend(),
          [&reassignments,
           &current_assignment](const model::broker_shard& bshard) {
              if (!in_broker_list(bshard, current_assignment.replicas)) {
                  reassignments.removing_replicas.push_back(bshard.node_id);
              }
          });
    }

    return reassignments;
}

// Returns the partition reassignments for a given ntp
ongoing_partition_reassignment list_partition_reassignments(
  const cluster::metadata_cache& md_cache, const model::ntp& ntp) {
    // Current replica set is in the current partition assignment
    auto current_assignment = md_cache.get_partition_assignment(ntp);
    if (!current_assignment.has_value()) {
        vlog(klog.debug, "No replica set: ntp {}", ntp);
        return ongoing_partition_reassignment{};
    }

    return compare_replica_sets(
      *current_assignment, md_cache.get_previous_replica_set(ntp));
}

// Returns all reassignments currently in progress
std::vector<ongoing_topic_reassignment> all_in_progress_partition_reassignments(
  const cluster::metadata_cache& md_cache) {
    const auto& in_progress = md_cache.updates_in_progress();
    // The in progress reassignments are in a map: ntp -> list of brokers which
    // is not suitable to use with the ListPartitionReassignments response. So
    // we restructure them here.
    absl::flat_hash_map<model::topic, ongoing_topic_reassignment>
      reassignments_by_topic;

    for (const auto& [ntp, status] : in_progress) {
        auto current_assignment = md_cache.get_partition_assignment(ntp);
        if (!current_assignment.has_value()) {
            vlog(klog.debug, "No replica set: ntp {}", ntp);
            continue;
        }

        auto partition_reassignment = compare_replica_sets(
          *current_assignment, status.get_previous_replicas());
        partition_reassignment.partition_index = ntp.tp.partition;
        if (reassignments_by_topic.contains(ntp.tp.topic)) {
            reassignments_by_topic[ntp.tp.topic].partitions.push_back(
              std::move(partition_reassignment));
        } else {
            reassignments_by_topic[ntp.tp.topic] = ongoing_topic_reassignment{
              .name = ntp.tp.topic, .partitions = {partition_reassignment}};
        }
    }

    std::vector<ongoing_topic_reassignment> all_in_progress_reassignments;
    all_in_progress_reassignments.reserve(reassignments_by_topic.size());
    for (auto& [_, reassignments] : reassignments_by_topic) {
        all_in_progress_reassignments.push_back(std::move(reassignments));
    }

    return all_in_progress_reassignments;
}

template<>
ss::future<response_ptr> list_partition_reassignments_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    list_partition_reassignments_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    list_partition_reassignments_response resp;

    auto authz = ctx.authorized(
      security::acl_operation::describe, security::default_cluster_name);

    if (!ctx.audit()) {
        resp.data.error_code = error_code::broker_not_available;
        resp.data.error_message = "Broker not available - audit system failure";
        co_return co_await ctx.respond(std::move(resp));
    }

    if (!authz) {
        vlog(
          klog.debug,
          "Failed cluster authorization. Requires DESCRIBE permissions on the "
          "cluster.");
        resp.data.error_code = error_code::cluster_authorization_failed;
        resp.data.error_message = ss::sstring{
          error_code_to_str(error_code::cluster_authorization_failed)};
        co_return co_await ctx.respond(std::move(resp));
    }

    // If topics is undefined, get all in progress reassignments
    if (!request.data.topics.has_value()) {
        vlog(klog.debug, "Request to list all in progress reassignments");
        auto all_in_progress_reassignments
          = all_in_progress_partition_reassignments(ctx.metadata_cache());
        resp.data.topics.reserve(all_in_progress_reassignments.size());
        std::for_each(
          all_in_progress_reassignments.begin(),
          all_in_progress_reassignments.end(),
          [&resp](const ongoing_topic_reassignment& topic_reassignment) {
              resp.data.topics.emplace_back(ongoing_topic_reassignment{
                .name = topic_reassignment.name,
                .partitions = topic_reassignment.partitions.copy()});
          });
        co_return co_await ctx.respond(std::move(resp));
    }

    resp.data.topics.reserve(request.data.topics->size());
    for (const list_partition_reassignments_topics& tp : *request.data.topics) {
        ongoing_topic_reassignment topic_reassignment{.name = tp.name};

        for (const model::partition_id& pid : tp.partition_indexes) {
            model::ntp ntp{model::kafka_namespace, tp.name, pid};

            vlog(klog.debug, "Request to list reassignments: ntp {}", ntp);

            if (ctx.metadata_cache().is_update_in_progress(ntp)) {
                auto reassignments = list_partition_reassignments(
                  ctx.metadata_cache(), ntp);
                reassignments.partition_index = pid;
                topic_reassignment.partitions.push_back(
                  std::move(reassignments));
            } else {
                vlog(klog.debug, "No updates in progress: ntp {}", ntp);
            }
        }

        if (!topic_reassignment.partitions.empty()) {
            resp.data.topics.push_back(std::move(topic_reassignment));
        }
    }

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
