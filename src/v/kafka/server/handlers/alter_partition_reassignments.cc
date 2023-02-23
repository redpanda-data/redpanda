
// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/alter_partition_reassignments.h"

#include "cluster/errc.h"
#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "config/node_config.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/alter_partition_reassignments_request.h"
#include "kafka/protocol/schemata/alter_partition_reassignments_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/fwd.h"
#include "kafka/server/protocol_utils.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"

#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>

#include <algorithm>
#include <iterator>
#include <vector>

namespace kafka {

using partitions_request_iterator
  = std::vector<reassignable_partition>::iterator;

template<typename ResultIter, typename Predicate>
partitions_request_iterator validate_replicas(
  partitions_request_iterator begin,
  partitions_request_iterator end,
  ResultIter out_it,
  error_code ec,
  const ss::sstring& error_message,
  Predicate&& p) {
    auto valid_range_end = std::partition(
      begin, end, std::forward<Predicate>(p));

    if (valid_range_end != end) {
        vlog(klog.debug, "Invalid replicas: ec {} - {}", ec, error_message);
    }

    std::transform(
      valid_range_end,
      end,
      out_it,
      [ec, &error_message](const reassignable_partition& partition) {
          return reassignable_partition_response{
            .partition_index = partition.partition_index,
            .error_code = ec,
            .error_message = error_message,
          };
      });
    return valid_range_end;
}

/**
 * @brief Validates partitions and places invalid partitions into @p resp_it
 * @param begin starting position for a vector<reassignable_partition>
 * @param end stopping position for a vector<reassignable_partition>
 * @param resp_it  a wrapper to std::back_inserter
 * @param topic_response a reassignable_topic_response to put errors into
 * @param alive_nodes list of RP nodes that are live
 * @param tp_metadata topic metadata used to check replication factor
 *
 * @return an iterator that represents the stop position of all valid
 * partitions
 */
template<typename Container>
partitions_request_iterator validate_partitions(
  partitions_request_iterator begin,
  partitions_request_iterator end,
  std::back_insert_iterator<Container> resp_it,
  reassignable_topic_response topic_response,
  std::vector<model::node_id> alive_nodes,
  std::optional<cluster::topic_metadata> tp_metadata) {
    // An undefined replicas vector is not an error, see "Replicas" in the
    // AlterPartitionReassignmentsRequest schemata. Therefore checks for
    // replicas.has_value are necessary.

    std::vector<reassignable_partition_response> invalid_partitions;

    auto valid_partitions_end = validate_replicas(
      begin,
      end,
      std::back_inserter(invalid_partitions),
      error_code::invalid_replica_assignment,
      "Empty replica list specified in partition reassignment.",
      [](const reassignable_partition& partition) {
          return !partition.replicas.has_value() ? true
                                                 : !partition.replicas->empty();
      });

    valid_partitions_end = validate_replicas(
      begin,
      valid_partitions_end,
      std::back_inserter(invalid_partitions),
      error_code::invalid_replica_assignment,
      "Duplicate replica ids in partition reassignment replica list",
      [](const reassignable_partition& partition) {
          if (partition.replicas.has_value()) {
              absl::flat_hash_set<model::node_id> replicas_set;
              for (const auto& node_id : *partition.replicas) {
                  auto res = replicas_set.insert(node_id);
                  if (!res.second) {
                      return false;
                  }
              }
          }
          return true;
      });

    valid_partitions_end = validate_replicas(
      begin,
      valid_partitions_end,
      std::back_inserter(invalid_partitions),
      error_code::invalid_replica_assignment,
      "Invalid broker id in replica list",
      [](const reassignable_partition& partition) {
          if (!partition.replicas.has_value()) {
              return true;
          }

          auto negative_node_id_it = std::find_if(
            partition.replicas->begin(),
            partition.replicas->end(),
            [](const model::node_id& node_id) { return node_id < 0; });
          return negative_node_id_it == partition.replicas->end();
      });

    valid_partitions_end = validate_replicas(
      begin,
      valid_partitions_end,
      std::back_inserter(invalid_partitions),
      error_code::invalid_replica_assignment,
      "Replica assignment has brokers that are not alive",
      [&alive_nodes](const reassignable_partition& partition) {
          if (!partition.replicas.has_value()) {
              return true;
          }

          auto unkown_broker_id_it = std::find_if(
            partition.replicas->begin(),
            partition.replicas->end(),
            [alive_nodes](const model::node_id& node_id) {
                return std::find(
                         alive_nodes.begin(), alive_nodes.end(), node_id)
                       == alive_nodes.end();
            });
          return unkown_broker_id_it == partition.replicas->end();
      });

    // Check for undefined topic here instead of outside
    // validate_partitions because there are only two places to store errors,
    // the top level and reassignable_partition. The top level should be for
    // errors that apply to all topics, not a single topic.
    valid_partitions_end = validate_replicas(
      begin,
      valid_partitions_end,
      std::back_inserter(invalid_partitions),
      error_code::unknown_topic_or_partition,
      "Topic or partition is undefined",
      [&tp_metadata](const reassignable_partition&) {
          return tp_metadata.has_value();
      });

    // Check the replication factor when topic metadata is defined.
    if (tp_metadata.has_value()) {
        valid_partitions_end = validate_replicas(
          begin,
          valid_partitions_end,
          std::back_inserter(invalid_partitions),
          error_code::invalid_replication_factor,
          "Number of replicas does not match the topic replication factor",
          [&tp_metadata](const reassignable_partition& partition) {
              if (!partition.replicas.has_value()) {
                  return true;
              }

              auto tp_replication_factor
                = tp_metadata.value().get_replication_factor();
              vlog(
                klog.debug,
                "Checking replication factor: cfg {}, replication factor {}, "
                "requested replicas {}",
                tp_metadata.value().get_configuration(),
                tp_replication_factor,
                *partition.replicas);
              return size_t(tp_replication_factor)
                     == partition.replicas->size();
          });
    }

    // Store any invalid partitions in the response
    if (!invalid_partitions.empty()) {
        topic_response.partitions = std::move(invalid_partitions);
        // resp_it is a wrapper to std::back_inserter
        *resp_it = std::move(topic_response);
    }

    return valid_partitions_end;
}

template<>
ss::future<response_ptr> alter_partition_reassignments_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    alter_partition_reassignments_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    alter_partition_reassignments_response resp;

    if (!config::shard_local_cfg().kafka_enable_partition_reassignment()) {
        vlog(
          klog.info,
          "Rejected alter partition reassignment request: API is disabled. See "
          "`kafka_enable_partition_reassignment` configuration option");
        resp.data.error_code = error_code::invalid_replica_assignment;
        resp.data.error_message
          = "AlterPartitionReassignment API is disabled. See "
            "`kafka_enable_partition_reassignment` configuration option.";
        co_return co_await ctx.respond(std::move(resp));
    }

    if (!ctx.authorized(
          security::acl_operation::alter, security::default_cluster_name)) {
        vlog(
          klog.debug,
          "Failed cluster authorization. Requires ALTER permissions on the "
          "cluster.");
        resp.data.error_code = error_code::cluster_authorization_failed;
        resp.data.error_message = ss::sstring{
          error_code_to_str(error_code::cluster_authorization_failed)};
        co_return co_await ctx.respond(std::move(resp));
    }

    resp.data.responses.reserve(request.data.topics.size());
    std::vector<model::node_id> alive_nodes;
    auto alive_brokers_md = co_await ctx.metadata_cache().alive_nodes();
    for (const auto& node_md : alive_brokers_md) {
        alive_nodes.push_back(node_md.broker.id());
    }

    for (auto& topic : request.data.topics) {
        reassignable_topic_response topic_response{.name = topic.name};
        auto tp_metadata = ctx.metadata_cache().get_topic_metadata(
          model::topic_namespace{model::kafka_namespace, topic.name});

        auto valid_partitions_end = validate_partitions(
          topic.partitions.begin(),
          topic.partitions.end(),
          std::back_inserter(resp.data.responses),
          topic_response,
          alive_nodes,
          tp_metadata);

        for (auto it = topic.partitions.begin(); it != valid_partitions_end;
             ++it) {
            const reassignable_partition& partition{*it};
            model::ntp ntp{
              model::kafka_namespace, topic.name, partition.partition_index};

            if (partition.replicas.has_value()) {
                vlog(
                  klog.debug,
                  "Request to reassign partitions: ntp {}, replicas {}",
                  ntp,
                  *partition.replicas);

                auto errc
                  = co_await ctx.topics_frontend().move_partition_replicas(
                    ntp,
                    std::move(*partition.replicas),
                    model::timeout_clock::now() + request.data.timeout_ms);

                if (!errc) {
                    topic_response.partitions.push_back(
                      reassignable_partition_response{
                        .partition_index = partition.partition_index});
                } else {
                    auto clerr = static_cast<cluster::errc>(errc.value());
                    vlog(
                      klog.debug,
                      "Failed to move partition replicas: ntp {}, ec {}",
                      ntp,
                      clerr);
                    auto kerr = map_topic_error_code(clerr);
                    topic_response.partitions.push_back(
                      reassignable_partition_response{
                        .partition_index = partition.partition_index,
                        .error_code = kerr,
                        .error_message = ss::sstring{error_code_to_str(kerr)}});
                }

            } else {
                // Otherwise we cancel the pending request
                vlog(
                  klog.debug, "Request to cancel pending request: ntp {}", ntp);
                auto errc = co_await ctx.topics_frontend()
                              .cancel_moving_partition_replicas(
                                ntp,
                                model::timeout_clock::now()
                                  + request.data.timeout_ms);
                if (!errc) {
                    topic_response.partitions.push_back(
                      reassignable_partition_response{
                        .partition_index = partition.partition_index});
                } else {
                    auto clerr = static_cast<cluster::errc>(errc.value());
                    vlog(
                      klog.debug,
                      "Failed to cancel pending request: ntp {}, ec {}",
                      ntp,
                      clerr);
                    auto kerr = map_topic_error_code(clerr);
                    topic_response.partitions.push_back(
                      reassignable_partition_response{
                        .partition_index = partition.partition_index,
                        .error_code = kerr,
                        .error_message = ss::sstring{error_code_to_str(kerr)}});
                }
            }
        }

        // Insert into the response if there were valid partitions
        if (std::distance(topic.partitions.begin(), valid_partitions_end) > 0) {
            resp.data.responses.emplace_back(std::move(topic_response));
        }
    }

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
