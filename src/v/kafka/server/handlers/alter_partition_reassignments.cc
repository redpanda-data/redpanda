
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
#include "container/fragmented_vector.h"
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
namespace {
struct alter_op_context {
    request_context rctx;
    alter_partition_reassignments_request request;
    alter_partition_reassignments_response response;
};
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
 * @param topic the reassignable_topic to validate
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
  reassignable_topic& topic,
  std::back_insert_iterator<Container> resp_it,
  std::vector<model::node_id> alive_nodes,
  std::optional<std::reference_wrapper<const cluster::topic_metadata>>
    tp_metadata) {
    // An undefined replicas vector is not an error, see "Replicas" in the
    // AlterPartitionReassignmentsRequest schemata. Therefore checks for
    // replicas.has_value are necessary.

    std::vector<reassignable_partition_response> invalid_partitions;
    auto begin = topic.partitions.begin();
    auto end = topic.partitions.end();

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
                = tp_metadata.value().get().get_replication_factor();
              vlog(
                klog.debug,
                "Checking replication factor: cfg {}, replication factor {}, "
                "requested replicas {}",
                tp_metadata.value().get().get_configuration(),
                tp_replication_factor,
                *partition.replicas);
              return size_t(tp_replication_factor)
                     == partition.replicas->size();
          });
    }

    // Store any invalid partitions in the response
    if (!invalid_partitions.empty()) {
        // resp_it is a wrapper to std::back_inserter
        *resp_it = reassignable_topic_response{
          .name = topic.name, .partitions = std::move(invalid_partitions)};
    }

    return valid_partitions_end;
}

ss::future<std::vector<model::node_id>>
collect_alive_nodes(request_context& ctx) {
    std::vector<model::node_id> alive_nodes;
    auto alive_brokers_md = co_await ctx.metadata_cache().alive_nodes();
    alive_nodes.reserve(alive_brokers_md.size());
    for (const auto& node_md : alive_brokers_md) {
        alive_nodes.push_back(node_md.broker.id());
    }

    co_return alive_nodes;
}

ss::future<std::error_code> handle_partition(
  model::ntp ntp, alter_op_context& octx, reassignable_partition partition) {
    if (partition.replicas.has_value()) {
        vlog(
          klog.debug,
          "Request to reassign partitions: ntp {}, replicas {}",
          ntp,
          *partition.replicas);

        return octx.rctx.topics_frontend().move_partition_replicas(
          ntp,
          std::vector<model::node_id>{
            std::make_move_iterator(partition.replicas->begin()),
            std::make_move_iterator(partition.replicas->end())},
          cluster::reconfiguration_policy::full_local_retention,
          model::timeout_clock::now() + octx.request.data.timeout_ms);

    } else {
        // Otherwise we cancel the pending request
        vlog(klog.debug, "Request to cancel pending request: ntp {}", ntp);
        return octx.rctx.topics_frontend().cancel_moving_partition_replicas(
          ntp, model::timeout_clock::now() + octx.request.data.timeout_ms);
    }
}

reassignable_partition_response
map_errc_to_response(const model::ntp& ntp, std::error_code ec) {
    reassignable_partition_response response{
      .partition_index = ntp.tp.partition};

    if (ec) {
        vlog(
          klog.info,
          "Failed to handle partition {} operation. Error: {}",
          ntp,
          ec);

        if (ec.category() == cluster::error_category()) {
            response.error_code = map_topic_error_code(
              static_cast<cluster::errc>(ec.value()));
            response.error_message = std::make_optional<ss::sstring>(
              error_code_to_str(response.error_code));

        } else {
            response.error_code = kafka::error_code::unknown_server_error;
        }
    }

    return response;
}

ss::future<reassignable_topic_response> do_handle_topic(
  reassignable_topic& topic,
  std::vector<model::node_id> alive_nodes,
  alter_op_context& octx) {
    reassignable_topic_response topic_response{.name = topic.name};
    topic_response.partitions.reserve(topic.partitions.size());
    auto tp_metadata_ref = octx.rctx.metadata_cache().get_topic_metadata_ref(
      model::topic_namespace_view{model::kafka_namespace, topic.name});

    auto valid_partitions_end = validate_partitions(
      topic,
      std::back_inserter(octx.response.data.responses),
      std::move(alive_nodes),
      tp_metadata_ref);

    return ssx::async_transform(
             topic.partitions.begin(),
             valid_partitions_end,
             [&octx,
              topic_name = topic.name](reassignable_partition partition) {
                 model::ntp ntp{
                   model::kafka_namespace,
                   topic_name,
                   partition.partition_index};
                 return handle_partition(ntp, octx, std::move(partition))
                   .then([ntp](std::error_code ec) {
                       return map_errc_to_response(ntp, ec);
                   });
             })
      .then([topic_response = std::move(topic_response)](
              std::vector<reassignable_partition_response> r) mutable {
          std::move(
            r.begin(), r.end(), std::back_inserter(topic_response.partitions));
          return std::move(topic_response);
      });
}
static ss::future<response_ptr> do_handle(alter_op_context& octx) {
    if (!config::shard_local_cfg().kafka_enable_partition_reassignment()) {
        vlog(
          klog.info,
          "Rejected alter partition reassignment request: API is disabled. See "
          "`kafka_enable_partition_reassignment` configuration option");
        octx.response.data.error_code = error_code::invalid_replica_assignment;
        octx.response.data.error_message
          = "AlterPartitionReassignment API is disabled. See "
            "`kafka_enable_partition_reassignment` configuration option.";
        return octx.rctx.respond(std::move(octx.response));
    }

    if (octx.rctx.recovery_mode_enabled()) {
        octx.response.data.error_code = error_code::policy_violation;
        octx.response.data.error_message = "Forbidden in recovery mode.";
        return octx.rctx.respond(std::move(octx.response));
    }

    auto additional_resources = [&octx]() {
        std::vector<model::topic> topics;
        topics.reserve(octx.request.data.topics.size());
        std::transform(
          octx.request.data.topics.begin(),
          octx.request.data.topics.end(),
          std::back_inserter(topics),
          [](const reassignable_topic& t) { return t.name; });

        return topics;
    };

    auto authz = octx.rctx.authorized(
      security::acl_operation::alter,
      security::default_cluster_name,
      std::move(additional_resources));

    if (!octx.rctx.audit()) {
        octx.response.data.error_code = error_code::broker_not_available;
        octx.response.data.error_message
          = "Broker not available - audit system failure";

        return octx.rctx.respond(std::move(octx.response));
    }

    if (!authz) {
        vlog(
          klog.debug,
          "Failed cluster authorization. Requires ALTER permissions on the "
          "cluster.");
        octx.response.data.error_code
          = error_code::cluster_authorization_failed;
        octx.response.data.error_message = ss::sstring{
          error_code_to_str(error_code::cluster_authorization_failed)};
        return octx.rctx.respond(std::move(octx.response));
    }

    return collect_alive_nodes(octx.rctx)
      .then([&octx](std::vector<model::node_id> alive_nodes) {
          return ssx::parallel_transform(
            std::make_move_iterator(octx.request.data.topics.begin()),
            std::make_move_iterator(octx.request.data.topics.end()),
            [&octx, alive_nodes = std::move(alive_nodes)](
              reassignable_topic topic) mutable {
                return ss::do_with(topic, [&octx, alive_nodes](auto& topic) {
                    return do_handle_topic(topic, alive_nodes, octx);
                });
            });
      })
      .then([&octx](std::vector<reassignable_topic_response> topic_responses) {
          for (auto& t : topic_responses) {
              if (!t.partitions.empty()) {
                  octx.response.data.responses.push_back(std::move(t));
              }
          }
          return octx.rctx.respond(std::move(octx.response));
      });
}

} // namespace

template<>
ss::future<response_ptr> alter_partition_reassignments_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    alter_partition_reassignments_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    return ss::do_with(
      alter_op_context{.rctx = std::move(ctx), .request = std::move(request)},
      [](alter_op_context& octx) { return do_handle(octx); });
}

} // namespace kafka
