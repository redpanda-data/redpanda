// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/offset_commit.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/metadata.h"
#include "model/namespace.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>
#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

struct offset_commit_ctx {
    request_context rctx;
    offset_commit_request request;
    ss::smp_service_group ssg;

    // topic partitions found not to existent prior to processing. responses for
    // these are patched back into the final response after processing.
    absl::
      flat_hash_map<model::topic, std::vector<offset_commit_response_partition>>
        nonexistent_tps;

    // topic partitions that principal is not authorized to read
    absl::
      flat_hash_map<model::topic, std::vector<offset_commit_response_partition>>
        unauthorized_tps;

    offset_commit_ctx(
      request_context&& rctx,
      offset_commit_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

process_result_stages
offset_commit_handler::handle(request_context ctx, ss::smp_service_group ssg) {
    offset_commit_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    if (request.data.group_instance_id) {
        return process_result_stages::single_stage(ctx.respond(
          offset_commit_response(request, error_code::unsupported_version)));
    }

    // check authorization for this group
    const auto group_authorized = ctx.authorized(
      security::acl_operation::read, request.data.group_id);

    offset_commit_ctx octx(std::move(ctx), std::move(request), ssg);

    /*
     * offset commit will operate normally on topic-partitions in the request
     * that exist, while returning partial errors for those that do not exist.
     * in order to deal with this we filter out nonexistent topic-partitions,
     * and pass those that exist on to the group membership layer.
     *
     * TODO: the filtering is expensive for large requests. there are two things
     * that can be done to speed this up. first, the metadata cache should
     * provide an interface for efficiently searching for topic-partitions.
     * second, the code generator should be extended to allow the generated
     * structures to contain extra fields. in this case, we could introduce a
     * flag to mark topic-partitions to be ignored by the group membership
     * subsystem.
     */
    for (auto it = octx.request.data.topics.begin();
         it != octx.request.data.topics.end();) {
        /*
         * not authorized for this group. all topics in the request are
         * responded to with a group authorization failed error code.
         */
        if (!group_authorized) {
            auto& parts = octx.unauthorized_tps[it->name];
            parts.reserve(it->partitions.size());
            for (const auto& part : it->partitions) {
                parts.push_back(offset_commit_response_partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::group_authorization_failed,
                });
            }
            it = octx.request.data.topics.erase(it);
            continue;
        }

        /*
         * check topic authorization
         */
        if (!octx.rctx.authorized(security::acl_operation::read, it->name)) {
            auto& parts = octx.unauthorized_tps[it->name];
            parts.reserve(it->partitions.size());
            for (const auto& part : it->partitions) {
                parts.push_back(offset_commit_response_partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::topic_authorization_failed,
                });
            }
            it = octx.request.data.topics.erase(it);
            continue;
        }

        /*
         * check if topic exists
         */
        const auto topic_name = model::topic(it->name);
        model::topic_namespace_view tn(model::kafka_namespace, topic_name);

        if (octx.rctx.metadata_cache().contains(tn)) {
            /*
             * check if each partition exists
             */
            auto split = std::partition(
              it->partitions.begin(),
              it->partitions.end(),
              [&octx, &tn](const offset_commit_request_partition& p) {
                  return octx.rctx.metadata_cache().contains(
                    tn, p.partition_index);
              });
            /*
             * build responses for nonexistent topic partitions
             */
            if (split != it->partitions.end()) {
                auto& parts = octx.nonexistent_tps[it->name];
                for (auto part = split; part != it->partitions.end(); part++) {
                    parts.push_back(offset_commit_response_partition{
                      .partition_index = part->partition_index,
                      .error_code = error_code::unknown_topic_or_partition,
                    });
                }
                it->partitions.erase(split, it->partitions.end());
            }
            ++it;
        } else {
            /*
             * the topic doesn't exist. build all partition responses.
             */
            auto& parts = octx.nonexistent_tps[it->name];
            for (const auto& part : it->partitions) {
                parts.push_back(offset_commit_response_partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::unknown_topic_or_partition,
                });
            }
            it = octx.request.data.topics.erase(it);
        }
    }

    // all of the topics either don't exist or failed authorization
    if (unlikely(octx.request.data.topics.empty())) {
        offset_commit_response resp;
        for (auto& topic : octx.nonexistent_tps) {
            resp.data.topics.push_back(offset_commit_response_topic{
              .name = topic.first,
              .partitions = std::move(topic.second),
            });
        }
        for (auto& topic : octx.unauthorized_tps) {
            resp.data.topics.push_back(offset_commit_response_topic{
              .name = topic.first,
              .partitions = std::move(topic.second),
            });
        }
        return process_result_stages::single_stage(
          octx.rctx.respond(std::move(resp)));
    }
    ss::promise<> dispatch;
    auto dispatch_f = dispatch.get_future();
    auto f = ss::do_with(
      std::move(octx),
      [dispatch = std::move(dispatch)](offset_commit_ctx& octx) mutable {
          auto stages = octx.rctx.groups().offset_commit(
            std::move(octx.request));
          stages.dispatched.forward_to(std::move(dispatch));
          return stages.result.then([&octx](offset_commit_response resp) {
              if (unlikely(!octx.nonexistent_tps.empty())) {
                  /*
                   * copy over partitions for topics that had some partitions
                   * that were processed normally.
                   */
                  for (auto& topic : resp.data.topics) {
                      auto it = octx.nonexistent_tps.find(topic.name);
                      if (it != octx.nonexistent_tps.end()) {
                          topic.partitions.insert(
                            topic.partitions.end(),
                            it->second.begin(),
                            it->second.end());
                          octx.nonexistent_tps.erase(it);
                      }
                  }
                  /*
                   * the remaining nonexistent topics are moved into the
                   * response directly.
                   */
                  for (auto& topic : octx.nonexistent_tps) {
                      resp.data.topics.push_back(offset_commit_response_topic{
                        .name = topic.first,
                        .partitions = std::move(topic.second),
                      });
                  }
              }
              // no need to handle partial sets of partitions here because
              // authorization occurs all or nothing on a level
              for (auto& topic : octx.unauthorized_tps) {
                  resp.data.topics.push_back(offset_commit_response_topic{
                    .name = topic.first,
                    .partitions = std::move(topic.second),
                  });
              }
              return octx.rctx.respond(std::move(resp));
          });
      });

    return process_result_stages(std::move(dispatch_f), std::move(f));
}

} // namespace kafka
