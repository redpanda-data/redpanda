// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/offset_commit_request.h"

#include "cluster/namespace.h"
#include "kafka/errors.h"
#include "kafka/groups/group_manager.h"
#include "kafka/groups/group_router.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "model/metadata.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>
#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

void offset_commit_response::encode(
  const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

struct offset_commit_ctx {
    request_context rctx;
    offset_commit_request request;
    ss::smp_service_group ssg;

    // topic partitions found not to existent prior to processing. responses for
    // these are patched back into the final response after processing.
    absl::
      flat_hash_map<model::topic, std::vector<offset_commit_response_partition>>
        nonexistent_tps;

    offset_commit_ctx(
      request_context&& rctx,
      offset_commit_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr>
offset_commit_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    offset_commit_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    if (request.data.group_instance_id) {
        return ctx.respond(
          offset_commit_response(request, error_code::unsupported_version));
    }

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
         * check if topic exists
         */
        const auto topic_name = model::topic(it->name);
        model::topic_namespace_view tn(cluster::kafka_namespace, topic_name);

        if (const auto& md = octx.rctx.metadata_cache().get_topic_metadata(tn);
            md) {
            /*
             * check if each partition exists
             */
            auto split = std::partition(
              it->partitions.begin(),
              it->partitions.end(),
              [&md](const offset_commit_request_partition& p) {
                  return std::any_of(
                    md->partitions.cbegin(),
                    md->partitions.cend(),
                    [&p](const model::partition_metadata& pmd) {
                        return pmd.id == p.partition_index;
                    });
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

    return ss::do_with(std::move(octx), [](offset_commit_ctx& octx) {
        return octx.rctx.groups()
          .offset_commit(std::move(octx.request))
          .then([&octx](offset_commit_response resp) {
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
              return octx.rctx.respond(std::move(resp));
          });
    });
}

} // namespace kafka
