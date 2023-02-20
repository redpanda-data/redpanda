// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/txn_offset_commit.h"

#include "cluster/topics_frontend.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

struct txn_offset_commit_ctx {
    request_context rctx;
    txn_offset_commit_request request;
    ss::smp_service_group ssg;

    // topic partitions found not to existent prior to processing. responses for
    // these are patched back into the final response after processing.
    absl::flat_hash_map<
      model::topic,
      std::vector<txn_offset_commit_response_partition>>
      nonexistent_tps;

    txn_offset_commit_ctx(
      request_context&& rctx,
      txn_offset_commit_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr> txn_offset_commit(txn_offset_commit_ctx& octx) {
    return octx.rctx.groups()
      .txn_offset_commit(std::move(octx.request))
      .then([&octx](txn_offset_commit_response resp) {
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
                  resp.data.topics.push_back(txn_offset_commit_response_topic{
                    .name = topic.first,
                    .partitions = std::move(topic.second),
                  });
              }
          }
          return octx.rctx.respond(std::move(resp));
      });
}

template<>
ss::future<response_ptr> txn_offset_commit_handler::handle(
  request_context ctx, ss::smp_service_group ssg) {
    txn_offset_commit_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    txn_offset_commit_ctx octx(std::move(ctx), std::move(request), ssg);

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
        model::topic_namespace_view tn(model::kafka_namespace, topic_name);

        if (octx.rctx.metadata_cache().contains(tn)) {
            /*
             * check if each partition exists
             */
            auto split = std::partition(
              it->partitions.begin(),
              it->partitions.end(),
              [&octx, &tn](const txn_offset_commit_request_partition& p) {
                  return octx.rctx.metadata_cache().contains(
                    tn, p.partition_index);
              });
            /*
             * build responses for nonexistent topic partitions
             */
            if (split != it->partitions.end()) {
                auto& parts = octx.nonexistent_tps[it->name];
                for (auto part = split; part != it->partitions.end(); part++) {
                    parts.push_back(txn_offset_commit_response_partition{
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
                parts.push_back(txn_offset_commit_response_partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::unknown_topic_or_partition,
                });
            }
            it = octx.request.data.topics.erase(it);
        }
    }

    return ss::do_with(std::move(octx), txn_offset_commit);
}

} // namespace kafka
