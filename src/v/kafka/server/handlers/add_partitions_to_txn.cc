// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/add_partitions_to_txn.h"

#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

template<>
ss::future<response_ptr> add_partitions_to_txn_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        add_partitions_to_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);

        cluster::add_paritions_tx_request tx_request{
          .transactional_id = request.data.transactional_id,
          .producer_id = request.data.producer_id,
          .producer_epoch = request.data.producer_epoch};
        tx_request.topics.reserve(request.data.topics.size());
        for (auto& topic : request.data.topics) {
            cluster::add_paritions_tx_request::topic tx_topic{
              .name = std::move(topic.name),
              .partitions = std::move(topic.partitions),
            };
            tx_request.topics.push_back(tx_topic);
        }

        return ctx.tx_gateway_frontend()
          .add_partition_to_tx(
            tx_request, config::shard_local_cfg().create_topic_timeout_ms())
          .then([&ctx](cluster::add_paritions_tx_reply tx_response) {
              add_partitions_to_txn_response_data data;
              for (auto& tx_topic : tx_response.results) {
                  add_partitions_to_txn_topic_result topic{
                    .name = std::move(tx_topic.name),
                  };
                  for (const auto& tx_partition : tx_topic.results) {
                      add_partitions_to_txn_partition_result partition{
                        .partition_index = tx_partition.partition_index};
                      switch (tx_partition.error_code) {
                      case cluster::tx_errc::none:
                          partition.error_code = error_code::none;
                          break;
                      case cluster::tx_errc::not_coordinator:
                          partition.error_code = error_code::not_coordinator;
                          break;
                      case cluster::tx_errc::invalid_producer_id_mapping:
                          partition.error_code
                            = error_code::invalid_producer_id_mapping;
                          break;
                      case cluster::tx_errc::fenced:
                          partition.error_code
                            = error_code::invalid_producer_epoch;
                          break;
                      case cluster::tx_errc::invalid_txn_state:
                          partition.error_code = error_code::invalid_txn_state;
                          break;
                      default:
                          partition.error_code
                            = error_code::unknown_server_error;
                          break;
                      }
                      topic.results.push_back(partition);
                  }
                  data.results.push_back(std::move(topic));
              }

              add_partitions_to_txn_response response;
              response.data = data;
              return ctx.respond(std::move(response));
          });
    });
}

} // namespace kafka
