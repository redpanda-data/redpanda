// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/add_offsets_to_txn.h"

#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/handlers/add_partitions_to_txn.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

template<>
ss::future<response_ptr>
add_offsets_to_txn_handler::handle(request_context ctx, ss::smp_service_group) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        add_offsets_to_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);

        cluster::add_offsets_tx_request tx_request{
          .transactional_id = request.data.transactional_id,
          .producer_id = request.data.producer_id,
          .producer_epoch = request.data.producer_epoch,
          .group_id = request.data.group_id};

        auto f = ctx.tx_gateway_frontend().add_offsets_to_tx(
          tx_request, config::shard_local_cfg().create_topic_timeout_ms());

        return f.then([&ctx](cluster::add_offsets_tx_reply tx_response) {
            add_offsets_to_txn_response_data data;
            switch (tx_response.error_code) {
            case cluster::tx_errc::none:
                data.error_code = error_code::none;
                break;
            case cluster::tx_errc::not_coordinator:
                data.error_code = error_code::not_coordinator;
                break;
            case cluster::tx_errc::coordinator_load_in_progress:
                data.error_code = error_code::coordinator_load_in_progress;
                break;
            case cluster::tx_errc::invalid_producer_id_mapping:
                data.error_code = error_code::invalid_producer_id_mapping;
                break;
            case cluster::tx_errc::fenced:
                data.error_code = error_code::invalid_producer_epoch;
                break;
            case cluster::tx_errc::invalid_txn_state:
                data.error_code = error_code::invalid_txn_state;
                break;
            default:
                data.error_code = error_code::unknown_server_error;
                break;
            }
            add_offsets_to_txn_response res;
            res.data = data;
            return ctx.respond(res);
        });
    });
}

} // namespace kafka
