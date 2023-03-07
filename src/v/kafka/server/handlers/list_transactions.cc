// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/list_transactions.h"

#include "cluster/errc.h"
#include "cluster/tm_stm.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/list_transactions_request.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

#include <algorithm>

namespace kafka {

template<>
ss::future<response_ptr>
list_transactions_handler::handle(request_context ctx, ss::smp_service_group) {
    list_transactions_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    list_transactions_response response;

    auto filter_tx = [](
                       const list_transactions_request& req,
                       const cluster::tm_transaction& tx) -> bool {
        if (!req.data.producer_id_filters.empty()) {
            if (std::none_of(
                  req.data.producer_id_filters.begin(),
                  req.data.producer_id_filters.end(),
                  [pid = tx.pid.get_id()](const auto& provided_pid) {
                      return pid == provided_pid;
                  })) {
                return false;
            }
        }

        if (!req.data.state_filters.empty()) {
            if (std::none_of(
                  req.data.state_filters.begin(),
                  req.data.state_filters.end(),
                  [status = tx.get_kafka_status()](
                    const auto& provided_status) {
                      return status == provided_status;
                  })) {
                return false;
            }
        }
        return true;
    };

    auto& tx_frontend = ctx.tx_gateway_frontend();
    auto txs = co_await tx_frontend.get_all_transactions();
    if (txs.has_value()) {
        for (const auto& tx : txs.value()) {
            if (!ctx.authorized(security::acl_operation::describe, tx.id)) {
                // We should skip this transactional id
                continue;
            }

            if (filter_tx(request, tx)) {
                list_transaction_state tx_state;
                tx_state.transactional_id = tx.id;
                tx_state.producer_id = kafka::producer_id(tx.pid.id);
                tx_state.transaction_state = ss::sstring(tx.get_status());
                response.data.transaction_states.push_back(std::move(tx_state));
            }
        }
    } else {
        // In this 2 errors not coordinator got request and we just return empty
        // array
        if (
          txs.error() != cluster::tx_errc::shard_not_found
          && txs.error() != cluster::tx_errc::not_coordinator) {
            vlog(
              klog.error,
              "Can not return list of transactions. Error: {}",
              txs.error());
            response.data.error_code = kafka::error_code::unknown_server_error;
        }
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
