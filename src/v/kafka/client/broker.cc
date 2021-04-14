// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/broker.h"

#include "kafka/client/logger.h"
#include "rpc/dns.h"

namespace kafka::client {

ss::future<shared_broker_t>
make_broker(model::node_id node_id, unresolved_address addr) {
    auto client = ss::make_lw_shared<transport>(
      rpc::base_transport::configuration{.server_addr = addr});
    return client->connect()
      .then([node_id, addr = std::move(addr), client] {
          vlog(
            kclog.info,
            "connected to broker:{} - {}:{}",
            node_id,
            addr.host(),
            addr.port());
          return ss::make_lw_shared<broker>(node_id, std::move(*client));
      })
      .handle_exception_type([node_id](const std::system_error& ex) {
          if (
            ex.code() == std::errc::host_unreachable
            || ex.code() == std::errc::connection_refused) {
              return ss::make_exception_future<shared_broker_t>(
                broker_error(node_id, error_code::network_exception));
          }
          vlog(kclog.warn, "std::system_error: ", ex.what());
          return ss::make_exception_future<shared_broker_t>(ex);
      });
}

} // namespace kafka::client
