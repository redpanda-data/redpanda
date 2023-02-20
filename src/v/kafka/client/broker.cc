// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/broker.h"

#include "cluster/cluster_utils.h"
#include "kafka/client/logger.h"
#include "kafka/client/sasl_client.h"
#include "net/connection.h"
#include "net/dns.h"

#include <seastar/core/coroutine.hh>

namespace kafka::client {

ss::future<shared_broker_t> make_broker(
  model::node_id node_id,
  net::unresolved_address addr,
  const configuration& config) {
    return cluster::maybe_build_reloadable_certificate_credentials(
             config.broker_tls())
      .then([addr, client_id = config.client_identifier()](
              ss::shared_ptr<ss::tls::certificate_credentials> creds) mutable {
          return ss::make_lw_shared<transport>(
            net::base_transport::configuration{
              .server_addr = addr, .credentials = std::move(creds)},
            std::move(client_id));
      })
      .then([node_id, addr](ss::lw_shared_ptr<transport> client) {
          return client->connect().then(
            [node_id, addr = std::move(addr), client] {
                vlog(
                  kclog.info,
                  "connected to broker:{} - {}:{}",
                  node_id,
                  addr.host(),
                  addr.port());
                return ss::make_lw_shared<broker>(node_id, std::move(*client));
            });
      })
      .handle_exception_type([node_id](const std::system_error& ex) {
          if (net::is_reconnect_error(ex)) {
              return ss::make_exception_future<shared_broker_t>(
                broker_error(node_id, error_code::network_exception));
          }
          vlog(kclog.warn, "std::system_error: {}", ex.what());
          return ss::make_exception_future<shared_broker_t>(ex);
      })
      .then([&config](shared_broker_t broker) {
          return do_authenticate(broker, config).then([broker]() {
              return broker;
          });
      });
}

} // namespace kafka::client
