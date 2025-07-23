/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/config_utils.h"

#include "base/seastarx.h"
#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "config/configuration.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "security/acl.h"
#include "strings/string_switch.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

#include <exception>

namespace kafka::client {
namespace {
std::optional<sasl_configuration> create_sasl_configuration_from_client_cfg(
  const kafka::client::configuration& client_cfg) {
    if (
      client_cfg.scram_password.is_overriden()
      || client_cfg.scram_username.is_overriden()
      || client_cfg.sasl_mechanism.is_overriden()) {
        return sasl_configuration{
          .mechanism = client_cfg.sasl_mechanism(),
          .username = client_cfg.scram_username(),
          .password = client_cfg.scram_password(),
        };
    }
    return std::nullopt;
}
} // namespace
ss::future<std::optional<kafka::client::sasl_configuration>>
create_client_credentials(
  cluster::controller& controller,
  const config::configuration& cluster_cfg,
  const kafka::client::configuration& client_cfg,
  security::acl_principal principal) {
    auto sasl_cfg = create_sasl_configuration_from_client_cfg(client_cfg);
    // If AuthZ is not enabled, don't create credentials.
    if (!cluster_cfg.kafka_enable_authorization().value_or(
          cluster_cfg.enable_sasl())) {
        co_return sasl_cfg;
    }

    // If the configuration is overriden, use it.
    if (
      client_cfg.scram_password.is_overriden()
      || client_cfg.scram_username.is_overriden()
      || client_cfg.sasl_mechanism.is_overriden()) {
        co_return sasl_cfg;
    }

    // Get the internal secret for user
    auto& frontend = controller.get_ephemeral_credential_frontend().local();
    auto pw = co_await frontend.get(principal);

    if (pw.err != cluster::errc::success) {
        co_return ss::coroutine::return_exception(
          std::runtime_error(fmt::format(
            "Failed to fetch credential for principal: {}", principal)));
    }

    co_return sasl_configuration{
      .mechanism = pw.credential.mechanism(),
      .username = pw.credential.user()(),
      .password = pw.credential.password()()};
}

} // namespace kafka::client
