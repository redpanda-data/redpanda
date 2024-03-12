/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/tls_config.h"
#include "rpc/logger.h"

namespace rpc {

/// \brief Log reload credential event
/// The function is supposed to be invoked from the callback passed to
/// 'build_reloadable_*_credentials' methods.
///
/// \param log is a ss::logger instance that should be used
/// \param system_name is a name of the subsystem that uses credentials
/// \param updated is a set of updated credential names
/// \param eptr is an exception ptr in case of error
void log_certificate_reload_event(
  ss::logger& log,
  const char* system_name,
  const std::unordered_set<ss::sstring>& updated,
  const std::exception_ptr& eptr);

inline ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
maybe_build_reloadable_certificate_credentials(config::tls_config tls_config) {
    return std::move(tls_config)
      .get_credentials_builder()
      .then([](std::optional<ss::tls::credentials_builder> credentials) {
          if (credentials) {
              return credentials->build_reloadable_certificate_credentials(
                [](
                  const std::unordered_set<ss::sstring>& updated,
                  const std::exception_ptr& eptr) {
                    log_certificate_reload_event(
                      rpclog, "Client TLS", updated, eptr);
                });
          }
          return ss::make_ready_future<
            ss::shared_ptr<ss::tls::certificate_credentials>>(nullptr);
      });
}

} // namespace rpc
