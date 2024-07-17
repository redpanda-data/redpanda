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

#include "rpc/rpc_utils.h"

#include "base/vlog.h"

namespace rpc {

void log_certificate_reload_event(
  ss::logger& log,
  const char* system_name,
  const std::unordered_set<ss::sstring>& updated,
  const std::exception_ptr& eptr) {
    if (eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (...) {
            vlog(
              log.error,
              "{} credentials reload error {}",
              system_name,
              std::current_exception());
        }
    } else {
        for (const auto& name : updated) {
            vlog(
              log.info,
              "{} key or certificate file "
              "updated - {}",
              system_name,
              name);
        }
    }
}

} // namespace rpc
