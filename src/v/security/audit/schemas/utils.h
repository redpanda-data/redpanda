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
#include "net/unresolved_address.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/types.h"
#include "security/types.h"
#include "utils/request_auth.h"
#include "utils/string_switch.h"

#include <seastar/http/handlers.hh>
#include <seastar/http/request.hh>

namespace security::audit {

api_activity make_api_activity_event(
  ss::httpd::const_req req,
  const request_auth_result& auth_result,
  bool authorized,
  const std::optional<std::string_view>& reason);

authentication make_authentication_event(
  ss::httpd::const_req req, const request_auth_result& r);

authentication make_authentication_failure_event(
  ss::httpd::const_req req,
  const security::credential_user& r,
  const ss::sstring& reason);

} // namespace security::audit
