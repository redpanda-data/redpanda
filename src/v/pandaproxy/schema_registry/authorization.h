/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "pandaproxy/schema_registry/auth.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/server.h"
#include "security/request_auth.h"

namespace pandaproxy::schema_registry::enterprise {

using server = ctx_server<service>;

void handle_authz(
  const server::request_t& rq,
  const auth& auth,
  request_auth_result& auth_result);

void handle_deferred_authz(
  const server::request_t& rq,
  const auth& auth,
  request_auth_result& auth_result,
  const chunked_vector<subject>& resource_names);

} // namespace pandaproxy::schema_registry::enterprise
