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

#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/auth.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/server.h"
#include "security/request_auth.h"

#include <seastar/core/future.hh>

#include <string_view>

namespace pandaproxy::schema_registry::enterprise {

using server = ctx_server<service>;

void handle_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  const auth& auth,
  request_auth_result& auth_result);

void handle_get_schemas_ids_id_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  std::optional<request_auth_result>& auth_result,
  const chunked_vector<context_subject>& subjects);

void handle_get_subjects_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  std::optional<request_auth_result>& auth_result,
  chunked_vector<context_subject>& subjects);

/// Handles authorization for GET /contexts by filtering the contexts vector
/// to only include contexts the user is authorized to see.
/// - Contexts with subjects: user needs describe access to at least one subject
/// - Empty contexts: user needs sr_registry describe access
ss::future<> handle_get_contexts_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  sharded_store& store,
  std::optional<request_auth_result>& auth_result,
  chunked_vector<context>& contexts);

/// Handles authorization for config/mode endpoints that operate on either
/// a context (e.g., PUT /config/:.ctx:) or a subject (e.g., PUT
/// /config/:.ctx:subject).
/// - Context-level operations require sr_registry access
/// - Subject-level operations require sr_subject access on the specific subject
void handle_config_mode_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  std::optional<request_auth_result>& auth_result,
  const context_subject& ctx_sub,
  security::acl_operation op);

} // namespace pandaproxy::schema_registry::enterprise
