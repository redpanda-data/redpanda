/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/fwd.h"
#include "kafka/server/fwd.h"
#include "proto/redpanda/core/admin/v2/security.proto.h"
#include "redpanda/admin/proxy/client.h"
#include "security/fwd.h"

namespace admin {

// Internal helper functions exposed for testing.
// These are implementation details and should not be used outside of
// security_service_impl and its tests.
namespace internal {

bool match_scram_credential(
  const proto::admin::scram_credential&, const security::scram_credential&);

void validate_scram_credential_name(const ss::sstring&);

void validate_pb_scram_credential(const proto::admin::scram_credential&);

security::scram_credential
convert_to_security_scram_credential(const proto::admin::scram_credential&);

proto::admin::scram_credential
convert_to_pb_scram_credential(ss::sstring, const security::scram_credential&);

void validate_role_name(const ss::sstring&);

void validate_pb_role_member(const proto::admin::role_member& pb_member);

security::role_member
convert_to_security_role_member(const proto::admin::role_member& pb_member);

security::role convert_to_security_role(const proto::admin::role& pb_role);

proto::admin::role_member
convert_to_pb_role_member(const security::role_member& role_member);

proto::admin::role
convert_to_pb_role(ss::sstring role_name, const security::role& role);

} // namespace internal

class security_service_impl : public proto::admin::security_service {
public:
    security_service_impl(
      admin::proxy::client proxy_client,
      cluster::controller* controller,
      ss::sharded<kafka::server>& kafka_server,
      ss::sharded<cluster::metadata_cache>& md_cache);

    seastar::future<proto::admin::create_scram_credential_response>
      create_scram_credential(
        serde::pb::rpc::context,
        proto::admin::create_scram_credential_request) override;

    seastar::future<proto::admin::get_scram_credential_response>
      get_scram_credential(
        serde::pb::rpc::context,
        proto::admin::get_scram_credential_request) override;

    seastar::future<proto::admin::list_scram_credentials_response>
      list_scram_credentials(
        serde::pb::rpc::context,
        proto::admin::list_scram_credentials_request) override;

    seastar::future<proto::admin::update_scram_credential_response>
      update_scram_credential(
        serde::pb::rpc::context,
        proto::admin::update_scram_credential_request) override;

    seastar::future<proto::admin::delete_scram_credential_response>
      delete_scram_credential(
        serde::pb::rpc::context,
        proto::admin::delete_scram_credential_request) override;

    seastar::future<proto::admin::create_role_response> create_role(
      serde::pb::rpc::context, proto::admin::create_role_request) override;

    seastar::future<proto::admin::get_role_response> get_role(
      serde::pb::rpc::context, proto::admin::get_role_request) override;

    seastar::future<proto::admin::list_roles_response> list_roles(
      serde::pb::rpc::context, proto::admin::list_roles_request) override;

    seastar::future<proto::admin::add_role_members_response> add_role_members(
      serde::pb::rpc::context, proto::admin::add_role_members_request) override;

    seastar::future<proto::admin::remove_role_members_response>
      remove_role_members(
        serde::pb::rpc::context,
        proto::admin::remove_role_members_request) override;

    seastar::future<proto::admin::delete_role_response> delete_role(
      serde::pb::rpc::context, proto::admin::delete_role_request) override;

    seastar::future<proto::admin::list_current_user_roles_response>
      list_current_user_roles(
        serde::pb::rpc::context,
        proto::admin::list_current_user_roles_request) override;

    seastar::future<proto::admin::resolve_oidc_identity_response>
      resolve_oidc_identity(
        serde::pb::rpc::context,
        proto::admin::resolve_oidc_identity_request) override;

    seastar::future<proto::admin::refresh_oidc_keys_response> refresh_oidc_keys(
      serde::pb::rpc::context,
      proto::admin::refresh_oidc_keys_request) override;

    seastar::future<proto::admin::revoke_oidc_sessions_response>
      revoke_oidc_sessions(
        serde::pb::rpc::context,
        proto::admin::revoke_oidc_sessions_request) override;

private:
    admin::proxy::client _proxy_client;
    cluster::controller* _controller;
    ss::sharded<kafka::server>& _kafka_server;
    ss::sharded<cluster::metadata_cache>& _md_cache;
};

} // namespace admin
