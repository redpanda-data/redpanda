/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "model/timeout_clock.h"
#include "security/scram_credential.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <system_error>

namespace cluster {

class security_frontend final {
public:
    security_frontend(
      model::node_id,
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<security::authorizer>&);

    ss::future<std::error_code> create_user(
      security::credential_user,
      security::scram_credential,
      model::timeout_clock::time_point);

    ss::future<std::error_code>
      delete_user(security::credential_user, model::timeout_clock::time_point);

    ss::future<std::error_code> update_user(
      security::credential_user,
      security::scram_credential,
      model::timeout_clock::time_point);

    ss::future<std::vector<errc>> create_acls(
      std::vector<security::acl_binding>, model::timeout_clock::duration);

    ss::future<std::vector<delete_acls_result>> delete_acls(
      std::vector<security::acl_binding_filter>,
      model::timeout_clock::duration);

    /**
     * For use during cluster creation, if RP_BOOTSTRAP_USER is set
     * then returns the user creds specified in it
     *
     * @returns empty value if RP_BOOTSTRAP_USER is not set or user creds
     *          cannot be parsed
     */
    static std::optional<user_and_credential>
    get_bootstrap_user_creds_from_env();

private:
    ss::future<std::vector<errc>> do_create_acls(
      std::vector<security::acl_binding>, model::timeout_clock::duration);

    ss::future<std::vector<errc>> dispatch_create_acls_to_leader(
      model::node_id,
      std::vector<security::acl_binding>,
      model::timeout_clock::duration);

    ss::future<std::vector<delete_acls_result>> do_delete_acls(
      std::vector<security::acl_binding_filter>,
      model::timeout_clock::duration);

    ss::future<std::vector<delete_acls_result>> dispatch_delete_acls_to_leader(
      model::node_id,
      std::vector<security::acl_binding_filter>,
      model::timeout_clock::duration);

    model::node_id _self;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<features::feature_table>& _features;
    ss::sharded<ss::abort_source>& _as;
    ss::sharded<security::authorizer>& _authorizer;
};

} // namespace cluster
