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

/**
 * Frontend interface to security-related controller commands
 *
 * User management:
 *   - create_user_cmd
 *   - delete_user_cmd
 *   - update_user_cmd
 *
 * Role management:
 *   - create_role_cmd
 *   - delete_role_cmd
 *   - update_role_cmd
 *
 * ACL management:
 *   - create_acls_cmd
 *   - delete_acls_cmd
 *
 * Member functions are not guaranteed to route outgoing requests to the
 * controller leader node. See individual members for detail.
 *
 */
class security_frontend final {
public:
    security_frontend(
      model::node_id self,
      controller* controller,
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<security::authorizer>&);

    /**
     * Create a SASL/SCRAM user with the provided credential.
     *
     * Returns:
     *   errc::user_exists
     *     if the user was already present in the credential store
     *   errc::success
     *     otherwise
     *
     * Should be called ONLY on the controller leader node, but may be called
     * from any shard
     */
    ss::future<std::error_code> create_user(
      security::credential_user,
      security::scram_credential,
      model::timeout_clock::time_point);

    /**
     * Delete a SASL/SCRAM user by name.
     *
     * Returns:
     *   errc::user_does_not_exist
     *     if the user was not present in the credential store
     *   errc::success
     *     otherwise
     *
     * Should be called ONLY on the controller leader node, but may be called
     * from any shard
     */
    ss::future<std::error_code>
      delete_user(security::credential_user, model::timeout_clock::time_point);

    /**
     * Update a SASL/SCRAM user's stored credential by (user)name.
     *
     * Returns:
     *   errc::user_does_not_exist
     *     if the user was not present in the credential store
     *   errc::success
     *     otherwise
     *
     * Should be called ONLY on the controller leader node, but may be called
     * from any shard
     */
    ss::future<std::error_code> update_user(
      security::credential_user,
      security::scram_credential,
      model::timeout_clock::time_point);

    /**
     * Create a Redpanda Role.
     *
     * Returns:
     *   errc::role_exists
     *     if the role was already present in the store
     *   errc::success
     *     otherwise
     *
     * Should be called ONLY on the controller leader node, but may be called
     * from any shard
     */
    ss::future<std::error_code> create_role(
      security::role_name name,
      security::role role,
      model::timeout_clock::time_point tout);

    /**
     * Delete a Redpanda Role by name.
     *
     * Returns:
     *   errc::role_does_not_exist
     *     if the role was not present in the store
     *   errc::success
     *     otherwise
     *
     * Should be called ONLY on the controller leader node, but may be called
     * from any shard
     */
    ss::future<std::error_code> delete_role(
      security::role_name name, model::timeout_clock::time_point tout);

    /**
     * Update a Redpanda Role by name, overwriting its contents with the
     * contents of the provided role.
     *
     * Returns:
     *   errc::role_does_not_exist
     *     if the role was not present in the store
     *   errc::success
     *     otherwise
     *
     * Should be called ONLY on the controller leader node, but may be called
     * from any shard
     */
    ss::future<std::error_code> update_role(
      security::role_name name,
      security::role role,
      model::timeout_clock::time_point tout);

    /**
     * Add ACL bindings to the authorizer
     *
     * Returns:
     *   A vector of cluster::errc, one for each of the requested bindings.
     *
     * May be called from any node; handles routing the underlying controller
     * command to the leader node automatically.
     */
    ss::future<std::vector<errc>> create_acls(
      std::vector<security::acl_binding>, model::timeout_clock::duration);

    /**
     * Remove ACL bindings matching the provided filters from the authorizer
     *
     * Returns:
     *   std::vector<delete_acls_result> (i.e. {cluster::errc, acl_binding})
     *     one for each binding that matched one of the provided filters
     *
     * May be called from any node; handles routing the underlying controller
     * command to the leader node automatically.
     */
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

    /**
     * Wait until this node has caught up to the controller leader committed
     * offset, subject to the provided timeout.
     *
     * Useful for achieving strong consistency for, e.g., describe ACL requests
     *
     * Returns:
     *   errc::shutting_down
     *     if the wait operation is interrupted by an abort request
     *   errc::timeout
     *     if the timeout expires before the node is caught up
     *   errc::success
     *     otherwise
     */
    ss::future<std::error_code>
      wait_until_caughtup_with_leader(model::timeout_clock::duration);

private:
    ss::future<result<model::offset>>
      get_leader_committed(model::timeout_clock::duration);

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
    controller* _controller;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<features::feature_table>& _features;
    ss::sharded<ss::abort_source>& _as;
    ss::sharded<security::authorizer>& _authorizer;
};

} // namespace cluster
