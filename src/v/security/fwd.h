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

#pragma once

namespace security {

class acl_matches;
class acl_store;
class authorizer;
class credential_store;
class ephemeral_credential_store;
class role;
class role_member;
class role_member_view;
class role_store;

namespace oidc {

class jws;
class jwt;
class service;
class principal_mapping_rule;
class verifier;

} // namespace oidc

namespace audit {

class audit_log_manager;

} // namespace audit

} // namespace security
