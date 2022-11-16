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

#include "cluster/errc.h"
#include "security/acl.h"
#include "security/ephemeral_credential.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "serde/envelope.h"

#include <iosfwd>

namespace cluster {

struct put_ephemeral_credential_request
  : serde::envelope<
      put_ephemeral_credential_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    put_ephemeral_credential_request() = default;
    explicit put_ephemeral_credential_request(
      security::acl_principal principal,
      security::credential_user user,
      security::scram_credential cred)
      : principal{std::move(principal)}
      , user{std::move(user)}
      , credential{std::move(cred)} {}

    auto serde_fields() { return std::tie(principal, user, credential); }

    security::acl_principal principal;
    security::credential_user user;
    security::scram_credential credential;
};

struct put_ephemeral_credential_reply
  : serde::envelope<
      put_ephemeral_credential_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    put_ephemeral_credential_reply() = default;
    explicit put_ephemeral_credential_reply(errc err)
      : err(err) {}

    auto serde_fields() { return std::tie(err); }

    errc err{errc::success};
};

} // namespace cluster
