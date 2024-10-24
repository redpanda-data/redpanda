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
#include "security/acl.h"
#include "security/types.h"

#include <seastar/core/sstring.hh>

#include <iosfwd>

namespace security {

/*
 * ephemeral_credential is the client side of a scram_credential.
 *
 * Since it contains a password it should not be serialized.
 **/
class ephemeral_credential {
public:
    ephemeral_credential() noexcept = default;
    ephemeral_credential(
      acl_principal principal,
      credential_user user,
      credential_password password,
      ss::sstring mechanism) noexcept
      : _principal(std::move(principal))
      , _user(std::move(user))
      , _password(std::move(password))
      , _mechanism(std::move(mechanism)) {}

    const acl_principal& principal() const { return _principal; }
    const credential_user& user() const { return _user; }
    const credential_password& password() const { return _password; }
    const ss::sstring& mechanism() const { return _mechanism; }

private:
    friend bool
    operator==(const ephemeral_credential&, const ephemeral_credential&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const ephemeral_credential&);

    acl_principal _principal;
    credential_user _user;
    credential_password _password;
    ss::sstring _mechanism;
};

} // namespace security
