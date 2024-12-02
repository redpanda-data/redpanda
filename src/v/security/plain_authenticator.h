/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "security/fwd.h"
#include "security/sasl_authentication.h"

namespace security {

/**
 * @class plain_authenticator
 * @brief A class that implements SASL/PLAIN authentication mechanism.
 *
 * This class is responsible for handling the SASL/PLAIN authentication process.
 * It authenticates the username and password provided by the client against
 * SCRAM users in the credential store.
 */
class plain_authenticator final : public sasl_mechanism {
public:
    static constexpr const char* name = "PLAIN";

    explicit plain_authenticator(credential_store& credentials)
      : _credentials(credentials) {}

    ss::future<result<bytes>> authenticate(bytes auth_bytes) override;

    bool complete() const override { return _state == state::complete; }
    bool failed() const override { return _state == state::failed; }

    const acl_principal& principal() const override {
        vassert(
          _state == state::complete,
          "Authentication id is not valid until auth process complete");
        return _principal;
    }

    const audit::user& audit_user() const override { return _audit_user; }

    const char* mechanism_name() const override { return "SASL-PLAIN"; }

private:
    enum class state {
        init,
        complete,
        failed,
    };

    state _state{state::init};
    credential_store& _credentials;
    acl_principal _principal;
    security::audit::user _audit_user;
};

} // namespace security
