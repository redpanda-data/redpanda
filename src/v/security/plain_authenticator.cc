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
#include "security/plain_authenticator.h"

#include "base/vlog.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/errc.h"
#include "security/logger.h"
#include "security/scram_authenticator.h"
#include "security/types.h"
#include "strings/utf8.h"

#include <seastar/util/defer.hh>

namespace security {

ss::future<result<bytes>> plain_authenticator::authenticate(bytes auth_bytes) {
    constexpr size_t max_length{255};
    constexpr std::string_view sep{"\0", 1};

    auto make_failed = ss::defer([this] { _state = state::failed; });

    if (_state != state::init) {
        vlog(
          seclog.warn,
          "invalid plain state: {}",
          _state == state::failed ? "failed" : "complete");
        co_return errc::invalid_credentials;
    }

    auto auth_str = std::string_view(
      reinterpret_cast<char*>(auth_bytes.data()), auth_bytes.size());

    if (!is_valid_utf8(auth_str)) {
        vlog(seclog.warn, "invalid utf8");
        co_return errc::invalid_credentials;
    }

    // [authorization identity] not supported
    if (!auth_str.starts_with(sep)) {
        vlog(seclog.warn, "[authorization identity] not supported");
        co_return errc::invalid_credentials;
    }
    auth_str = auth_str.substr(sep.length());
    auto it = auth_str.find(sep);
    if (std::string_view::npos == it) {
        vlog(seclog.warn, "seperator not found");
        co_return errc::invalid_credentials;
    }

    credential_user username{auth_str.substr(0, it)};
    credential_password password{auth_str.substr(it + sep.length())};

    if (username().empty()) {
        vlog(seclog.warn, "username not found");
        co_return errc::invalid_credentials;
    }

    if (username().length() > max_length) {
        vlog(seclog.warn, "username too long");
        co_return errc::invalid_credentials;
    }

    if (password().empty()) {
        vlog(seclog.warn, "password not found");
        co_return errc::invalid_credentials;
    }

    if (password().length() > max_length) {
        vlog(seclog.warn, "password too long");
        co_return errc::invalid_credentials;
    }

    _audit_user.name = username;
    auto cred = _credentials.get<scram_credential>(username);
    if (!cred.has_value()) {
        vlog(seclog.warn, "credential not found");
        co_return errc::invalid_credentials;
    }

    if (!validate_scram_credential(*cred, password).has_value()) {
        vlog(seclog.warn, "scram authentication failed");
        co_return errc::invalid_credentials;
    }

    vlog(seclog.trace, "Authenticated user {}", username);

    make_failed.cancel();

    _principal = cred->principal().value_or(
      acl_principal{principal_type::user, username()});
    _audit_user.name = _principal.name();
    _audit_user.type_id = audit::user::type::user;

    _state = state::complete;
    co_return bytes{};
}

} // namespace security
