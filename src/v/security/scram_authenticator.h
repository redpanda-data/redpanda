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
#include "security/acl.h"
#include "security/fwd.h"
#include "security/sasl_authentication.h"
#include "security/scram_algorithm.h"
#include "security/types.h"

namespace security {

template<typename ScramMechanism>
struct scram_mechanism_traits;

template<typename ScramMechanism>
class scram_authenticator final : public sasl_mechanism {
    static constexpr int nonce_size = 130;

public:
    using scram = ScramMechanism;

    explicit scram_authenticator(credential_store& credentials)
      : _state{state::client_first_message}
      , _credentials(credentials)
      , _audit_user() {}

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

    const char* mechanism_name() const override {
        return scram_mechanism_traits<scram>::name;
    }

private:
    enum class state {
        client_first_message,
        client_final_message,
        complete,
        failed,
    };

    // handlers for client messages
    result<bytes> handle_client_first(bytes_view);
    result<bytes> handle_client_final(bytes_view);
    result<bytes> handle_next(bytes_view);

    void clear_credentials();

    state _state;
    credential_store& _credentials;
    acl_principal _principal;
    security::audit::user _audit_user;

    // populated during authentication process
    std::unique_ptr<scram_credential> _credential;
    std::unique_ptr<client_first_message> _client_first;
    std::unique_ptr<server_first_message> _server_first;
};

template<>
struct scram_mechanism_traits<scram_sha256> {
    static constexpr const char* name = "SCRAM-SHA-256";
    static constexpr scram_algorithm_t algorithm = scram_algorithm_t::sha256;
};

template<>
struct scram_mechanism_traits<scram_sha512> {
    static constexpr const char* name = "SCRAM-SHA-512";
    static constexpr scram_algorithm_t algorithm = scram_algorithm_t::sha512;
};

struct scram_sha256_authenticator {
    using auth = scram_authenticator<scram_sha256>;
    static constexpr const char* name
      = scram_mechanism_traits<scram_sha256>::name;
};

struct scram_sha512_authenticator {
    using auth = scram_authenticator<scram_sha512>;
    static constexpr const char* name
      = scram_mechanism_traits<scram_sha512>::name;
};

class scram_credential_cache;

/// Validates a plaintext password against a stored SCRAM credential, for
/// non-SCRAM authentication such as HTTP Basic auth and SASL/PLAIN. Returns
/// the credential's SASL mechanism name on success.
///
/// Consults the shard's scram_credential_cache to skip the expensive salted
/// password derivation for recently validated (password, credential) pairs,
/// unless disabled via the scram_credential_cache_enabled config.
std::optional<std::string_view> validate_scram_credential(
  const scram_credential& cred, const credential_password& password);

namespace detail {
/// As above, against a caller-provided cache.
///
/// Neither the cache pointer nor any view it returns may be held across a
/// scheduling point: a config watch may flush (destroy) the cache between
/// suspensions. Validation is currently synchronous end to end, which is
/// what makes them safe.
std::optional<std::string_view> validate_scram_credential(
  const scram_credential& cred,
  const credential_password& password,
  scram_credential_cache* cache);
} // namespace detail
} // namespace security
