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
#include "security/scram_authenticator.h"

#include "base/vlog.h"
#include "config/configuration.h"
#include "random/secure_generators.h"
#include "security/credential_store.h"
#include "security/errc.h"
#include "security/logger.h"
#include "security/scram_credential_cache.h"

namespace security {

template<typename T>
result<bytes>
scram_authenticator<T>::handle_client_first(bytes_view auth_bytes) {
    // request from client
    _client_first = std::make_unique<client_first_message>(auth_bytes);
    vlog(seclog.debug, "Received client first message {}", *_client_first);

    // lookup credentials for this user
    auto authid = _client_first->username_normalized();
    _audit_user.name = authid;
    auto credential = _credentials.get<scram_credential>(
      credential_user(authid));
    if (!credential) {
        return errc::invalid_credentials;
    }
    _principal = credential->principal().value_or(
      acl_principal{principal_type::user, authid});
    _credential = std::make_unique<scram_credential>(std::move(*credential));
    _audit_user.name = _principal.name();
    const auto principal_type_to_audit_type = [](principal_type type) {
        switch (type) {
        case principal_type::user:
            return audit::user::type::user;
        case principal_type::ephemeral_user:
            return audit::user::type::system;
        case principal_type::role:
        case principal_type::group:
            return audit::user::type::other;
        }
    };
    _audit_user.type_id = principal_type_to_audit_type(_principal.type());

    if (
      !_client_first->authzid().empty() && _client_first->authzid() != authid) {
        vlog(seclog.info, "Invalid authorization id and username pair");
        return errc::invalid_credentials;
    }

    if (_credential->iterations() < scram::min_iterations) {
        vlog(
          seclog.info,
          "Requested iterations less than minimum {}",
          scram::min_iterations);
        return errc::invalid_credentials;
    }

    // build server reply
    _server_first = std::make_unique<server_first_message>(
      _client_first->nonce(),
      random_generators::gen_alphanum_string_secure(nonce_size),
      _credential->salt(),
      _credential->iterations());

    _state = state::client_final_message;

    auto reply = _server_first->sasl_message();
    return bytes(reply.cbegin(), reply.cend());
}

template<typename T>
result<bytes>
scram_authenticator<T>::handle_client_final(bytes_view auth_bytes) {
    client_final_message client_final(auth_bytes);
    vlog(seclog.debug, "Received client final message {}", client_final);

    auto client_signature = scram::client_signature(
      _credential->stored_key(), *_client_first, *_server_first, client_final);

    auto computed_stored_key = scram::computed_stored_key(
      client_signature,
      bytes(client_final.proof().cbegin(), client_final.proof().cend()));

    if (computed_stored_key != _credential->stored_key()) {
        vlog(
          seclog.info,
          "Authentication failed: stored and client submitted credentials do "
          "not match");
        return errc::invalid_credentials;
    }

    const auto username = _client_first->username();
    vlog(seclog.debug, "Authentication key match for user {}", username);

    auto server_signature = scram::server_signature(
      _credential->server_key(), *_client_first, *_server_first, client_final);

    server_final_message server_final(std::nullopt, server_signature);
    auto reply = server_final.sasl_message();

    clear_credentials();
    _state = state::complete;

    return bytes(reply.cbegin(), reply.cend());
}

template<typename T>
void scram_authenticator<T>::clear_credentials() {
    _credential.reset();
    _client_first.reset();
    _server_first.reset();
}

template<typename T>
result<bytes> scram_authenticator<T>::handle_next(bytes_view auth_bytes) {
    switch (_state) {
    case state::client_first_message:
        return handle_client_first(auth_bytes);

    case state::client_final_message:
        return handle_client_final(auth_bytes);

    default:
        return errc::invalid_scram_state;
    }
}

template<typename T>
ss::future<result<bytes>>
scram_authenticator<T>::authenticate(bytes auth_bytes) {
    auto ret = handle_next(auth_bytes);
    if (!ret) {
        _state = state::failed;
        clear_credentials();
    }
    co_return ret;
}

template class scram_authenticator<scram_sha256>;
template class scram_authenticator<scram_sha512>;

namespace {

// Matches a password against a stored credential by rederiving its stored
// key. When a cache is given, the derivation is memoized; a null cache
// derives every time (the plain, un-memoized path).
template<typename scram>
bool match_password(
  scram_credential_cache* cache,
  const scram_credential& cred,
  const credential_password& password) {
    constexpr auto mech = scram_mechanism_traits<scram>::algorithm;
    if (cache != nullptr) {
        auto cached = cache->get(
          mech, password, cred.salt(), cred.iterations());
        if (cached != nullptr) {
            return cached->data == cred.stored_key();
        }
    }
    auto stored_key = scram::derive_stored_key(
      password(), cred.salt(), cred.iterations());
    auto valid = stored_key == cred.stored_key();
    if (cache != nullptr) {
        cache->put(
          mech,
          password,
          cred.salt(),
          cred.iterations(),
          std::move(stored_key));
    }
    return valid;
}
} // namespace

namespace detail {

std::optional<std::string_view> validate_scram_credential(
  const scram_credential& cred,
  const credential_password& password,
  scram_credential_cache* cache) {
    std::optional<std::string_view> sasl_mechanism;
    if (
      cred.stored_key().size() == scram_sha256::key_size
      && match_password<scram_sha256>(cache, cred, password)) {
        sasl_mechanism = scram_sha256_authenticator::name;
    } else if (
      cred.stored_key().size() == scram_sha512::key_size
      && match_password<scram_sha512>(cache, cred, password)) {
        sasl_mechanism = scram_sha512_authenticator::name;
    }

    return sasl_mechanism;
}
} // namespace detail

std::optional<std::string_view> validate_scram_credential(
  const scram_credential& cred, const credential_password& password) {
    // Constructed on first use, which forces the shard's configuration
    // thread_local into existence first and therefore destroys the holder
    // (and its config binding) before the configuration at thread exit.
    static thread_local scram_credential_cache_holder holder{
      config::shard_local_cfg().scram_credential_cache_enabled.bind(),
      scram_credential_cache::default_capacity};
    return detail::validate_scram_credential(cred, password, holder.get());
}

} // namespace security
