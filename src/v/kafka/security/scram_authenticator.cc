/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/security/scram_authenticator.h"

#include "kafka/security/errc.h"
#include "vlog.h"

namespace kafka {

template<typename T>
result<bytes>
scram_authenticator<T>::handle_client_first(bytes_view auth_bytes) {
    // request from client
    _client_first = std::make_unique<client_first_message>(auth_bytes);
    vlog(klog.debug, "Received client first message {}", *_client_first);

    // lookup credentials for this user
    _authid = _client_first->username_normalized();
    auto credential = _credentials.get<scram_credential>(_authid);
    if (!credential) {
        return errc::invalid_credentials;
    }
    _credential = std::make_unique<scram_credential>(std::move(*credential));

    if (
      !_client_first->authzid().empty()
      && _client_first->authzid() != _authid) {
        vlog(klog.info, "Invalid authorization id and username pair");
        return errc::invalid_credentials;
    }

    if (_credential->iterations() < scram::min_iterations) {
        vlog(
          klog.info,
          "Requested iterations {} less than minimum {}",
          _credential->iterations(),
          scram::min_iterations);
        return errc::invalid_credentials;
    }

    // build server reply
    _server_first = std::make_unique<server_first_message>(
      _client_first->nonce(),
      random_generators::gen_alphanum_string(nonce_size),
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
    vlog(klog.debug, "Received client final message {}", client_final);

    auto client_signature = scram::client_signature(
      _credential->stored_key(), *_client_first, *_server_first, client_final);

    auto computed_stored_key = scram::computed_stored_key(
      client_signature,
      bytes(client_final.proof().begin(), client_final.proof().end()));

    if (computed_stored_key != _credential->stored_key()) {
        vlog(
          klog.info,
          "Authentication failed: stored and client submitted credentials do "
          "not match");
        return errc::invalid_credentials;
    }

    const auto username = _client_first->username();
    vlog(klog.debug, "Authentication key match for user {}", username);

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
result<bytes> scram_authenticator<T>::authenticate(bytes_view auth_bytes) {
    auto ret = handle_next(auth_bytes);
    if (!ret) {
        _state = state::failed;
        clear_credentials();
    }
    return ret;
}

template class scram_authenticator<scram_sha256>;
template class scram_authenticator<scram_sha512>;

} // namespace kafka
