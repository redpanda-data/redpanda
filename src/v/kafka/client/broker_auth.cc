// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "kafka/client/broker.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/protocol/sasl_handshake.h"
#include "random/generators.h"
#include "security/oidc_authenticator.h"
#include "security/plain_authenticator.h"
#include "security/scram_authenticator.h"

#include <seastar/core/coroutine.hh>

namespace kafka::client {

ss::future<std::optional<api_version_range>>
remote_broker::get_supported_versions(
  api_key key, std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (_supported_versions.empty()) {
        co_await maybe_reconnect(as);
    }

    auto it = _supported_versions.find(key);
    if (it == _supported_versions.end()) {
        co_return std::nullopt;
    }
    co_return it->second;
}

ss::future<> remote_broker::do_authenticate() {
    if (!_config->sasl_cfg.has_value()) {
        co_return;
    }
    const auto& mechanism = _config->sasl_cfg->mechanism;

    if (
      mechanism != security::scram_sha256_authenticator::name
      && mechanism != security::scram_sha512_authenticator::name
      && mechanism != security::oidc::sasl_authenticator::name
      && mechanism != security::plain_authenticator::name) {
        throw broker_error{
          _node_id,
          error_code::sasl_authentication_failed,
          fmt_with_ctx(ssx::sformat, "Unknown mechanism: {}", mechanism)};
    }

    const auto& username = _config->sasl_cfg->username;
    const auto& password = _config->sasl_cfg->password;

    vlog(
      _logger.debug,
      "Connecting to broker {} with authentication: {}:{}",
      _node_id,
      mechanism,
      username);

    if (username.empty() || password.empty()) {
        throw broker_error{
          _node_id,
          error_code::sasl_authentication_failed,
          "Username or password is empty"};
    }
    // perform handshake
    co_await do_sasl_handshake(mechanism);

    if (mechanism == security::scram_sha256_authenticator::name) {
        co_await do_authenticate_scram256(username, password);
    } else if (mechanism == security::scram_sha512_authenticator::name) {
        co_await do_authenticate_scram512(username, password);
    } else if (mechanism == security::oidc::sasl_authenticator::name) {
        co_await do_authenticate_oauthbearer(password);
    } else if (mechanism == security::plain_authenticator::name) {
        co_await do_authenticate_plain(username, password);
    } else {
        throw broker_error{
          _node_id,
          error_code::sasl_authentication_failed,
          fmt_with_ctx(ssx::sformat, "Unknown mechanism: {}", mechanism)};
    }
}

ss::future<> remote_broker::do_sasl_handshake(ss::sstring mechanism) {
    sasl_handshake_request req;
    req.data.mechanism = std::move(mechanism);
    auto resp = co_await do_dispatch(req, get_sasl_handshake_request_version());
    if (resp.data.error_code != error_code::none) {
        throw broker_error{_node_id, resp.data.error_code};
    }
}

ss::future<security::server_first_message>
remote_broker::send_scram_client_first(
  const security::client_first_message& client_first) {
    sasl_authenticate_request client_first_req;
    {
        auto msg = client_first.message();
        client_first_req.data.auth_bytes = bytes(msg.cbegin(), msg.cend());
    }
    auto client_first_resp = co_await do_dispatch(
      client_first_req, get_sasl_authenticate_request_version());
    if (client_first_resp.data.error_code != error_code::none) {
        throw broker_error{
          _node_id,
          client_first_resp.data.error_code,
          client_first_resp.data.error_message.value_or("<no error message>")};
    }
    co_return security::server_first_message(client_first_resp.data.auth_bytes);
}

ss::future<security::server_final_message>
remote_broker::send_scram_client_final(
  const security::client_final_message& client_final) {
    sasl_authenticate_request client_last_req;
    {
        auto msg = client_final.message();
        client_last_req.data.auth_bytes = bytes(msg.cbegin(), msg.cend());
    }

    auto client_last_resp = co_await do_dispatch(
      client_last_req, get_sasl_authenticate_request_version());

    if (client_last_resp.data.error_code != error_code::none) {
        throw broker_error{
          _node_id,
          client_last_resp.data.error_code,
          client_last_resp.data.error_message.value_or("<no error message>")};
    }

    co_return security::server_final_message(client_last_resp.data.auth_bytes);
}

template<typename ScramAlgo>
ss::future<> remote_broker::do_authenticate_scram(
  ss::sstring username, ss::sstring password) {
    /*
     * send client first message
     */
    const auto nonce = random_generators::gen_alphanum_string(130);
    const security::client_first_message client_first(
      std::move(username), nonce);

    /*
     * handle server first response
     */
    const auto server_first = co_await send_scram_client_first(client_first);

    if (!std::string_view(server_first.nonce())
           .starts_with(std::string_view(nonce))) {
        throw broker_error{
          _node_id,
          error_code::sasl_authentication_failed,
          "Server nonce doesn't match client nonce"};
    }

    if (server_first.iterations() < ScramAlgo::min_iterations) {
        throw broker_error{
          _node_id,
          error_code::sasl_authentication_failed,
          fmt_with_ctx(
            ssx::sformat,
            "Server minimum iterations {} < required {}",
            server_first.iterations(),
            ScramAlgo::min_iterations)};
    }

    /*
     * send client final message
     */
    security::client_final_message client_final(
      bytes::from_string("n,,"), server_first.nonce());

    auto salted_password = ScramAlgo::hi(
      bytes(password.cbegin(), password.cend()),
      server_first.salt(),
      server_first.iterations());

    client_final.set_proof(
      ScramAlgo::client_proof(
        salted_password, client_first, server_first, client_final));

    const auto server_final = co_await send_scram_client_final(client_final);

    /*
     * handle server final response
     */
    if (server_final.error()) {
        throw broker_error{
          _node_id,
          error_code::sasl_authentication_failed,
          server_final.error().value()};
    }

    auto server_key = ScramAlgo::server_key(salted_password);
    auto server_sig = ScramAlgo::server_signature(
      server_key, client_first, server_first, client_final);

    if (server_final.signature() != server_sig) {
        throw broker_error{
          _node_id,
          error_code::sasl_authentication_failed,
          "Server signature does not match calculated signature"};
    }
}

ss::future<> remote_broker::do_authenticate_scram256(
  ss::sstring username, ss::sstring password) {
    return do_authenticate_scram<security::scram_sha256>(
      std::move(username), std::move(password));
}

ss::future<> remote_broker::do_authenticate_scram512(
  ss::sstring username, ss::sstring password) {
    return do_authenticate_scram<security::scram_sha512>(
      std::move(username), std::move(password));
}

ss::future<> remote_broker::do_authenticate_oauthbearer(ss::sstring token) {
    sasl_authenticate_request req;
    req.data.auth_bytes = bytes::from_string(
      fmt::format("n,,\1auth={}\1\1", token));
    auto res = co_await do_dispatch(
      std::move(req), get_sasl_authenticate_request_version());
    if (res.data.errored()) {
        throw broker_error{
          _node_id,
          res.data.error_code,
          res.data.error_message.value_or("<no error message>")};
    }
}

ss::future<> remote_broker::do_authenticate_plain(
  ss::sstring username, ss::sstring password) {
    sasl_authenticate_request req;
    std::string bytes;
    // 2 - number of null characters in the PLAIN auth message
    bytes.reserve(2 + username.size() + password.size());
    bytes.push_back('\0');
    bytes.append(username.cbegin(), username.cend());
    bytes.push_back('\0');
    bytes.append(password.cbegin(), password.cend());
    req.data.auth_bytes = bytes::from_string(std::move(bytes));
    auto res = co_await do_dispatch(
      std::move(req), get_sasl_authenticate_request_version());
    if (res.data.errored()) {
        throw broker_error{
          _node_id,
          res.data.error_code,
          res.data.error_message.value_or("<no error message>")};
    }
}

namespace {
template<typename ReqT>
api_version get_auth_request_version(
  model::node_id id,
  const absl::flat_hash_map<api_key, api_version_range>& versions) {
    using api_t = typename ReqT::api_type;
    auto it = versions.find(api_t::key);
    if (it == versions.end()) {
        throw broker_error(
          id,
          error_code::unsupported_version,
          fmt_with_ctx(
            ssx::sformat,
            "Broker does not support required {} request",
            api_t::name));
    }
    return std::min(it->second.max, api_t::max_valid);
}
} // namespace

api_version remote_broker::get_sasl_authenticate_request_version() const {
    return get_auth_request_version<sasl_authenticate_request>(
      _node_id, _supported_versions);
}

api_version remote_broker::get_sasl_handshake_request_version() const {
    return get_auth_request_version<sasl_handshake_request>(
      _node_id, _supported_versions);
}

} // namespace kafka::client
