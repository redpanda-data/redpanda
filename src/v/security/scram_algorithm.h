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
#include "bytes/bytes.h"
#include "hashing/secure.h"
#include "net/types.h"
#include "random/generators.h"
#include "security/scram_credential.h"
#include "ssx/sformat.h"
#include "utils/base64.h"

#include <absl/container/node_hash_map.h>

/**
 * scram algorthm - https://tools.ietf.org/html/rfc5802
 *
 * The implementation follows closely the rfc, with some scenarios and
 * configuration not handled by kafka omitted from the implementation. The kafka
 * implementation is closely based on the rfc as well, so they are fairly easy
 * to compare.
 *
 * TODO
 * ====
 *
 * 1. unnecessary copying from std::array<char> for hash digest types and
 * built-in bytes type. the conversion is mostly mechanical, but std::array
 * needs to be propogated to all other types which need to be templated on the
 * same scram_algorithm template parameters.
 */
namespace security {

class scram_exception final : public net::authentication_exception {
public:
    explicit scram_exception(ss::sstring msg) noexcept
      : net::authentication_exception(std::move(msg)) {}
};

/**
 * First message received by the server.
 */
class client_first_message {
public:
    client_first_message(ss::sstring username, ss::sstring nonce)
      : _username(std::move(username))
      , _nonce(std::move(nonce)) {}

    explicit client_first_message(bytes_view data);

    client_first_message(client_first_message&&) = delete;
    client_first_message& operator=(client_first_message&&) = delete;
    client_first_message(const client_first_message&) = delete;
    client_first_message& operator=(const client_first_message&) = delete;
    ~client_first_message() noexcept = default;

    const ss::sstring& authzid() const { return _authzid; }
    const ss::sstring& username() const { return _username; }
    ss::sstring username_normalized() const;
    const ss::sstring& nonce() const { return _nonce; }

    ss::sstring bare_message() const {
        return ssx::sformat("n={},r={}", _username, _nonce);
    }

    ss::sstring message() const {
        return ssx::sformat("n,{},{}", _authzid, bare_message());
    }

    bool token_authenticated() const;

private:
    friend std::ostream& operator<<(std::ostream&, const client_first_message&);

    ss::sstring _authzid;
    ss::sstring _username;
    ss::sstring _nonce;
    absl::node_hash_map<ss::sstring, ss::sstring> _extensions;
};

/**
 * Reply to client's first message.
 */
class server_first_message {
public:
    server_first_message(
      const ss::sstring& client_nonce,
      const ss::sstring& server_nonce,
      bytes salt,
      int iterations) noexcept
      : _nonce(client_nonce + server_nonce)
      , _salt(std::move(salt))
      , _iterations(iterations) {}

    explicit server_first_message(bytes_view);

    server_first_message(server_first_message&&) noexcept = default;
    server_first_message& operator=(server_first_message&&) = delete;
    server_first_message(const server_first_message&) = delete;
    server_first_message& operator=(const server_first_message&) = delete;
    ~server_first_message() noexcept = default;

    ss::sstring sasl_message() const;

    const ss::sstring& nonce() const { return _nonce; }
    const bytes& salt() const { return _salt; }
    int iterations() const { return _iterations; }

private:
    friend std::ostream& operator<<(std::ostream&, const server_first_message&);

    ss::sstring _nonce;
    bytes _salt;
    int _iterations;
};

/**
 * Final client message expected by server.
 */
class client_final_message {
public:
    explicit client_final_message(bytes_view data);

    client_final_message(bytes channel_binding, ss::sstring nonce)
      : _channel_binding(std::move(channel_binding))
      , _nonce(std::move(nonce)) {}

    client_final_message(client_final_message&&) = delete;
    client_final_message& operator=(client_final_message&&) = delete;
    client_final_message(const client_final_message&) = delete;
    client_final_message& operator=(const client_final_message&) = delete;
    ~client_final_message() noexcept = default;

    const ss::sstring& nonce() const { return _nonce; }
    const bytes& proof() const { return _proof; }
    const bytes& channel_binding() const { return _channel_binding; }
    ss::sstring msg_no_proof() const;

    void set_proof(bytes proof) { _proof = std::move(proof); }

    ss::sstring message() const {
        return ssx::sformat("{},p={}", msg_no_proof(), bytes_to_base64(_proof));
    }

private:
    friend std::ostream& operator<<(std::ostream&, const client_final_message&);

    bytes _channel_binding;
    ss::sstring _nonce;
    ss::sstring _extensions;
    bytes _proof;
};

/**
 * Final reply from the server.
 */
class server_final_message {
public:
    server_final_message(
      std::optional<ss::sstring> error, bytes signature) noexcept
      : _error(std::move(error))
      , _signature(std::move(signature)) {}

    explicit server_final_message(bytes_view);

    server_final_message(server_final_message&&) noexcept = default;
    server_final_message& operator=(server_final_message&&) = delete;
    server_final_message(const server_final_message&) = delete;
    server_final_message& operator=(const server_final_message&) = delete;
    ~server_final_message() noexcept = default;

    ss::sstring sasl_message() const;
    const std::optional<ss::sstring>& error() const { return _error; }
    const bytes& signature() const { return _signature; }

private:
    friend std::ostream& operator<<(std::ostream&, const server_final_message&);

    std::optional<ss::sstring> _error;
    bytes _signature;
};

template<
  typename MacType,
  typename HashType,
  size_t SaltSize,
  int MinIterations>
class scram_algorithm {
public:
    static constexpr int min_iterations = MinIterations;
    static_assert(min_iterations > 0, "Minimum iterations must be positive");

    static bytes client_signature(
      bytes_view stored_key,
      const client_first_message& client_first,
      const server_first_message& server_first,
      const client_final_message& client_final) {
        MacType mac(stored_key);
        mac.update(auth_message(client_first, server_first, client_final));
        auto result = mac.reset();
        return bytes(result.begin(), result.end());
    }

    static bytes server_signature(
      bytes_view server_key,
      const client_first_message& client_first,
      const server_first_message& server_first,
      const client_final_message& client_final) {
        MacType mac(server_key);
        mac.update(auth_message(client_first, server_first, client_final));
        auto result = mac.reset();
        return bytes(result.begin(), result.end());
    }

    static bytes
    computed_stored_key(bytes_view client_signature, bytes_view client_proof) {
        HashType hash;
        hash.update(client_signature ^ client_proof);
        auto result = hash.reset();
        return bytes(result.begin(), result.end());
    }

    /**
     * helper for building credentials either for building test cases or
     * computing credentials to be stored based on client requests.
     */
    static scram_credential
    make_credentials(const ss::sstring& password, int iterations) {
        bytes salt = random_generators::get_bytes(SaltSize);
        bytes salted_password = salt_password(password, salt, iterations);
        auto clientkey = client_key(salted_password);
        auto storedkey = stored_key(clientkey);
        auto serverkey = server_key(salted_password);
        return scram_credential(
          std::move(salt),
          std::move(serverkey),
          std::move(storedkey),
          iterations);
    }
    static scram_credential make_credentials(
      acl_principal principal, const ss::sstring& password, int iterations) {
        bytes salt = random_generators::get_bytes(SaltSize);
        bytes salted_password = salt_password(password, salt, iterations);
        auto clientkey = client_key(salted_password);
        auto storedkey = stored_key(clientkey);
        auto serverkey = server_key(salted_password);
        return scram_credential(
          std::move(salt),
          std::move(serverkey),
          std::move(storedkey),
          iterations,
          std::move(principal));
    }

    static bytes client_proof(
      bytes_view salted_password,
      const client_first_message& client_first,
      const server_first_message& server_first,
      const client_final_message& client_final) {
        auto c_key = client_key(salted_password);
        HashType hash;
        hash.update(c_key);
        auto stored_key = hash.reset();
        auto c_signature = client_signature(
          bytes(stored_key.begin(), stored_key.end()),
          client_first,
          server_first,
          client_final);
        return c_key ^ c_signature;
    }

    static bytes hi(bytes_view str, bytes_view salt, int iterations) {
        MacType mac(str);
        mac.update(salt);
        mac.update(std::array<char, 4>{0, 0, 0, 1});
        auto u1 = mac.reset();
        auto prev = u1;
        auto result = u1;
        for (int i = 2; i <= iterations; i++) {
            mac.update(prev);
            auto ui = mac.reset();
            result = result ^ ui;
            prev = ui;
        }
        return bytes(result.begin(), result.end());
    }

    static bytes server_key(bytes_view salted_password) {
        MacType mac(salted_password);
        mac.update("Server Key");
        auto result = mac.reset();
        return bytes(result.begin(), result.end());
    }

    /**
     * For doing non-SCRAM authentication, such as HTTP basic auth over TLS,
     * using stored SCRAM credentials.
     */
    static bool validate_password(
      const ss::sstring& password,
      bytes_view reference_stored_key,
      bytes_view salt,
      int iterations) {
        auto salted_password = salt_password(password, salt, iterations);
        auto clientkey = client_key(salted_password);
        auto storedkey = stored_key(clientkey);
        return storedkey == reference_stored_key;
    }

private:
    static bytes salt_password(
      const ss::sstring& password, bytes_view salt, int iterations) {
        bytes password_bytes(password.begin(), password.end());
        return hi(password_bytes, salt, iterations);
    }

    static bytes client_key(bytes_view salted_password) {
        MacType mac(salted_password);
        mac.update("Client Key");
        auto result = mac.reset();
        return bytes(result.begin(), result.end());
    }

    static bytes stored_key(bytes_view client_key) {
        HashType hash;
        hash.update(client_key);
        auto result = hash.reset();
        return bytes(result.begin(), result.end());
    }

    static ss::sstring auth_message(
      const client_first_message& client_first,
      const server_first_message& server_first,
      const client_final_message& client_final) {
        return ssx::sformat(
          "{},{},{}",
          client_first.bare_message(),
          server_first.sasl_message(),
          client_final.msg_no_proof());
    }
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
using scram_sha512 = scram_algorithm<hmac_sha512, hash_sha512, 130, 4096>;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
using scram_sha256 = scram_algorithm<hmac_sha256, hash_sha256, 130, 4096>;

} // namespace security
