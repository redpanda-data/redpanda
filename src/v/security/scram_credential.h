/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "reflection/adl.h"
#include "security/acl.h"
#include "serde/envelope.h"
#include "serde/rw/bytes.h"

#include <iosfwd>

namespace security {

class scram_credential
  : public serde::
      envelope<scram_credential, serde::version<0>, serde::compat_version<0>> {
public:
    scram_credential() noexcept = default;

    scram_credential(
      bytes salt, bytes server_key, bytes stored_key, int iterations) noexcept
      : _salt(std::move(salt))
      , _server_key(std::move(server_key))
      , _stored_key(std::move(stored_key))
      , _iterations(iterations) {}

    scram_credential(
      bytes salt,
      bytes server_key,
      bytes stored_key,
      int iterations,
      acl_principal principal) noexcept
      : _salt(std::move(salt))
      , _server_key(std::move(server_key))
      , _stored_key(std::move(stored_key))
      , _iterations(iterations)
      , _principal(std::move(principal)) {}

    const bytes& salt() const { return _salt; }
    const bytes& server_key() const { return _server_key; }
    const bytes& stored_key() const { return _stored_key; }
    int iterations() const { return _iterations; }
    const std::optional<acl_principal>& principal() const { return _principal; }

    bool operator==(const scram_credential&) const = default;

    auto serde_fields() {
        return std::tie(_salt, _server_key, _stored_key, _iterations);
    }

private:
    friend std::ostream& operator<<(std::ostream&, const scram_credential&);

    bytes _salt;
    bytes _server_key;
    bytes _stored_key;
    int _iterations{0};
    // Principal is not serialized on disk, it is sent over internal rpc
    std::optional<acl_principal> _principal;
};

} // namespace security

// TODO: avoid bytes-to-iobuf conersion. either add bytes specialization to
// reflection or wait for reflection-v2 which will have a new interface. in
// either case, this is only used when managing users not on a hot path.
namespace reflection {
template<>
struct adl<security::scram_credential> {
    static constexpr int8_t current_version = 1;

    void to(iobuf& out, security::scram_credential&& c) {
        adl<int8_t>{}.to(out, current_version);
        serialize(
          out,
          bytes_to_iobuf(c.salt()),
          bytes_to_iobuf(c.server_key()),
          bytes_to_iobuf(c.stored_key()),
          static_cast<int32_t>(c.iterations()));
    }

    security::scram_credential from(iobuf_parser& in) {
        auto version = adl<int8_t>{}.from(in);
        vassert(
          version == current_version,
          "Unexpected scram credential version {} (expected {})",
          version,
          current_version);
        auto salt = adl<iobuf>{}.from(in);
        auto server_key = adl<iobuf>{}.from(in);
        auto stored_key = adl<iobuf>{}.from(in);
        auto iterations = adl<int32_t>{}.from(in);
        return security::scram_credential(
          iobuf_to_bytes(salt),
          iobuf_to_bytes(server_key),
          iobuf_to_bytes(stored_key),
          iterations);
    }
};
} // namespace reflection
