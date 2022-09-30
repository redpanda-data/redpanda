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
#include "bytes/bytes.h"
#include "bytes/fwd.h"
#include "cluster/errc.h"
#include "model/timeout_clock.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

#include <iosfwd>

namespace cluster {

class internal_secret {
public:
    using key_t = named_type<bytes, struct key_tag>;
    using value_t = named_type<bytes, struct value_tag>;

    internal_secret() noexcept = default;

    internal_secret(bytes key, bytes value) noexcept
      : _key(std::move(key))
      , _value(std::move(value)) {}

    const key_t& key() const { return _key; }
    const value_t& value() const { return _value; }

private:
    friend bool operator==(const internal_secret&, const internal_secret&)
      = default;
    friend bool operator<(const internal_secret& l, const internal_secret& r) {
        return std::tie(l._key, l._value) < std::tie(r._key, r._value);
    }
    friend std::ostream& operator<<(std::ostream&, const internal_secret&);

    key_t _key;
    value_t _value;
};

struct fetch_internal_secret_request
  : serde::envelope<fetch_internal_secret_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;

    fetch_internal_secret_request(
      model::timeout_clock::duration timeout, internal_secret::key_t k)
      : timeout{timeout}
      , key{std::move(k)} {}

    friend bool operator==(
      const fetch_internal_secret_request&,
      const fetch_internal_secret_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const fetch_internal_secret_request& rep);

    auto serde_fields() { return std::tie(timeout, key); }

    static fetch_internal_secret_request
    serde_direct_read(iobuf_parser& in, size_t bytes_left_limit);

    model::timeout_clock::duration timeout;
    internal_secret::key_t key;
};

struct fetch_internal_secret_reply
  : serde::envelope<fetch_internal_secret_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;

    fetch_internal_secret_reply(internal_secret s, errc ec)
      : secret(std::move(s))
      , ec(ec) {}

    friend bool operator==(
      const fetch_internal_secret_reply&, const fetch_internal_secret_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const fetch_internal_secret_reply& rep);

    static fetch_internal_secret_reply
    serde_direct_read(iobuf_parser& in, size_t bytes_left_limit);
    void serde_write(iobuf& out);

    internal_secret secret;
    errc ec;
};

} // namespace cluster
