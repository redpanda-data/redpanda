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

#include "cluster/internal_secret.h"

#include "serde/serde.h"

#include <bytes/iobuf_parser.h>
#include <fmt/ostream.h>

namespace cluster {

std::ostream& operator<<(std::ostream& os, const internal_secret&) {
    // NOTE: this stream is intentially left minimal to err away from exposing
    // anything that may be useful for an attacker to use.
    return os << "{internal_secret}";
}

std::ostream&
operator<<(std::ostream& o, const fetch_internal_secret_request& rep) {
    fmt::print(o, "secret: {}", rep.key());
    return o;
}

fetch_internal_secret_request fetch_internal_secret_request::serde_direct_read(
  iobuf_parser& in, size_t bytes_left_limit) {
    using serde::read_nested;
    auto timeout = read_nested<model::timeout_clock::duration>(
      in, bytes_left_limit);
    auto secret = read_nested<internal_secret::key_t>(in, bytes_left_limit);
    return {timeout, std::move(secret)};
}

std::ostream&
operator<<(std::ostream& o, const fetch_internal_secret_reply& rep) {
    fmt::print(o, "secret: {}, ec: {}", rep.secret.key(), rep.ec);
    return o;
}

fetch_internal_secret_reply fetch_internal_secret_reply::serde_direct_read(
  iobuf_parser& in, size_t bytes_left_limit) {
    using serde::read_nested;
    auto k = read_nested<internal_secret::key_t>(in, bytes_left_limit);
    auto v = read_nested<internal_secret::value_t>(in, bytes_left_limit);
    auto ec = read_nested<errc>(in, bytes_left_limit);
    return {{std::move(k), std::move(v)}, ec};
}

void fetch_internal_secret_reply::serde_write(iobuf& out) {
    using serde::write;
    write(out, secret.key());
    write(out, secret.value());
    write(out, ec);
}

} // namespace cluster
