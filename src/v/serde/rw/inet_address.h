// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/vlog.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "ssx/sformat.h"

#include <seastar/net/inet_address.hh>

namespace serde {

inline void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  ss::net::inet_address& t,
  const std::size_t bytes_left_limit) {
    bool is_ipv4 = read_nested<bool>(in, bytes_left_limit);
    auto address_buf = read_nested<iobuf>(in, bytes_left_limit);
    auto address_bytes = iobuf_to_bytes(address_buf);
    if (is_ipv4) {
        ::in_addr addr{};
        if (unlikely(address_bytes.size() != sizeof(addr))) {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "reading type ss::net::inet_address of size {}: {} bytes left - "
              "unexpected ipv4 "
              "address size, read: {}, expected: {}",
              sizeof(ss::net::inet_address),
              in.bytes_left(),
              address_bytes.size(),
              sizeof(addr)));
        }

        std::memcpy(&addr, address_bytes.data(), sizeof(addr));
        t = ss::net::inet_address(addr);
    } else {
        ::in6_addr addr{};
        if (unlikely(address_bytes.size() != sizeof(addr))) {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "reading type ss::net::inet_address of size {}: {} bytes left - "
              "unexpected ipv6 "
              "address size, read: {}, expected: {}",
              sizeof(ss::net::inet_address),
              in.bytes_left(),
              address_bytes.size(),
              sizeof(addr)));
        }
        std::memcpy(&addr, address_bytes.data(), sizeof(addr));
        t = ss::net::inet_address(addr);
    }
}

inline void tag_invoke(tag_t<write_tag>, iobuf& out, ss::net::inet_address t) {
    iobuf address_bytes;

    // NOLINTNEXTLINE
    address_bytes.append((const char*)t.data(), t.size());

    write(out, t.is_ipv4());
    write(out, std::move(address_bytes));
}

} // namespace serde
