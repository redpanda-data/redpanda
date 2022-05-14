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

#include "net/tls.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>

#include <array>

namespace net {
ss::future<std::optional<ss::sstring>> find_ca_file() {
    // list of all possible ca-cert file locations on different linux distros
    static constexpr std::array<std::string_view, 6> ca_cert_locations = {{
      "/etc/ssl/certs/ca-certificates.crt",
      "/etc/pki/tls/certs/ca-bundle.crt",
      "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
      "/etc/ssl/cert.pem",
      "/etc/ssl/ca-bundle.pem",
      "/etc/pki/tls/cacert.pem",
    }};

    for (auto ca_loc : ca_cert_locations) {
        if (co_await ss::file_exists(ca_loc)) {
            co_return ca_loc;
        }
    }
    co_return std::nullopt;
}
} // namespace net
