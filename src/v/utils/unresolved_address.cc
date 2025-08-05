/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "utils/unresolved_address.h"

#include "base/vlog.h"

#include <charconv>

namespace net {
unresolved_address
unresolved_address::parse_address(std::string_view maybe_address) {
    auto separator = maybe_address.find(':');
    if (
      separator == std::string_view::npos || separator == 0
      || separator == maybe_address.size() - 1) {
        throw std::invalid_argument{fmt_with_ctx(
          fmt::format,
          "address override environment variable expected format: 'host:port', "
          "found: {}",
          maybe_address)};
    }

    auto host_view = maybe_address.substr(0, separator);
    auto port_view = maybe_address.substr(separator + 1);

    try {
        uint16_t port = 0;
        auto result = std::from_chars(
          port_view.data(), port_view.data() + port_view.size(), port);

        if (result.ec != std::errc{}) {
            throw std::invalid_argument{fmt_with_ctx(
              fmt::format,
              "failed to convert {} to port (uint16_t)",
              port_view)};
        }

        if (result.ptr != port_view.data() + port_view.size()) {
            throw std::invalid_argument{fmt_with_ctx(
              fmt::format,
              "failed to convert {} to port (uint16_t)",
              port_view)};
        }

        return net::unresolved_address{
          {host_view.data(), host_view.size()}, port};
    } catch (const std::exception& ex) {
        throw std::invalid_argument{fmt_with_ctx(
          fmt::format,
          "failed to convert port {} to port (uint16_t): {}",
          port_view,
          ex.what())};
    }
}
} // namespace net
