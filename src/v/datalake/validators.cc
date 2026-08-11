/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/validators.h"

#include "absl/strings/numbers.h"

#include <fmt/format.h>

#include <ada.h>
#include <limits>

namespace datalake {

tl::expected<ada::url_aggregator, ss::sstring>
parse_iceberg_rest_catalog_endpoint(const ss::sstring& url_str) {
    auto url = ada::parse(url_str);
    if (!url.has_value()) {
        return tl::unexpected(
          fmt::format(
            "Malformed Iceberg REST catalog endpoint url: {}", url_str));
    }
    if (url->has_port()) {
        int32_t port_from_uri{0};
        auto parsed = absl::SimpleAtoi(url->get_port(), &port_from_uri);
        if (
          !parsed || port_from_uri < 0
          || port_from_uri > std::numeric_limits<uint16_t>::max()) {
            return tl::unexpected(
              fmt::format(
                "Malformed Iceberg REST catalog endpoint url: {}, unable to "
                "parse port",
                url_str));
        }
    }
    return std::move(url.value());
}

} // namespace datalake
