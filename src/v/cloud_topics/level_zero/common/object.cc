/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/common/object.h"

#include "bytes/iobuf_parser.h"
#include "serde/rw/rw.h"

#include <fmt/format.h>

#include <stdexcept>

namespace cloud_topics::l0 {

footer footer::copy() const {
    footer result;
    for (const auto& [ntp, info] : partitions) {
        result.partitions.emplace_hint(result.partitions.end(), ntp, info);
    }
    return result;
}

std::variant<footer, size_t> footer::read(iobuf buf) {
    if (buf.size_bytes() < sizeof(uint32_t)) {
        throw std::runtime_error(fmt::format(
          "expected at least {} bytes in footer, got: {}",
          sizeof(uint32_t),
          buf.size_bytes()));
    }

    // Read the footer size from the last 4 bytes
    auto footer_size
      = iobuf_parser(buf.tail(sizeof(uint32_t))).consume_type<uint32_t>();

    if (buf.size_bytes() >= (footer_size + sizeof(uint32_t))) {
        // We have enough data to read the complete footer
        iobuf footer_data = buf.share(
          buf.size_bytes() - sizeof(uint32_t) - footer_size, footer_size);
        return serde::from_iobuf<footer>(std::move(footer_data));
    }

    // Return how many more bytes are needed
    return (footer_size + sizeof(uint32_t)) - buf.size_bytes();
}

} // namespace cloud_topics::l0
