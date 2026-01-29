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

#include "lsm/block/handle.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"

namespace lsm::block {

iobuf handle::as_iobuf() const {
    iobuf buf;
    buf.append(std::bit_cast<std::array<uint8_t, sizeof(offset)>>(offset));
    buf.append(std::bit_cast<std::array<uint8_t, sizeof(size)>>(size));
    return buf;
}

handle handle::from_iobuf(iobuf buf) {
    dassert(
      buf.size_bytes() == sizeof(handle),
      "incorrect handle size, expected {} got {}",
      sizeof(handle),
      buf.size_bytes());
    iobuf_parser parser(std::move(buf));
    auto o = parser.consume_type<decltype(handle::offset)>();
    auto s = parser.consume_type<decltype(handle::size)>();
    return {.offset = o, .size = s};
}

fmt::iterator handle::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{offset:{},size:{}}}", offset, size);
}

} // namespace lsm::block
