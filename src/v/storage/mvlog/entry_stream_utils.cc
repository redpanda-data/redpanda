// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/entry_stream_utils.h"

#include "hashing/crc32c.h"

namespace storage::experimental::mvlog {

iobuf entry_header_to_iobuf(const entry_header& hdr) {
    iobuf b;
    serde::write(b, hdr.header_crc);
    serde::write(b, hdr.body_size);

    // NOTE: serde serializes enums as int32_t (serde_enum_serialized_t).
    // Explicitly serialize as int8_t.
    serde::write(b, static_cast<int8_t>(hdr.type));
    return b;
}

entry_header entry_header_from_iobuf(iobuf b) {
    vassert(
      b.size_bytes() == packed_entry_header_size,
      "Expected buf of size {}: {}",
      packed_entry_header_size,
      b.size_bytes());
    iobuf_parser parser(std::move(b));
    auto header_crc = serde::read_nested<uint32_t>(parser, 0);
    auto body_size = serde::read_nested<int32_t>(parser, 0);
    auto type = entry_type{serde::read_nested<int8_t>(parser, 0)};
    return {header_crc, body_size, type};
}

uint32_t entry_header_crc(int32_t body_size, entry_type type) {
    auto c = crc::crc32c();
    c.extend(ss::cpu_to_le(body_size));
    c.extend(static_cast<std::underlying_type_t<entry_type>>(type));
    return c.value();
}

} // namespace storage::experimental::mvlog
