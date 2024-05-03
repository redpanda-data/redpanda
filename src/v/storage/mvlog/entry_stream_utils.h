// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "storage/mvlog/entry.h"

namespace storage::experimental::mvlog {

// Serializes the given header into an iobuf, e.g. to be written to disk.
//
// Note that this is a more basic serialization than serde (no version, compat
// version, etc.) because the intent is that the serialized bytes will be
// deserialized on the read path via a stream.
//
// Generally, streaming poses a challenge for serde readers, which expect an
// iobuf of known size; we can't know the size of an object without first
// streaming some bytes (but how many bytes?). It is thus much simpler to opt
// out of the flexibility of serde and read the exact expected fields.
iobuf entry_header_to_iobuf(const entry_header& header);

// Deserializes an `entry_header` from the given buffer. Callers must ensure
// that the buffer is of size `packed_entry_header_size`.
//
// No correctness validations (e.g. correct CRC, valid enums) are performed;
// these are left to the caller.
entry_header entry_header_from_iobuf(iobuf b);

// Size of the header when serialized with entry_header_to_iobuf.
// NOTE: this differs from sizeof(entry_header) in that it does not get aligned
// to a word.
static constexpr size_t packed_entry_header_size
  = sizeof(entry_header::header_crc)  // 4
    + sizeof(entry_header::body_size) // 4
    + sizeof(entry_header::type);     // 1

// Returns the CRC to be used with the given paramters.
uint32_t entry_header_crc(int32_t body_size, entry_type type);

} // namespace storage::experimental::mvlog
