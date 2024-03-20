// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "bytes/iobuf.h"
#include "model/fundamental.h"

#include <cstdint>

namespace storage::experimental::mvlog {

enum class entry_type : int8_t {
    record_batch = 0,
    max = record_batch,
};
std::ostream& operator<<(std::ostream&, entry_type);

struct entry_header {
    uint32_t header_crc;
    int32_t body_size;
    entry_type type;

    friend std::ostream& operator<<(std::ostream&, const entry_header&);
    bool operator==(const entry_header& other) const = default;
};

struct record_batch_entry_body
  : public serde::checksum_envelope<
      record_batch_entry_body,
      serde::version<0>,
      serde::compat_version<0>> {
    // Record batch header, serialized for on-disk format.
    iobuf record_batch_header;

    // Body of the record batch.
    iobuf records;

    // The term of the record batch.
    model::term_id term;
};

} // namespace storage::experimental::mvlog
