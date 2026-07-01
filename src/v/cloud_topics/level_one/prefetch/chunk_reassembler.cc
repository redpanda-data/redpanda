/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/chunk_reassembler.h"

#include "bytes/iobuf_parser.h"
#include "cloud_topics/level_one/common/object.h"
#include "model/record.h"

namespace cloud_topics::prefetch {

namespace {

// Size in bytes of the data_type delimiter that precedes every batch in an
// L1 object (one uint8_t).
constexpr size_t data_type_size = sizeof(uint8_t);

// Minimum bytes needed to determine the full size of one L1 batch entry:
// the data_type delimiter plus the L1 batch header (which contains
// size_bytes).
size_t min_entry_prefix() { return data_type_size + l1::l1_batch_header_size; }

} // namespace

chunked_vector<model::record_batch> chunk_reassembler::feed(iobuf chunk) {
    _carry.append(std::move(chunk));
    chunked_vector<model::record_batch> out;

    while (_carry.size_bytes() >= min_entry_prefix()) {
        // Peek at the header without consuming from _carry.
        // Skip the 1-byte data_type delimiter then parse the L1 header.
        iobuf_parser hp(_carry.share(data_type_size, l1::l1_batch_header_size));
        auto hdr = l1::parse_batch_header(hp);

        // hdr.size_bytes is the Redpanda on-disk batch size: it covers the
        // 61-byte packed header plus the body, but not the 8-byte term field
        // or the data_type byte. Total bytes consumed from _carry per entry:
        //   1 (data_type) + 69 (L1 header) + (size_bytes - 61) (body)
        //   = 9 + size_bytes
        const size_t body_size = static_cast<size_t>(hdr.size_bytes)
                                 - model::packed_record_batch_header_size;
        const size_t entry_size = data_type_size + l1::l1_batch_header_size
                                  + body_size;

        if (_carry.size_bytes() < entry_size) {
            break; // incomplete batch — keep as slack
        }

        // Consume and discard the data_type byte + L1 header, then take body.
        _carry.trim_front(data_type_size + l1::l1_batch_header_size);
        iobuf body;
        body.append(_carry.share(0, body_size));
        _carry.trim_front(body_size);

        out.push_back(
          model::record_batch(
            hdr, std::move(body), model::record_batch::tag_ctor_ng{}));
    }

    return out;
}

} // namespace cloud_topics::prefetch
