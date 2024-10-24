// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "storage/mvlog/entry_stream.h"

#include "base/outcome.h"
#include "base/seastarx.h"
#include "bytes/iostream.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/entry_stream_utils.h"
#include "storage/mvlog/errc.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <memory>

namespace storage::experimental::mvlog {

ss::future<result<std::optional<entry>, errc>> entry_stream::next() {
    auto header_buf = co_await read_iobuf_exactly(
      stream_, packed_entry_header_size);
    if (header_buf.size_bytes() != packed_entry_header_size) {
        if (header_buf.size_bytes() == 0) {
            co_return std::nullopt;
        }
        co_return errc::short_read;
    }

    auto entry_hdr = entry_header_from_iobuf(std::move(header_buf));
    auto computed_crc = entry_header_crc(entry_hdr.body_size, entry_hdr.type);
    if (entry_hdr.header_crc != computed_crc) {
        co_return errc::checksum_mismatch;
    }

    auto body_buf = co_await read_iobuf_exactly(stream_, entry_hdr.body_size);
    if (body_buf.size_bytes() != static_cast<size_t>(entry_hdr.body_size)) {
        co_return errc::short_read;
    }
    co_return entry{entry_hdr, std::move(body_buf)};
}

} // namespace storage::experimental::mvlog
