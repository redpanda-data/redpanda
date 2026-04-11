// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/sst/footer.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"
#include "lsm/core/exceptions.h"

#include <bit>

namespace lsm::sst {

namespace {
constexpr static const uint64_t table_magic_number = 0xdb92384792387433;
}

iobuf footer::as_iobuf() const {
    iobuf buf;
    buf.append(metaindex_handle.as_iobuf());
    buf.append(index_handle.as_iobuf());
    crc::crc32c crc;
    crc_extend_iobuf(crc, buf);
    buf.append(
      std::bit_cast<std::array<uint8_t, sizeof(crc.value())>>(crc.value()));
    buf.append(
      std::bit_cast<std::array<uint8_t, sizeof(table_magic_number)>>(
        table_magic_number));
    dassert(
      buf.size_bytes() == encoded_length,
      "expected to encode to encoded_length");
    return buf;
}

footer footer::from_iobuf(iobuf buf) {
    dassert(
      buf.size_bytes() == encoded_length,
      "unexpected encoded langth for footer got {}, expected {}",
      buf.size_bytes(),
      encoded_length);
    iobuf_parser parser(std::move(buf));
    crc::crc32c actual_crc;
    crc_extend_iobuf(
      actual_crc,
      parser.share_no_consume(
        sizeof(decltype(footer::metaindex_handle))
        + sizeof(decltype(footer::index_handle))));
    auto metaindex_handle = block::handle::from_iobuf(
      parser.share(sizeof(decltype(footer::metaindex_handle))));
    auto index_handle = block::handle::from_iobuf(
      parser.share(sizeof(decltype(footer::index_handle))));
    auto expected_crc = parser.consume_type<crc::crc32c::value_type>();
    if (expected_crc != actual_crc.value()) {
        throw corruption_exception(
          "unexpected crc, got: {}, want: {} for SST footer",
          actual_crc.value(),
          expected_crc);
    }
    auto magic
      = parser.consume_type<std::decay_t<decltype(table_magic_number)>>();
    if (magic != table_magic_number) {
        throw corruption_exception(
          "sstable corruption, bad magic number: 0x{:x}", magic);
    }
    dassert(
      parser.bytes_left() == 0,
      "expected bytes left, got: {}",
      parser.bytes_left());
    return {.metaindex_handle = metaindex_handle, .index_handle = index_handle};
}

fmt::iterator footer::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{metaindex_handle:{},index_handle:{}}}",
      metaindex_handle,
      index_handle);
}

} // namespace lsm::sst
