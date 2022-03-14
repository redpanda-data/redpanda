// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/index_state.h"

#include "bytes/iobuf_parser.h"
#include "bytes/utils.h"
#include "likely.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "serde/serde.h"
#include "serde/serde_exception.h"
#include "storage/index_state_serde_compat.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <optional>

namespace storage {

bool index_state::maybe_index(
  size_t accumulator,
  size_t step,
  size_t starting_position_in_file,
  model::offset batch_base_offset,
  model::offset batch_max_offset,
  model::timestamp first_timestamp,
  model::timestamp last_timestamp) {
    vassert(
      batch_base_offset >= base_offset,
      "cannot track offsets that are lower than our base, o:{}, "
      "_state.base_offset:{} - index: {}",
      batch_base_offset,
      base_offset,
      *this);

    bool retval = false;
    // index_state
    if (empty()) {
        base_timestamp = first_timestamp;
        max_timestamp = first_timestamp;
        retval = true;
    }
    // NOTE: we don't need the 'max()' trick below because we controll the
    // offsets ourselves and it would be a bug otherwise - see assert above
    max_offset = batch_max_offset;
    // some clients leave max timestamp uninitialized in cases there is a
    // single record in a batch in this case we use first timestamp as a
    // last one
    last_timestamp = std::max(first_timestamp, last_timestamp);
    max_timestamp = std::max(max_timestamp, last_timestamp);

#ifndef NDEBUG
    // If future-timestamped content creeps into our storage layer,
    // it can disrupt time-based compaction: log when this happens.
    // Related: https://github.com/redpanda-data/redpanda/issues/3924
    auto now = model::timestamp::now();
    if (max_timestamp > model::timestamp::now()) {
        vlog(
          stlog.warn,
          "Updating index with max timestamp {} in future (now={}, "
          "first_timestamp={}, base_timestamp={})",
          max_timestamp,
          now,
          first_timestamp,
          base_timestamp());
    }
#endif

    // always saving the first batch simplifies a lot of book keeping
    if (accumulator >= step || retval) {
        // We know that a segment cannot be > 4GB
        add_entry(
          batch_base_offset() - base_offset(),
          last_timestamp() - base_timestamp(),
          starting_position_in_file);

        retval = true;
    }
    return retval;
}

std::ostream& operator<<(std::ostream& o, const index_state& s) {
    return o << "{header_bitflags:" << s.bitflags
             << ", base_offset:" << s.base_offset
             << ", max_offset:" << s.max_offset
             << ", base_timestamp:" << s.base_timestamp
             << ", max_timestamp:" << s.max_timestamp << ", index("
             << s.relative_offset_index.size() << ","
             << s.relative_time_index.size() << "," << s.position_index.size()
             << ")}";
}

void index_state::serde_write(iobuf& out) const {
    using serde::write;

    iobuf tmp;
    write(tmp, bitflags);
    write(tmp, base_offset);
    write(tmp, max_offset);
    write(tmp, base_timestamp);
    write(tmp, max_timestamp);
    write(tmp, relative_offset_index.copy());
    write(tmp, relative_time_index.copy());
    write(tmp, position_index.copy());

    crc::crc32c crc;
    crc_extend_iobuf(crc, tmp);
    const uint32_t tmp_crc = crc.value();

    // data blob + crc
    write(out, std::move(tmp));
    write(out, tmp_crc);
}

void read_nested(
  iobuf_parser& in, index_state& st, const size_t bytes_left_limit) {
    /*
     * peek at the 1-byte version prefix. this will either correspond to a
     * version from the deprecated format, or a version in the range supported
     * by the serde format.
     */
    const auto compat_version = serde::peek_version(in);

    /*
     * supported old version to avoid rebuilding all indices.
     */
    if (compat_version == serde_compat::index_state_serde::ondisk_version) {
        in.skip(sizeof(int8_t));
        st = serde_compat::index_state_serde::decode(in);
        return;
    }

    /*
     * unsupported old version.
     */
    if (compat_version < serde_compat::index_state_serde::ondisk_version) {
        throw serde::serde_exception(
          fmt_with_ctx(fmt::format, "Unsupported version: {}", compat_version));
    }

    /*
     * support for new serde format.
     */
    const auto hdr = serde::read_header<index_state>(in, bytes_left_limit);

    using serde::read_nested;

    // data blog + crc
    iobuf tmp;
    uint32_t tmp_crc = 0;
    read_nested(in, tmp, hdr._bytes_left_limit);
    read_nested(in, tmp_crc, hdr._bytes_left_limit);

    crc::crc32c crc;
    crc_extend_iobuf(crc, tmp);
    const uint32_t expected_tmp_crc = crc.value();

    if (tmp_crc != expected_tmp_crc) {
        throw serde::serde_exception(fmt_with_ctx(
          fmt::format,
          "Mismatched checksum {} expected {}",
          tmp_crc,
          expected_tmp_crc));
    }

    // unwrap actual fields
    iobuf_parser p(std::move(tmp));
    read_nested(p, st.bitflags, 0U);
    read_nested(p, st.base_offset, 0U);
    read_nested(p, st.max_offset, 0U);
    read_nested(p, st.base_timestamp, 0U);
    read_nested(p, st.max_timestamp, 0U);
    read_nested(p, st.relative_offset_index, 0U);
    read_nested(p, st.relative_time_index, 0U);
    read_nested(p, st.position_index, 0U);
}

} // namespace storage
