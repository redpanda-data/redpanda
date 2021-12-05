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
#include "hashing/xx.h"
#include "likely.h"
#include "reflection/adl.h"
#include "storage/index_state_serde_compat.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <optional>

namespace storage {

uint64_t index_state::checksum_state(const index_state& r) {
    auto xx = incremental_xxhash64{};
    xx.update_all(
      r.bitflags,
      r.base_offset(),
      r.max_offset(),
      r.base_timestamp(),
      r.max_timestamp(),
      uint32_t(r.relative_offset_index.size()));
    const uint32_t vsize = r.relative_offset_index.size();
    for (auto i = 0U; i < vsize; ++i) {
        xx.update(r.relative_offset_index[i]);
    }
    for (auto i = 0U; i < vsize; ++i) {
        xx.update(r.relative_time_index[i]);
    }
    for (auto i = 0U; i < vsize; ++i) {
        xx.update(r.position_index[i]);
    }
    return xx.digest();
}
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
    return o << "{header_size:" << s.size << ", header_checksum:" << s.checksum
             << ", header_bitflags:" << s.bitflags
             << ", base_offset:" << s.base_offset
             << ", max_offset:" << s.max_offset
             << ", base_timestamp:" << s.base_timestamp
             << ", max_timestamp:" << s.max_timestamp << ", index("
             << s.relative_offset_index.size() << ","
             << s.relative_time_index.size() << "," << s.position_index.size()
             << ")}";
}

std::optional<index_state> index_state::hydrate_from_buffer(iobuf b) {
    iobuf_parser parser(std::move(b));

    auto version = reflection::adl<int8_t>{}.from(parser);
    switch (version) {
    case serde_compat::index_state_serde::ondisk_version:
        break;

    default:
        /*
         * v3: changed the on-disk format to use 64-bit values for physical
         * offsets to avoid overflow for segments larger than 4gb. backwards
         * compat would require converting the overflowed values. instead, we
         * fully deprecate old versions and rebuild the offset indexes.
         *
         * v2: fully deprecated
         *
         * v1: fully deprecated
         *
         *     version 1 code stored an on disk size that was calculated as 4
         *     bytes too small, and the decoder did not check the size. instead
         *     of rebuilding indexes for version 1 we'll adjust the size because
         *     the checksums are still verified.
         *
         * v0: fully deprecated
         */
        vlog(
          stlog.debug,
          "Forcing index rebuild for unknown or unsupported version {}",
          version);
        return std::nullopt;
    }

    try {
        return serde_compat::index_state_serde::decode(parser);
    } catch (const serde::serde_exception& ex) {
        vlog(stlog.debug, "Decoding index state: {}", ex.what());
        return std::nullopt;
    }
}

iobuf index_state::checksum_and_serialize() {
    return serde_compat::index_state_serde::encode(*this);
}

} // namespace storage
