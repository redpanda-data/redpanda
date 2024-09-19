// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/index_state.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"
#include "hashing/xx.h"
#include "reflection/adl.h"
#include "serde/peek.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"
#include "serde/rw/scalar.h"
#include "serde/rw/vector.h"
#include "serde/serde_exception.h"
#include "utils/to_string.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

namespace storage {

offset_time_index::offset_time_index(
  model::timestamp ts, offset_delta_time with_offset)
  : _with_offset(with_offset) {
    if (_with_offset == offset_delta_time::yes) {
        _val = static_cast<uint32_t>(
          std::clamp(ts(), delta_time_min, delta_time_max) + offset);
    } else {
        _val = _val = static_cast<uint32_t>(std::clamp(
          ts(),
          model::timestamp::type{std::numeric_limits<uint32_t>::min()},
          model::timestamp::type{std::numeric_limits<uint32_t>::max()}));
    }
}

uint32_t offset_time_index::operator()() const {
    if (_with_offset == offset_delta_time::yes) {
        return _val - static_cast<uint32_t>(offset);
    } else {
        return _val;
    }
}

offset_time_index::offset_time_index(
  uint32_t val, offset_delta_time with_offset)
  : _with_offset(with_offset)
  , _val(val) {}

uint32_t offset_time_index::raw_value() const { return _val; }

index_state index_state::make_empty_index(offset_delta_time with_offset) {
    index_state idx{};
    idx.with_offset = with_offset;

    return idx;
}

bool index_state::maybe_index(
  size_t accumulator,
  size_t step,
  size_t starting_position_in_file,
  model::offset batch_base_offset,
  model::offset batch_max_offset,
  model::timestamp first_timestamp,
  model::timestamp last_timestamp,
  std::optional<model::timestamp> new_broker_timestamp,
  bool user_data,
  size_t compactible_records) {
    vassert(
      batch_base_offset >= base_offset,
      "cannot track offsets that are lower than our base, o:{}, "
      "_state.base_offset:{} - index: {}",
      batch_base_offset,
      base_offset,
      *this);

    bool retval = false;

    // The first non-config batch in the segment, use its timestamp
    // to override the timestamps of any config batch that was indexed
    // by virtue of being the first in the segment.
    if (user_data && non_data_timestamps) {
        vassert(relative_time_index.size() == 1, "");
        relative_time_index[0]
          = offset_time_index{last_timestamp, with_offset}.raw_value();

        base_timestamp = first_timestamp;
        max_timestamp = first_timestamp;
        non_data_timestamps = false;
    }

    // index_state
    if (empty()) {
        // Ordinarily, we do not allow configuration batches to contribute to
        // the segment's timestamp bounds (because config batches use walltime
        // but user data timestamps may be anything).  However, for the first
        // batch we set the timestamps, and then set a `non_data_timestamps`
        // flag so that the next time we see user data we will overwrite
        // the walltime timestamps with the user data timestamps.
        non_data_timestamps = !user_data;

        base_timestamp = first_timestamp;
        max_timestamp = first_timestamp;
        retval = true;
    }

    // NOTE: we don't need the 'max()' trick below because we controll the
    // offsets ourselves and it would be a bug otherwise - see assert above
    max_offset = batch_max_offset;

    // Do not allow config batches to contribute to segment timestamp bounds,
    // because their timestamps may differ wildly from user-provided timestamps
    if (user_data) {
        // some clients leave max timestamp uninitialized in cases there is a
        // single record in a batch in this case we use first timestamp as a
        // last one
        last_timestamp = std::max(first_timestamp, last_timestamp);
        max_timestamp = std::max(max_timestamp, last_timestamp);
        if (new_broker_timestamp.has_value()) {
            broker_timestamp = *new_broker_timestamp;
        }
    }
    if (compactible_records > 0) {
        num_compactible_records_appended
          = num_compactible_records_appended.value_or(0) + compactible_records;
    }
    // always saving the first batch simplifies a lot of book keeping
    if ((accumulator >= step && user_data) || retval) {
        add_entry(
          // We know that a segment cannot be > 4GB
          batch_base_offset() - base_offset(),
          offset_time_index{last_timestamp - base_timestamp, with_offset},
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
             << ", max_timestamp:" << s.max_timestamp
             << ", batch_timestamps_are_monotonic:"
             << s.batch_timestamps_are_monotonic
             << ", with_offset:" << s.with_offset
             << ", non_data_timestamps:" << s.non_data_timestamps
             << ", broker_timestamp:" << s.broker_timestamp
             << ", num_compactible_records_appended:"
             << s.num_compactible_records_appended
             << ", clean_compact_timestamp:" << s.clean_compact_timestamp
             << ", may_have_tombstone_records:" << s.may_have_tombstone_records
             << ", index(" << s.relative_offset_index.size() << ","
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
    write(tmp, batch_timestamps_are_monotonic);
    write(tmp, with_offset);
    write(tmp, non_data_timestamps);
    write(tmp, broker_timestamp);
    write(tmp, num_compactible_records_appended);
    write(tmp, clean_compact_timestamp);
    write(tmp, may_have_tombstone_records);

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
        st.batch_timestamps_are_monotonic = false;
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

    if (hdr._version < index_state::monotonic_timestamps_version) {
        st.batch_timestamps_are_monotonic = false;
        return;
    }

    if (hdr._version >= index_state::monotonic_timestamps_version) {
        read_nested(p, st.batch_timestamps_are_monotonic, 0U);
        read_nested(p, st.with_offset, 0U);
        // if we are deserializing we are likely dealing with a closed segment,
        // this means that the value of this flag is unused in this object,
        // since no new data will be appended. but it's still necessary to read
        // it.
        read_nested(p, st.non_data_timestamps, 0U);
    }

    if (hdr._version >= index_state::broker_timestamp_version) {
        read_nested(p, st.broker_timestamp, 0U);
    }
    if (hdr._version >= index_state::num_compactible_records_version) {
        read_nested(p, st.num_compactible_records_appended, 0U);
    } else {
        st.num_compactible_records_appended = std::nullopt;
    }
    if (hdr._version >= index_state::clean_compact_timestamp_version) {
        read_nested(p, st.clean_compact_timestamp, 0U);
    } else {
        st.clean_compact_timestamp = std::nullopt;
    }
    if (hdr._version >= index_state::may_have_tombstone_records_version) {
        read_nested(p, st.may_have_tombstone_records, 0U);
    } else {
        st.may_have_tombstone_records = true;
    }
}

index_state index_state::copy() const { return *this; }

size_t index_state::size() const { return relative_offset_index.size(); }

bool index_state::empty() const { return relative_offset_index.empty(); }

void index_state::add_entry(
  uint32_t relative_offset, offset_time_index relative_time, uint64_t pos) {
    relative_offset_index.push_back(relative_offset);
    relative_time_index.push_back(relative_time.raw_value());
    position_index.push_back(pos);
}
void index_state::pop_back() {
    relative_offset_index.pop_back();
    relative_time_index.pop_back();
    position_index.pop_back();
    if (empty()) {
        non_data_timestamps = false;
    }
}
std::tuple<uint32_t, offset_time_index, uint64_t>
index_state::get_entry(size_t i) const {
    return {
      relative_offset_index[i],
      offset_time_index{relative_time_index[i], with_offset},
      position_index[i]};
}

void index_state::shrink_to_fit() {
    relative_offset_index.shrink_to_fit();
    relative_time_index.shrink_to_fit();
    position_index.shrink_to_fit();
}

std::optional<std::tuple<uint32_t, offset_time_index, uint64_t>>
index_state::find_entry(model::timestamp ts) {
    const auto idx = offset_time_index{ts, with_offset};

    auto it = std::lower_bound(
      std::begin(relative_time_index),
      std::end(relative_time_index),
      idx.raw_value(),
      std::less<uint32_t>{});
    if (it == relative_offset_index.end()) {
        return std::nullopt;
    }

    const auto dist = std::distance(relative_offset_index.begin(), it);

    // lower_bound will place us on the first batch in the index that has
    // 'max_timestamp' greater than 'ts'. Since not every batch is indexed,
    // it's not guaranteed* that 'ts' will be present in the batch
    // (i.e. 'ts > first_timestamp'). For this reason, we go back one batch.
    //
    // *In the case where lower_bound places on the first batch, we'll
    // start the timequery from the beggining of the segment as the user
    // data batch is always indexed.
    return get_entry(dist > 0 ? dist - 1 : 0);
}

void index_state::update_batch_timestamps_are_monotonic(bool pred) {
    batch_timestamps_are_monotonic = batch_timestamps_are_monotonic && pred;
}

index_state::index_state(const index_state& o) noexcept
  : bitflags(o.bitflags)
  , base_offset(o.base_offset)
  , max_offset(o.max_offset)
  , base_timestamp(o.base_timestamp)
  , max_timestamp(o.max_timestamp)
  , relative_offset_index(o.relative_offset_index.copy())
  , relative_time_index(o.relative_time_index.copy())
  , position_index(o.position_index.copy())
  , batch_timestamps_are_monotonic(o.batch_timestamps_are_monotonic)
  , with_offset(o.with_offset)
  , non_data_timestamps(o.non_data_timestamps)
  , broker_timestamp(o.broker_timestamp)
  , num_compactible_records_appended(o.num_compactible_records_appended)
  , clean_compact_timestamp(o.clean_compact_timestamp)
  , may_have_tombstone_records(o.may_have_tombstone_records) {}

namespace serde_compat {
uint64_t index_state_serde::checksum(const index_state& r) {
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

index_state index_state_serde::decode(iobuf_parser& parser) {
    index_state retval;

    const auto size = reflection::adl<uint32_t>{}.from(parser);
    if (unlikely(parser.bytes_left() != size)) {
        throw serde::serde_exception(fmt_with_ctx(
          fmt::format,
          "Index size does not match header size. Got:{}, expected:{}",
          parser.bytes_left(),
          size));
    }

    const auto expected_checksum = reflection::adl<uint64_t>{}.from(parser);
    retval.bitflags = reflection::adl<uint32_t>{}.from(parser);
    retval.base_offset = model::offset(
      reflection::adl<model::offset::type>{}.from(parser));
    retval.max_offset = model::offset(
      reflection::adl<model::offset::type>{}.from(parser));
    retval.base_timestamp = model::timestamp(
      reflection::adl<model::timestamp::type>{}.from(parser));
    retval.max_timestamp = model::timestamp(
      reflection::adl<model::timestamp::type>{}.from(parser));

    const uint32_t vsize = ss::le_to_cpu(
      reflection::adl<uint32_t>{}.from(parser));

    for (auto i = 0U; i < vsize; ++i) {
        retval.relative_offset_index.push_back(
          reflection::adl<uint32_t>{}.from(parser));
    }

    for (auto i = 0U; i < vsize; ++i) {
        retval.relative_time_index.push_back(
          reflection::adl<uint32_t>{}.from(parser));
    }

    for (auto i = 0U; i < vsize; ++i) {
        retval.position_index.push_back(
          reflection::adl<uint64_t>{}.from(parser));
    }

    retval.relative_offset_index.shrink_to_fit();
    retval.relative_time_index.shrink_to_fit();
    retval.position_index.shrink_to_fit();

    const auto computed_checksum = checksum(retval);
    if (unlikely(expected_checksum != computed_checksum)) {
        throw serde::serde_exception(fmt_with_ctx(
          fmt::format,
          "Invalid checksum for index. Got:{}, expected:{}",
          computed_checksum,
          expected_checksum));
    }

    return retval;
}

iobuf index_state_serde::encode(const index_state& st) {
    iobuf out;
    vassert(
      st.relative_offset_index.size() == st.relative_time_index.size()
        && st.relative_offset_index.size() == st.position_index.size(),
      "ALL indexes must match in size. {}",
      st);
    const uint32_t final_size
      = sizeof(uint64_t) // checksum
        + sizeof(storage::index_state::bitflags)
        + sizeof(storage::index_state::base_offset)
        + sizeof(storage::index_state::max_offset)
        + sizeof(storage::index_state::base_timestamp)
        + sizeof(storage::index_state::max_timestamp)
        + sizeof(uint32_t) // index size
        + (st.relative_offset_index.size() * (sizeof(uint32_t) * 2 + sizeof(uint64_t)));
    const uint64_t computed_checksum = checksum(st);
    reflection::serialize(
      out,
      ondisk_version,
      final_size,
      computed_checksum,
      st.bitflags,
      st.base_offset(),
      st.max_offset(),
      st.base_timestamp(),
      st.max_timestamp(),
      uint32_t(st.relative_offset_index.size()));
    const uint32_t vsize = st.relative_offset_index.size();
    for (auto i = 0U; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, st.relative_offset_index[i]);
    }
    for (auto i = 0U; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, st.relative_time_index[i]);
    }
    for (auto i = 0U; i < vsize; ++i) {
        reflection::adl<uint64_t>{}.to(out, st.position_index[i]);
    }
    // add back the version and size field
    const auto expected_size = final_size + sizeof(int8_t) + sizeof(uint32_t);
    vassert(
      out.size_bytes() == expected_size,
      "Unexpected serialization size {} != expected {}",
      out.size_bytes(),
      expected_size);
    return out;
}
} // namespace serde_compat

} // namespace storage
