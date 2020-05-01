#include "storage/index_state.h"

#include "bytes/iobuf_parser.h"
#include "hashing/xx.h"
#include "likely.h"
#include "reflection/adl.h"
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

std::ostream& operator<<(std::ostream& o, const index_state& s) {
    return o << "{version:" << (int)s.version << ", header_size:" << s.size
             << ", header_checksum:" << s.checksum
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
    index_state retval;
    retval.version = reflection::adl<int8_t>{}.from(parser);
    if (retval.version != 1) {
        // we screwed up version 0; and we only have version 1, so
        // we force the users to rebuild the all indices here
        return std::nullopt;
    }
    retval.size = reflection::adl<uint32_t>{}.from(parser);
    retval.checksum = reflection::adl<uint64_t>{}.from(parser);
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
    retval.relative_offset_index.reserve(vsize);
    retval.relative_time_index.reserve(vsize);
    retval.position_index.reserve(vsize);
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
          reflection::adl<uint32_t>{}.from(parser));
    }
    const auto computed_checksum = storage::index_state::checksum_state(retval);
    if (unlikely(retval.checksum != computed_checksum)) {
        vlog(
          stlog.debug,
          "Invalid checksum for index. Got:{}, expected:{}",
          computed_checksum,
          retval.checksum);
        return std::nullopt;
    }
    return retval;
}

iobuf index_state::checksum_and_serialize() {
    iobuf out;
    vassert(
      relative_offset_index.size() == relative_time_index.size()
        && relative_offset_index.size() == position_index.size(),
      "ALL indexes must match in size. {}",
      *this);
    const uint32_t final_size
      = sizeof(storage::index_state::checksum)
        + sizeof(storage::index_state::bitflags)
        + sizeof(storage::index_state::base_offset)
        + sizeof(storage::index_state::max_offset)
        + sizeof(storage::index_state::base_timestamp)
        + sizeof(storage::index_state::max_timestamp) + (uint32_t) // index size
        + (relative_offset_index.size() * (sizeof(uint32_t) * 3));
    size = final_size;
    checksum = storage::index_state::checksum_state(*this);
    reflection::serialize(
      out,
      version,
      size,
      checksum,
      bitflags,
      base_offset(),
      max_offset(),
      base_timestamp(),
      max_timestamp(),
      uint32_t(relative_offset_index.size()));
    const uint32_t vsize = relative_offset_index.size();
    for (auto i = 0U; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, relative_offset_index[i]);
    }
    for (auto i = 0U; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, relative_time_index[i]);
    }
    for (auto i = 0U; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, position_index[i]);
    }
    return out;
}
} // namespace storage
