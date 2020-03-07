#include "storage/index_state.h"

#include "hashing/xx.h"
#include "likely.h"
#include "vassert.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

namespace storage {

uint64_t index_state::checksum_state(const index_state& r) {
    auto xx = incremental_xxhash64{};
    xx.update_all(
      r.bitflags,
      r.base_offset(),
      r.base_timestamp(),
      r.max_timestamp(),
      uint32_t(r.relative_offset_index.size()));
    const uint32_t vsize = r.relative_offset_index.size();
    for (auto i = 0; i < vsize; ++i) {
        xx.update(r.relative_offset_index[i]);
    }
    for (auto i = 0; i < vsize; ++i) {
        xx.update(r.relative_time_index[i]);
    }
    for (auto i = 0; i < vsize; ++i) {
        xx.update(r.position_index[i]);
    }
    return xx.digest();
}

std::ostream& operator<<(std::ostream& o, const index_state& s) {
    return o << "{ size:" << s.size << ", checksum:" << s.checksum
             << ", bitflags" << s.bitflags << ", base_offset:" << s.base_offset
             << ", base_timestamp" << s.base_timestamp
             << ", max_timestamp:" << s.max_timestamp << ", index("
             << s.relative_offset_index.size() << ","
             << s.relative_time_index.size() << "," << s.position_index.size()
             << ")}";
}

} // namespace storage

namespace reflection {
void adl<storage::index_state>::to(iobuf& out, storage::index_state&& r) {
    vassert(
      r.relative_offset_index.size() == r.relative_time_index.size()
        && r.relative_offset_index.size() == r.position_index.size(),
      "ALL indexes must match in size. {}",
      r);
    const uint32_t final_size
      = sizeof(storage::index_state::size)
        + sizeof(storage::index_state::checksum)
        + sizeof(storage::index_state::bitflags)
        + sizeof(storage::index_state::base_offset)
        + sizeof(storage::index_state::base_timestamp)
        + sizeof(storage::index_state::max_timestamp) + (uint32_t) // index size
        + (r.relative_offset_index.size() * (sizeof(uint32_t) * 3));
    r.size = final_size;
    r.checksum = storage::index_state::checksum_state(r);
    reflection::serialize(
      out,
      r.size,
      r.checksum,
      r.bitflags,
      r.base_offset(),
      r.base_timestamp(),
      r.max_timestamp(),
      uint32_t(r.relative_offset_index.size()));
    const uint32_t vsize = r.relative_offset_index.size();
    for (auto i = 0; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, r.relative_offset_index[i]);
    }
    for (auto i = 0; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, r.relative_time_index[i]);
    }
    for (auto i = 0; i < vsize; ++i) {
        reflection::adl<uint32_t>{}.to(out, r.position_index[i]);
    }

} // namespace reflection

storage::index_state adl<storage::index_state>::from(iobuf_parser& in) {
    storage::index_state ret;
    ret.size = ss::le_to_cpu(reflection::adl<uint32_t>{}.from(in));
    ret.checksum = ss::le_to_cpu(reflection::adl<uint64_t>{}.from(in));
    ret.bitflags = ss::le_to_cpu(reflection::adl<uint32_t>{}.from(in));
    ret.base_offset = model::offset(
      ss::le_to_cpu(reflection::adl<model::offset::type>{}.from(in)));
    ret.base_timestamp = model::timestamp(
      ss::le_to_cpu(reflection::adl<model::timestamp::type>{}.from(in)));
    ret.max_timestamp = model::timestamp(
      ss::le_to_cpu(reflection::adl<model::timestamp::type>{}.from(in)));

    const uint32_t vsize = ss::le_to_cpu(reflection::adl<uint32_t>{}.from(in));
    ret.relative_offset_index.reserve(vsize);
    ret.relative_time_index.reserve(vsize);
    ret.position_index.reserve(vsize);
    for (auto i = 0; i < vsize; ++i) {
        ret.relative_offset_index.push_back(
          reflection::adl<uint32_t>{}.from(in));
    }
    for (auto i = 0; i < vsize; ++i) {
        ret.relative_time_index.push_back(reflection::adl<uint32_t>{}.from(in));
    }
    for (auto i = 0; i < vsize; ++i) {
        ret.position_index.push_back(reflection::adl<uint32_t>{}.from(in));
    }
    if (unlikely(ret.checksum != storage::index_state::checksum_state(ret))) {
        throw std::runtime_error(fmt::format(
          "cannot recover storage::index_state due "
          "to missmatching checksum: {}",
          ret));
    }
    return ret;
}

} // namespace reflection
