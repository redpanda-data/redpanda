#include "storage/index_state.h"

#include "hashing/xx.h"
#include "likely.h"
#include "utils/vint.h"

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
      uint32_t(r.index.size()));
    for (auto& e : r.index) {
        xx.update_all(e.relative_offset, e.relative_time, e.filepos);
    }
    return xx.digest();
}

std::ostream& operator<<(std::ostream& o, const index_state& s) {
    return o << "{ size:" << s.size << ", checksum:" << s.checksum
             << ", bitflags" << s.bitflags << ", base_offset:" << s.base_offset
             << ", base_timestamp" << s.base_timestamp
             << ", max_timestamp:" << s.max_timestamp
             << ", index-size:" << s.index.size() << "}";
}

} // namespace storage

namespace reflection {
using entry = typename storage::index_state::entry;
void adl<entry>::to(iobuf& out, entry&& r) {
    reflection::serialize_cpu_to_le(
      out, r.relative_offset, r.relative_time, r.filepos);
}
entry adl<entry>::from(iobuf_parser& in) {
    auto o = ss::le_to_cpu(reflection::adl<uint32_t>{}.from(in));
    auto t = ss::le_to_cpu(reflection::adl<uint32_t>{}.from(in));
    auto f = ss::le_to_cpu(reflection::adl<uint32_t>{}.from(in));
    return entry(o, t, f);
}

void adl<storage::index_state>::to(iobuf& out, storage::index_state&& r) {
    const uint32_t final_size = sizeof(storage::index_state::size)
                                + sizeof(storage::index_state::checksum)
                                + sizeof(storage::index_state::bitflags)
                                + sizeof(storage::index_state::base_offset)
                                + sizeof(storage::index_state::base_timestamp)
                                + sizeof(storage::index_state::max_timestamp)
                                + (uint32_t) // index size
                                + (r.index.size() * (sizeof(uint32_t) * 3));
    r.size = final_size;
    r.checksum = storage::index_state::checksum_state(r);
    reflection::serialize_cpu_to_le(
      out,
      r.size,
      r.checksum,
      r.bitflags,
      r.base_offset(),
      r.base_timestamp(),
      r.max_timestamp(),
      uint32_t(r.index.size()));
    for (auto& e : r.index) {
        reflection::adl<entry>{}.to(out, std::move(e));
    }
}

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

    const auto index_size = ss::le_to_cpu(reflection::adl<uint32_t>{}.from(in));
    ret.index.reserve(index_size);
    for (uint32_t i = 0; i < index_size; ++i) {
        ret.index.emplace_back(reflection::adl<entry>{}.from(in));
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
