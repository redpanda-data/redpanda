#include "storage/segment_offset_index_utils.h"

#include "hashing/xx.h"

#include <fmt/format.h>

namespace storage {
using idx_header_t = segment_offset_index::header;
static constexpr size_t idx_header_sz = sizeof(idx_header_t);
static constexpr size_t index_element_size = sizeof(uint32_t) * 2;

std::vector<std::pair<uint32_t, uint32_t>>
offset_index_from_buf(ss::temporary_buffer<char> buf) {
    if (buf.size() < idx_header_sz) {
        throw std::runtime_error(fmt::format(
          "Cannot serialize offset index, size:{}, need at least:{} bytes",
          buf.size(),
          idx_header_sz));
    }
    idx_header_t header;
    std::copy_n(buf.get(), idx_header_sz, reinterpret_cast<char*>(&header));
    if (header.size != buf.size() - idx_header_sz) {
        throw std::runtime_error(fmt::format(
          "Cannot serialize offset index, size:{}, header size missmatch. "
          "Header size required: {}, found:(buffer.size() - index_size):{}",
          buf.size(),
          header.size,
          buf.size() - idx_header_sz));
    }
    if (header.size == 0) {
        return {};
    }
    const uint32_t csum = xxhash_32(buf.get() + idx_header_sz, header.size);
    if (csum != header.checksum) {
        throw std::runtime_error(fmt::format(
          "checksum missmatch for offset index. got:{}, expected:{}",
          csum,
          header.checksum));
    }
    std::vector<std::pair<uint32_t, uint32_t>> ret(
      header.size / (sizeof(uint32_t) * 2));
    std::copy_n(
      buf.get() + idx_header_sz,
      header.size,
      reinterpret_cast<char*>(ret.data()));
    return ret;
}
ss::temporary_buffer<char>
offset_index_to_buf(const std::vector<std::pair<uint32_t, uint32_t>>& v) {
    idx_header_t header;
    header.size = v.size() * (sizeof(uint32_t) * 2);
    header.checksum = xxhash_32(
      reinterpret_cast<const char*>(v.data()), header.size);
    ss::temporary_buffer<char> ret(header.size + idx_header_sz);
    std::copy_n(
      reinterpret_cast<const char*>(&header), idx_header_sz, ret.get_write());
    std::copy_n(
      reinterpret_cast<const char*>(v.data()),
      header.size,
      ret.get_write() + idx_header_sz);
    return ret;
}

} // namespace storage
