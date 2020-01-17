#include "storage/segment_offset_index.h"

#include "hashing/xx.h"
#include "storage/fs_utils.h"
#include "vassert.h"

#include <seastar/core/fstream.hh>

#include <bits/stdint-uintn.h>
#include <boost/container/container_fwd.hpp>
#include <fmt/format.h>

#include <algorithm>

namespace storage {

segment_offset_index::segment_offset_index(
  ss::sstring filename, ss::file f, model::offset base, size_t step)
  : _name(std::move(filename))
  , _out(std::move(f))
  , _base(base)
  , _step(step) {}

void segment_offset_index::maybe_track(
  model::offset o, size_t pos, size_t data_size) {
    vassert(o >= _base, "cannot track offsets that are lower than our base");
    if (!_positions.empty()) {
        // check if this is an earlier offset; ignore if so
        const uint32_t i = o() - _base();
        if (_positions.back().first > i) {
            return;
        }
    }
    _acc += data_size;
    if (_acc >= _step) {
        _acc = 0;
        // We know that a segment cannot be > 4GB
        _positions.emplace_back(
          static_cast<uint32_t>(o() - _base()), // NOLINT
          static_cast<uint32_t>(pos));          // NOLINT
        _needs_persistence = true;
    }
}
struct base_comparator {
    bool
    operator()(const std::pair<uint32_t, uint32_t>& p, uint32_t needle) const {
        return p.first < needle;
    }
};
std::optional<size_t> segment_offset_index::lower_bound(model::offset o) {
    if (o < _base) {
        return std::nullopt;
    }
    const uint32_t i = o() - _base();
    if (auto it = std::lower_bound(
          _positions.begin(), _positions.end(), i, base_comparator{});
        it != _positions.end()) {
        return it->second;
    }
    return std::nullopt;
}

static std::vector<std::pair<uint32_t, uint32_t>>
serialize(ss::temporary_buffer<char> buf) {
    using idx_t = segment_offset_index::header;
    static constexpr size_t idx_sz = sizeof(idx_t);
    if (buf.size() < idx_sz) {
        throw std::runtime_error(fmt::format(
          "Cannot serialize offset index, size:{}, need at least:{} bytes",
          buf.size(),
          idx_sz));
    }
    idx_t header;
    std::copy_n(buf.get(), idx_sz, reinterpret_cast<char*>(&header));
    if (header.size != buf.size() - idx_sz) {
        throw std::runtime_error(fmt::format(
          "Cannot serialize offset index, size:{}, header size missmatch. "
          "Header size required: {}, found:(buffer.size() - index_size):{}",
          buf.size(),
          header.size,
          buf.size() - idx_sz));
    }
    if (header.size == 0) {
        return {};
    }
    const uint32_t csum = xxhash_32(buf.get() + idx_sz, header.size);
    if (csum != header.checksum) {
        throw std::runtime_error(fmt::format(
          "checksum missmatch for offset index. got:{}, expected:{}",
          csum,
          header.checksum));
    }
    std::vector<std::pair<uint32_t, uint32_t>> ret(
      header.size / (sizeof(uint32_t) * 2));
    std::copy_n(
      buf.get() + idx_sz, header.size, reinterpret_cast<char*>(ret.data()));
    return ret;
}
static ss::temporary_buffer<char>
deserialize(const std::vector<std::pair<uint32_t, uint32_t>>& v) {
    using idx_t = segment_offset_index::header;
    static constexpr size_t idx_sz = sizeof(idx_t);
    idx_t header;
    header.size = v.size() * (sizeof(uint32_t) * 2);
    header.checksum = xxhash_32(
      reinterpret_cast<const char*>(v.data()), header.size);
    ss::temporary_buffer<char> ret(header.size + idx_sz);
    std::copy_n(
      reinterpret_cast<const char*>(&header), idx_sz, ret.get_write());
    std::copy_n(
      reinterpret_cast<const char*>(v.data()),
      header.size,
      ret.get_write() + idx_sz);
    return ret;
}

ss::future<> segment_offset_index::materialize_index() {
    return _out.size()
      .then([this](uint64_t size) mutable {
          return _out.dma_read_bulk<char>(0, size);
      })
      .then([this](ss::temporary_buffer<char> buf) {
          _positions = serialize(std::move(buf));
      });
}

ss::future<> segment_offset_index::close() {
    if (!_needs_persistence) {
        return _out.close();
    }
    _positions.shrink_to_fit();
    auto b = deserialize(_positions);
    auto out = ss::make_lw_shared<ss::output_stream<char>>(
      ss::make_file_output_stream(std::move(_out)));
    return out->write(b.get(), b.size())
      .then([out] { return out->flush(); })
      .then([out] { return out->close(); })
      .finally([out] {});
}

} // namespace storage
