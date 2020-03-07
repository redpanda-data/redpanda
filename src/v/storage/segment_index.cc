#include "storage/segment_index.h"

#include "model/timestamp.h"
#include "storage/logger.h"
#include "vassert.h"

#include <seastar/core/fstream.hh>

#include <bits/stdint-uintn.h>
#include <boost/container/container_fwd.hpp>
#include <fmt/format.h>

#include <algorithm>

namespace storage {

static inline segment_index::entry translate_index_entry(
  const index_state& s, std::tuple<uint32_t, uint32_t, uint32_t> entry) {
    auto [relative_offset, relative_time, filepos] = entry;
    return segment_index::entry{
      .offset = model::offset(relative_offset + s.base_offset()),
      .timestamp = model::timestamp(relative_time + s.base_timestamp()),
      .filepos = filepos,
    };
}

segment_index::segment_index(
  ss::sstring filename, ss::file f, model::offset base, size_t step)
  : _name(std::move(filename))
  , _out(std::move(f))
  , _step(step) {
    _state.base_offset = base;
}

void segment_index::maybe_track(
  const model::record_batch_header& hdr, size_t filepos) {
    vassert(
      hdr.base_offset >= _state.base_offset,
      "cannot track offsets that are lower than our base, o:{}, "
      "_state.base_offset:{}",
      hdr.base_offset,
      _state.base_offset);

    if (_state.empty()) {
        _state.base_timestamp = hdr.first_timestamp;
    }

    _needs_persistence = true;

    _acc += hdr.size_bytes;
    _state.max_timestamp = std::max(hdr.max_timestamp, _state.max_timestamp);

    if (_acc >= _step) {
        _acc = 0;
        // We know that a segment cannot be > 4GB
        _state.add_entry(
          hdr.base_offset() - _state.base_offset(),
          hdr.max_timestamp() - _state.base_timestamp(),
          filepos);
    }
}

std::optional<segment_index::entry>
segment_index::find_nearest(model::timestamp t) {
    if (t < _state.base_timestamp) {
        return std::nullopt;
    }
    if (_state.empty()) {
        return std::nullopt;
    }
    const uint32_t i = t() - _state.base_timestamp();
    auto it = std::lower_bound(
      std::begin(_state.relative_time_index),
      std::end(_state.relative_time_index),
      i,
      std::less<uint32_t>{});
    if (it == _state.relative_offset_index.end()) {
        return std::nullopt;
    }
    auto dist = std::distance(_state.relative_offset_index.begin(), it);
    return translate_index_entry(_state, _state.get_entry(dist));
}

std::optional<segment_index::entry>
segment_index::find_nearest(model::offset o) {
    vassert(
      o >= _state.base_offset,
      "segment_offset::index::lower_bound cannot find offset:{} below:{}",
      o,
      _state.base_offset);
    if (_state.empty()) {
        return std::nullopt;
    }
    const uint32_t i = o() - _state.base_offset();
    auto it = std::lower_bound(
      std::begin(_state.relative_offset_index),
      std::end(_state.relative_offset_index),
      i,
      std::less<uint32_t>{});
    if (it == _state.relative_offset_index.end()) {
        it = std::prev(it);
    }
    size_t dist = std::distance(_state.relative_offset_index.begin(), it);
    if (*it <= i) {
        return translate_index_entry(_state, _state.get_entry(dist));
    }
    dist = std::distance(_state.relative_offset_index.begin(), it);
    if (dist > 0) {
        it = std::prev(it);
    }
    if (*it <= i) {
        return translate_index_entry(_state, _state.get_entry(dist));
    }
    return std::nullopt;
}

ss::future<> segment_index::truncate(model::offset o) {
    vassert(
      o >= _state.base_offset,
      "segment_index::truncate cannot find offset:{} below:{}",
      o,
      _state.base_offset);
    const uint32_t i = o() - _state.base_offset();
    auto it = std::lower_bound(
      std::begin(_state.relative_offset_index),
      std::end(_state.relative_offset_index),
      i,
      std::less<uint32_t>{});

    if (it != _state.relative_offset_index.end()) {
        _needs_persistence = true;
        int remove_back_elems = std::distance(
          it, _state.relative_offset_index.end());
        while (remove_back_elems-- > 0) {
            _state.pop_back();
        }
        if (_state.empty()) {
            _state.max_timestamp = _state.base_timestamp;
        } else {
            _state.max_timestamp = model::timestamp(
              _state.relative_time_index.back() + _state.base_timestamp());
        }
    }
    return flush();
}

ss::future<bool> segment_index::materialize_index() {
    return _out.size()
      .then([this](uint64_t size) mutable {
          return _out.dma_read_bulk<char>(0, size);
      })
      .then([this](ss::temporary_buffer<char> buf) {
          if (buf.empty()) {
              return false;
          }
          iobuf b;
          b.append(std::move(buf));
          iobuf_parser p(std::move(b));
          _state = reflection::adl<index_state>{}.from(p);
          return true;
      });
}

ss::future<> segment_index::flush() {
    if (!_needs_persistence) {
        return ss::make_ready_future<>();
    }
    _needs_persistence = false;
    return _out.truncate(0).then([this] {
        iobuf b;
        reflection::adl<index_state>{}.to(b, std::move(_state));
        auto out = ss::make_file_output_stream(ss::file(_out.dup()));
        return do_with(
          std::move(b),
          std::move(out),
          [](iobuf& buff, ss::output_stream<char>& out) {
              return ss::do_for_each(
                       buff,
                       [&out](const iobuf::fragment& f) {
                           return out.write(f.get(), f.size());
                       })
                .then([&out] { return out.flush(); })
                .then([&out] { return out.close(); });
          });
    });
}
ss::future<> segment_index::close() {
    return flush().then([this] { return _out.close(); });
}
std::ostream& operator<<(std::ostream& o, const segment_index& i) {
    return o << "{file:" << i.filename() << ", offsets:" << i.base_offset()
             << ", index:" << i._state << ", step:" << i._step
             << ", needs_persistence:" << i._needs_persistence << "}";
}
std::ostream& operator<<(std::ostream& o, const segment_index_ptr& i) {
    if (i) {
        return o << "{ptr=" << *i << "}";
    }
    return o << "{ptr=nullptr}";
}

} // namespace storage
