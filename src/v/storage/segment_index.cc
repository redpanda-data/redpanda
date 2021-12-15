// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_index.h"

#include "model/timestamp.h"
#include "serde/serde.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "vassert.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

#include <bits/stdint-uintn.h>
#include <boost/container/container_fwd.hpp>
#include <fmt/format.h>

#include <algorithm>

namespace storage {

static inline segment_index::entry translate_index_entry(
  const index_state& s, std::tuple<uint32_t, uint32_t, uint64_t> entry) {
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

void segment_index::reset() {
    auto base = _state.base_offset;
    _state = {};
    _state.base_offset = base;
    _acc = 0;
}

void segment_index::swap_index_state(index_state&& o) {
    _needs_persistence = true;
    _acc = 0;
    std::swap(_state, o);
}

void segment_index::maybe_track(
  const model::record_batch_header& hdr, size_t filepos) {
    _acc += hdr.size_bytes;
    if (_state.maybe_index(
          _acc,
          _step,
          filepos,
          hdr.base_offset,
          hdr.last_offset(),
          hdr.first_timestamp,
          hdr.max_timestamp)) {
        _acc = 0;
    }
    _needs_persistence = true;
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
    if (o < _state.base_offset || _state.empty()) {
        return std::nullopt;
    }
    const uint32_t needle = o() - _state.base_offset();
    auto it = std::lower_bound(
      std::begin(_state.relative_offset_index),
      std::end(_state.relative_offset_index),
      needle,
      std::less<uint32_t>{});
    if (it == _state.relative_offset_index.end()) {
        it = std::prev(it);
    }
    // make it signed so it can be negative
    int i = std::distance(_state.relative_offset_index.begin(), it);
    do {
        if (_state.relative_offset_index[i] <= needle) {
            return translate_index_entry(_state, _state.get_entry(i));
        }
    } while (i-- > 0);

    return std::nullopt;
}

ss::future<> segment_index::truncate(model::offset o) {
    if (o < _state.base_offset) {
        return ss::now();
    }
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
    }

    if (o < _state.max_offset) {
        _needs_persistence = true;
        if (_state.empty()) {
            _state.max_timestamp = _state.base_timestamp;
            _state.max_offset = _state.base_offset;
        } else {
            _state.max_timestamp = model::timestamp(
              _state.relative_time_index.back() + _state.base_timestamp());
            _state.max_offset = o;
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
          try {
              _state = serde::from_iobuf<index_state>(std::move(b));
              return true;
          } catch (const serde::serde_exception& ex) {
              vlog(
                stlog.info,
                "Rebuilding index_state after decoding failure: {}",
                ex.what());
              return false;
          }
      });
}

ss::future<> segment_index::drop_all_data() {
    reset();
    return _out.truncate(0);
}

ss::future<> segment_index::flush() {
    if (!_needs_persistence) {
        return ss::make_ready_future<>();
    }
    _needs_persistence = false;
    return _out.truncate(0)
      .then(
        [this] { return ss::make_file_output_stream(ss::file(_out.dup())); })
      .then([this](ss::output_stream<char> out) {
          auto b = serde::to_iobuf(_state.copy());
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
std::ostream&
operator<<(std::ostream& o, const std::optional<segment_index::entry>& e) {
    if (e) {
        return o << *e;
    }
    return o << "{empty segment_index::entry}";
}
std::ostream& operator<<(std::ostream& o, const segment_index::entry& e) {
    return o << "{offset:" << e.offset << ", time:" << e.timestamp
             << ", filepos:" << e.filepos << "}";
}

} // namespace storage
