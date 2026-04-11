// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/block/reader.h"

#include "lsm/core/exceptions.h"
#include "lsm/core/internal/keys.h"

#include <array>
#include <compare>
#include <iterator>

namespace lsm::block {

namespace {

// A range is a pair of offsets that represent the start and length of a
// value in the block contents.
struct range {
    uint32_t offset = 0;
    uint32_t length = 0;
};

std::array<uint32_t, 3>
decode_entry(const contents& data, uint32_t offset, uint32_t limit) {
    assert(data.size() >= limit);
    if (offset + (3 * sizeof(uint32_t)) > limit) [[unlikely]] {
        throw corruption_exception(
          "corruption: entry too small: {} < {}",
          3 * sizeof(uint32_t),
          data.size() - offset);
    }
    return std::to_array({
      data.read_fixed32(offset),
      data.read_fixed32(offset + sizeof(uint32_t)),
      data.read_fixed32(offset + (2 * sizeof(uint32_t))),
    });
}

class iter final : public internal::iterator {
public:
    iter(
      ss::lw_shared_ptr<contents> data,
      // NOLINTNEXTLINE(*swappable-parameters*)
      uint32_t restarts,
      uint32_t num_restarts)
      : _data(std::move(data))
      , _restarts_offset(restarts)
      , _num_restarts(num_restarts)
      , _current(restarts)
      , _restart_index(num_restarts) {}

    bool valid() const final { return _current < _restarts_offset; }
    ss::future<> seek_to_first() final {
        seek_to_restart_point(0);
        parse_next_key();
        return ss::now();
    }
    ss::future<> seek_to_last() final {
        seek_to_restart_point(_num_restarts - 1);
        while (parse_next_key() && next_entry_offset() < _restarts_offset) {
            // Keep skipping
        }
        return ss::now();
    }
    ss::future<> seek(internal::key_view target) final {
        // Binary search in restart array to find the last restart point
        // with a key < target
        uint32_t left = 0;
        uint32_t right = _num_restarts - 1;
        std::strong_ordering current_key_compare = std::strong_ordering::equal;

        if (valid()) {
            // If we're already scanning, use the current position as a starting
            // point. This is beneficial if the key we're seeking to is ahead of
            // the current position.
            current_key_compare = _key <=> std::string_view(target);
            if (current_key_compare < 0) {
                // _key is smaller than target
                left = _restart_index;
            } else if (current_key_compare > 0) {
                right = _restart_index;
            } else {
                // We're seeking to the key we're already at.
                return ss::now();
            }
        }

        while (left < right) {
            uint32_t mid = (left + right + 1) / 2;
            uint32_t region_offset = get_restart_point(mid);
            auto [shared, non_shared, value_length] = decode_entry(
              *_data, region_offset, _restarts_offset);
            uint32_t p = region_offset + (3 * sizeof(uint32_t));
            if (shared != 0) {
                throw corruption_exception(
                  "corruption: restart key should have no shared prefix, "
                  "got: {}",
                  shared);
            }
            auto mid_key = _data->read_string(p, non_shared);
            if (mid_key < std::string_view(target)) {
                // Key at "mid" is smaller than "target". Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else {
                // Key at "mid" is >= "target". Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            }
        }

        // We might be able to use our current position within the restart
        // block. This is true if we determined the key we desire is in the
        // current block and is after than the current key.
        assert(current_key_compare == 0 || valid());
        bool skip_seek = left == _restart_index && current_key_compare < 0;
        if (!skip_seek) {
            seek_to_restart_point(left);
        }
        // Linear search (within restart block) for first key >= target
        while (true) {
            if (!parse_next_key()) {
                return ss::now();
            }
            if (_key >= std::string_view(target)) {
                return ss::now();
            }
        }
        return ss::now();
    }
    ss::future<> next() final {
        parse_next_key();
        return ss::now();
    }
    ss::future<> prev() final {
        // Scan backwards to a restart point before current_
        const uint32_t original = _current;
        while (get_restart_point(_restart_index) >= original) {
            if (_restart_index == 0) {
                // No more entries
                _current = _restarts_offset;
                _restart_index = _num_restarts;
                return ss::now();
            }
            _restart_index--;
        }

        seek_to_restart_point(_restart_index);
        // NOLINTNEXTLINE(*avoid-do-while*)
        do {
            // Loop until end of current entry hits the start of original entry
        } while (parse_next_key() && next_entry_offset() < original);
        return ss::now();
    }
    internal::key_view key() final {
        return internal::key_view::from_encoded(_key);
    }
    iobuf value() final { return _data->share(_value.offset, _value.length); }

private:
    // Return the offset in data_ just past the end of the current entry.
    uint32_t next_entry_offset() const {
        return (_value.offset + _value.length);
    }

    uint32_t get_restart_point(uint32_t index) {
        return _data->read_fixed32(
          _restarts_offset + (index * sizeof(uint32_t)));
    }

    void seek_to_restart_point(uint32_t index) {
        _key.resize(0);
        _restart_index = index;
        // _current will be fixed by parse_next_key();

        // parse_next_key() starts at the end of _value, so set _value
        // accordingly
        _value = {.offset = get_restart_point(index), .length = 0};
    }

    bool parse_next_key() {
        _current = next_entry_offset();
        // Restarts come right after data
        if (_current >= _restarts_offset) {
            // No more entries to return. Mark as invalid.
            _current = _restarts_offset;
            _restart_index = _num_restarts;
            return false;
        }
        // Decode next entry
        auto [shared, non_shared, value_length] = decode_entry(
          *_data, _current, _restarts_offset);
        uint32_t p = _current + (3 * sizeof(uint32_t));
        if (_key.size() < shared) {
            throw corruption_exception(
              "corruption: shared key too short: {} < {}", _key.size(), shared);
        } else {
            _key.resize(shared + non_shared);
            auto it = _key.begin();
            std::advance(it, shared);
            for (std::string_view s : _data->read_string(p, non_shared)) {
                it = std::ranges::copy(s, it).out;
            }
            _value = range{.offset = p + non_shared, .length = value_length};
            while (_restart_index + 1 < _num_restarts
                   && get_restart_point(_restart_index + 1) < _current) {
                ++_restart_index;
            }
            return true;
        }
    }

    ss::lw_shared_ptr<contents> _data;
    uint32_t _restarts_offset; // Offset of restart array (list of fixed32)
    uint32_t _num_restarts;    // Number of uint32_t entries in restart array

    // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
    uint32_t _current;
    uint32_t _restart_index; // Index of restart block in which current_ falls

    ss::sstring _key;
    range _value;
};

uint32_t num_restarts(const contents& c) {
    return c.read_fixed32(c.size() - sizeof(uint32_t));
}

} // namespace

reader::reader(ss::lw_shared_ptr<contents> c)
  : _data(std::move(c))
  , _restart_offset(0) {
    if (_data->size() > sizeof(uint32_t)) {
        size_t max_restarts_allowed = (_data->size() - sizeof(uint32_t))
                                      / sizeof(uint32_t);
        uint32_t n_restarts = num_restarts(*_data);
        if (n_restarts <= max_restarts_allowed) {
            _restart_offset = _data->size()
                              - ((1 + n_restarts) * sizeof(uint32_t));
        }
    }
}

std::unique_ptr<internal::iterator> reader::create_iterator() {
    if (_data->size() < sizeof(uint32_t)) [[unlikely]] {
        throw corruption_exception(
          "corruption: bad block contents, size: {}", _data->size());
    }
    uint32_t n_restarts = num_restarts(*_data);
    if (n_restarts == 0) {
        return internal::iterator::create_empty();
    } else {
        return std::make_unique<iter>(_data, _restart_offset, n_restarts);
    }
}

} // namespace lsm::block
