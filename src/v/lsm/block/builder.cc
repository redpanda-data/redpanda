// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/block/builder.h"

#include <limits>
#include <utility>

namespace lsm::block {

builder::builder()
  : builder(builder::options{}) {}

builder::builder(builder::options opts)
  : _restart_interval(opts.restart_interval) {
    // First restart point is at offset 0
    _restarts.push_back(0);
}

size_t builder::current_size_estimate() const { return _buf.size_bytes(); }

void builder::add(internal::key key, iobuf&& value) {
    if (_counter >= _restart_interval) {
        _last_key = {};
        _counter = 0;
        _restarts.push_back(_buf.size_bytes());
    }
    uint32_t shared = 0;
    uint32_t minlen = std::min(key.size(), _last_key.size());
    while (shared < minlen && key[shared] == _last_key[shared]) {
        ++shared;
    }
    uint32_t unique = key.size() - shared;
    dassert(
      value.size_bytes() < std::numeric_limits<uint32_t>::max(),
      "value size too large");
    uint32_t valuelen = value.size_bytes();
    for (uint32_t size : {shared, unique, valuelen}) {
        _buf.append(std::bit_cast<std::array<uint8_t, sizeof(size)>>(size));
    }
    // Add the key and value.
    _buf.append(&key[shared], unique);
    _buf.append(std::move(value));
    _last_key = std::move(key);
    ++_counter;
}

iobuf builder::finish() {
    for (uint32_t restart : _restarts) {
        _buf.append(
          std::bit_cast<std::array<uint8_t, sizeof(restart)>>(restart));
    }
    auto num = static_cast<uint32_t>(_restarts.size());
    _buf.append(std::bit_cast<std::array<uint8_t, sizeof(num)>>(num));
    _restarts.clear();
    _restarts.push_back(0);
    _last_key = {};
    _counter = 0;
    return std::exchange(_buf, {});
}

} // namespace lsm::block
