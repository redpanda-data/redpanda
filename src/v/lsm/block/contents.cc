// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/block/contents.h"

#include <seastar/core/align.hh>
#include <seastar/core/file.hh>

#include <cstdlib>

namespace lsm::block {

ss::future<ss::lw_shared_ptr<contents>>
contents::read(io::random_access_file_reader* f, handle h) {
    auto data = co_await f->read(h.offset, h.size);
    co_return ss::make_lw_shared<contents>(contents(std::move(data)));
}

ss::lw_shared_ptr<contents> contents::copy_from(const iobuf& buf) {
    return ss::make_lw_shared<contents>(contents(ioarray::copy_from(buf)));
}

// NOLINTNEXTLINE(*swappable-parameters*)
iobuf contents::share(size_t pos, size_t length) {
    return _data.share(pos, length).as_iobuf();
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray::string_view contents::read_string(size_t pos, size_t length) const {
    return _data.read_string(pos, length);
}

contents::contents(ioarray data)
  : _data(std::move(data)) {}

uint32_t contents::read_fixed32(size_t offset) const {
    uint32_t v = _data.read_fixed32(offset);
    v = ss::le_to_cpu(v);
    return v;
}
} // namespace lsm::block
