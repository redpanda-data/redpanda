// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/iterator.h"

#include "lsm/core/exceptions.h"

namespace lsm::internal {

namespace {
class empty_iterator final : public iterator {
    bool valid() const final { return false; }
    ss::future<> seek_to_first() final { return ss::now(); }
    ss::future<> seek_to_last() final { return ss::now(); }
    ss::future<> seek(key_view) final { return ss::now(); }
    ss::future<> next() final {
        throw invalid_argument_exception("next() called on empty iterator");
    }
    ss::future<> prev() final {
        throw invalid_argument_exception("prev() called on empty iterator");
    }
    key_view key() final {
        throw invalid_argument_exception("key() called on empty iterator");
    }
    iobuf value() final {
        throw invalid_argument_exception("value() called on empty iterator");
    }
};
} // namespace

std::unique_ptr<iterator> iterator::create_empty() {
    return std::make_unique<empty_iterator>();
}

} // namespace lsm::internal
