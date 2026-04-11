// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/two_level_iterator.h"

#include <seastar/core/coroutine.hh>

namespace lsm::internal {

namespace {

class impl : public iterator {
public:
    impl(
      std::unique_ptr<iterator> index_iter, data_iterator_function data_iter_fn)
      : _index_iter(std::move(index_iter))
      , _data_iter_fn(std::move(data_iter_fn)) {}

    ~impl() override = default;

    bool valid() const override { return _data_iter && _data_iter->valid(); }

    ss::future<> seek_to_first() override {
        co_await _index_iter->seek_to_first();
        co_await init_data_block();
        if (_data_iter) {
            co_await _data_iter->seek_to_first();
        }
        co_await skip_empty_data_blocks_forward();
    }

    ss::future<> seek_to_last() override {
        co_await _index_iter->seek_to_last();
        co_await init_data_block();
        if (_data_iter) {
            co_await _data_iter->seek_to_last();
        }
        co_await skip_empty_data_blocks_backward();
    }

    ss::future<> seek(key_view target) override {
        co_await _index_iter->seek(target);
        co_await init_data_block();
        if (_data_iter) {
            co_await _data_iter->seek(target);
        }
        co_await skip_empty_data_blocks_forward();
    }

    ss::future<> next() override {
        assert(valid());
        co_await _data_iter->next();
        co_await skip_empty_data_blocks_forward();
    }

    ss::future<> prev() override {
        assert(valid());
        co_await _data_iter->prev();
        co_await skip_empty_data_blocks_backward();
    }

    key_view key() override {
        assert(valid());
        return _data_iter->key();
    }

    iobuf value() override {
        assert(valid());
        return _data_iter->value();
    }

private:
    ss::future<> init_data_block() {
        if (!_index_iter->valid()) {
            _data_iter = nullptr;
            co_return;
        }
        auto handle = _index_iter->value();
        _data_iter = co_await _data_iter_fn(std::move(handle));
    }
    ss::future<> skip_empty_data_blocks_forward() {
        while (!_data_iter || !_data_iter->valid()) {
            if (!_index_iter->valid()) {
                _data_iter = nullptr;
                co_return;
            }
            co_await _index_iter->next();
            co_await init_data_block();
            if (_data_iter) {
                co_await _data_iter->seek_to_first();
            }
        }
    }
    ss::future<> skip_empty_data_blocks_backward() {
        while (!_data_iter || !_data_iter->valid()) {
            if (!_index_iter->valid()) {
                _data_iter = nullptr;
                co_return;
            }
            co_await _index_iter->prev();
            co_await init_data_block();
            if (_data_iter) {
                co_await _data_iter->seek_to_last();
            }
        }
    }

    std::unique_ptr<iterator> _index_iter;
    data_iterator_function _data_iter_fn;
    // May be nullptr, since it's not at an API boundary we do not use
    // ss::optimized_optional, as that makes the code slightly harder
    // to read.
    std::unique_ptr<iterator> _data_iter;
};

} // namespace

std::unique_ptr<iterator> create_two_level_iterator(
  std::unique_ptr<iterator> index_iter, data_iterator_function data_iter_fn) {
    return std::make_unique<impl>(
      std::move(index_iter), std::move(data_iter_fn));
}

} // namespace lsm::internal
