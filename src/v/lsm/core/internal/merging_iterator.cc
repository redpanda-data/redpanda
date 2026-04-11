// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/merging_iterator.h"

#include "container/chunked_vector.h"

#include <seastar/core/coroutine.hh>

#include <memory>
#include <utility>

namespace lsm::internal {

namespace {

enum class direction : uint8_t { forward, backward };

class merging_iterator : public iterator {
public:
    explicit merging_iterator(
      chunked_vector<std::unique_ptr<iterator>> children)
      : _children(std::move(children)) {}

    bool valid() const override { return _current != nullptr; }
    ss::future<> seek_to_first() override {
        for (auto& child : _children) {
            co_await child->seek_to_first();
        }
        find_smallest();
        _dir = direction::forward;
    }
    ss::future<> seek_to_last() override {
        for (auto& child : _children) {
            co_await child->seek_to_last();
        }
        find_largest();
        _dir = direction::backward;
    }
    ss::future<> seek(key_view target) override {
        for (auto& child : _children) {
            co_await child->seek(target);
        }
        find_smallest();
        _dir = direction::forward;
    }

    ss::future<> next() override {
        dassert(valid(), "next must be called on a valid iterator");
        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (_dir != direction::forward) {
            for (auto& child : _children) {
                if (child.get() != _current) {
                    co_await child->seek(key());
                    if (child->valid() && key() == child->key()) {
                        co_await child->next();
                    }
                }
            }
            _dir = direction::forward;
        }
        co_await _current->next();
        find_smallest();
    }
    ss::future<> prev() override {
        dassert(valid(), "prev must be called on a valid iterator");
        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (_dir != direction::backward) {
            for (auto& child : _children) {
                if (child.get() != _current) {
                    co_await child->seek(key());
                    if (child->valid()) {
                        // Child is at first entry >= key. Step back one to be <
                        // key().
                        co_await child->prev();
                    } else {
                        // Child has no entries >= key(). Position at last
                        // entry.
                        co_await child->seek_to_last();
                    }
                }
            }
            _dir = direction::backward;
        }
        co_await _current->prev();
        find_largest();
    }
    key_view key() override {
        dassert(valid(), "key must be called on a valid iterator");
        return _current->key();
    }
    iobuf value() override {
        dassert(valid(), "value must be called on a valid iterator");
        return _current->value();
    }

private:
    void find_smallest() {
        iterator* smallest = nullptr;
        for (auto& child : _children) {
            if (child->valid()) {
                if (smallest == nullptr || child->key() < smallest->key()) {
                    smallest = child.get();
                }
            }
        }
        _current = smallest;
    }

    void find_largest() {
        iterator* largest = nullptr;
        for (auto& child : _children) {
            if (child->valid()) {
                if (largest == nullptr || child->key() > largest->key()) {
                    largest = child.get();
                }
            }
        }
        _current = largest;
    }

    chunked_vector<std::unique_ptr<iterator>> _children;
    iterator* _current = nullptr;
    direction _dir = direction::forward;
};

} // namespace

std::unique_ptr<iterator>
create_merging_iterator(chunked_vector<std::unique_ptr<iterator>> children) {
    if (children.empty()) {
        return iterator::create_empty();
    } else if (children.size() == 1) {
        return std::move(children[0]);
    } else {
        return std::make_unique<merging_iterator>(std::move(children));
    }
}

} // namespace lsm::internal
