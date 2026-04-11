// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/memtable.h"

#include "absl/container/btree_map.h"
#include "base/vassert.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/lookup_result.h"

#include <seastar/util/variant_utils.hh>

#include <memory>
#include <variant>

namespace lsm::db {

using internal::operator""_seqno;

class memtable::iterator : public internal::iterator {
public:
    // The dummy iterator is only a place holder for the linked list in the
    // memtable.
    struct dummy {};
    explicit iterator(dummy) {}

    explicit iterator(ss::lw_shared_ptr<memtable> memtable)
      : _mem(std::move(memtable))
      , _it(std::monostate{}) {}
    iterator(const iterator&) = delete;
    iterator(iterator&&) = delete;
    iterator& operator=(const iterator&) = delete;
    iterator& operator=(iterator&&) = delete;
    ~iterator() override {
        if (_prev) {
            _prev->_next = _next;
        }
        if (_next) {
            _next->_prev = _prev;
        }
    }

    bool valid() const override {
        return ss::visit(
          _it,
          [](std::monostate) { return false; },
          [this](memtable::table::iterator it) {
              return it != _mem->_table.end();
          },
          [](const internal::key&) { return true; });
    }

    ss::future<> seek_to_first() override {
        _it = _mem->_table.begin();
        return ss::now();
    }

    ss::future<> seek_to_last() override {
        _it = _mem->_table.empty() ? _mem->_table.end()
                                   : std::prev(_mem->_table.end());
        return ss::now();
    }

    ss::future<> seek(lsm::internal::key_view target) override {
        _it = _mem->_table.lower_bound(target);
        return ss::now();
    }

    ss::future<> next() override {
        auto& it = restore();
        if (it != _mem->_table.end()) {
            ++it;
        }
        return ss::now();
    }

    ss::future<> prev() override {
        auto& it = restore();
        if (it == _mem->_table.begin()) {
            it = _mem->_table.end();
        } else if (it != _mem->_table.end()) {
            --it;
        } else if (!_mem->_table.empty()) {
            it = std::prev(_mem->_table.end());
        }
        return ss::now();
    }

    lsm::internal::key_view key() override { return restore()->first; }

    iobuf value() override { return restore()->second.share(); }

    void stash_position() {
        ss::visit(
          _it,
          [](std::monostate) {},
          [this](memtable::table::iterator it) {
              if (it == _mem->_table.end()) {
                  _it = std::monostate{};
              } else {
                  _it = it->first;
              }
          },
          [](const internal::key&) {});
    }

private:
    friend class memtable;

    memtable::table::iterator& restore() const {
        using iter = memtable::table::iterator;
        return *ss::visit(
          _it,
          [this](std::monostate) -> iter* {
              return &_it.emplace<iter>(_mem->_table.end());
          },
          [](memtable::table::iterator& it) -> iter* { return &it; },
          [this](const internal::key& key) -> iter* {
              return &_it.emplace<iter>(_mem->_table.find(key));
          });
    }

    iterator* _next = nullptr;
    iterator* _prev = nullptr;
    ss::lw_shared_ptr<memtable> _mem;
    // We need to support stashing the iterator between scheduling points
    // because the underlying absl::btree_map can be mutated.
    // monostate: the iterator is invalid (seek'd past the end, etc)
    // iterator: the table hasn't been changed and this is our current position
    // key: the iterator was valid but the table was modified so we stashed the
    //      key to restore it the next time a seek method is called.
    mutable std::
      variant<std::monostate, memtable::table::iterator, internal::key>
        _it;
};

void memtable::put(internal::key key, iobuf value) {
    internal::key_view::parts parts = internal::key_view{key}.decode();
    vassert(
      parts.type == internal::value_type::value,
      "when add a put to a memtable, keys much be of value type",
      key);
    vassert(
      parts.seqno >= _last_seqno,
      "seqno should only go up: {} >= {}",
      parts.seqno);
    invalidate_iterators();
    _memory_usage += key.memory_usage() + value.memory_usage();
    _last_seqno = parts.seqno;
    _table.emplace(std::move(key), std::move(value));
}

void memtable::remove(internal::key key) {
    internal::key_view::parts parts = internal::key_view{key}.decode();
    vassert(
      parts.type == internal::value_type::tombstone,
      "when add a tombstone to a memtable, keys much be of tombstone type",
      key);
    vassert(
      parts.seqno >= _last_seqno,
      "seqno should only go up: {} >= {}",
      parts.seqno);
    invalidate_iterators();
    iobuf value;
    _memory_usage += key.memory_usage() + value.memory_usage();
    _last_seqno = parts.seqno;
    _table.emplace(std::move(key), std::move(value));
}

void memtable::merge(ss::lw_shared_ptr<memtable> other) {
    vassert(
      _last_seqno < other->last_seqno(),
      "expected new batch seqno to be greater than what is applied: {} < "
      "{}",
      _last_seqno.value_or(0_seqno),
      other->last_seqno().value_or(0_seqno));
    invalidate_iterators();
    _memory_usage += other->approximate_memory_usage();
    _last_seqno = other->last_seqno();
    _table.merge(std::move(other->_table));
}

lookup_result memtable::get(internal::key_view key) {
    dassert(
      key.type() == internal::value_type::value,
      "when getting from the memtable, keys must be of value type",
      key.decode());
    auto it = _table.lower_bound(key.without_type());
    if (it != _table.end() && it->first.user_key() == key.user_key()) {
        if (it->first.type() == internal::value_type::tombstone) {
            return lookup_result::tombstone();
        }
        iobuf& v = it->second;
        return lookup_result::value(v.share());
    }
    return lookup_result::missing();
}

std::unique_ptr<internal::iterator> memtable::create_iterator() {
    auto it = std::make_unique<iterator>(shared_from_this());
    // Insert into our circularly linked list.
    it->_next = _list_holder->_next;
    it->_prev = _list_holder.get();
    _list_holder->_next->_prev = it.get();
    _list_holder->_next = it.get();
    return it;
}

memtable::memtable() noexcept
  : _list_holder(std::make_unique<iterator>(iterator::dummy{})) {
    // initialize our circularly linked list.
    _list_holder->_next = _list_holder.get();
    _list_holder->_prev = _list_holder.get();
}

memtable::~memtable() = default;

void memtable::invalidate_iterators() {
    auto sentinel = _list_holder.get();
    for (auto* it = sentinel->_next; it != sentinel; it = it->_next) {
        it->stash_position();
    }
}

} // namespace lsm::db
