// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/iter.h"

#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>

#include <memory>

namespace lsm::db {

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries. This iterator combines multiple
// entries for the same user key found in the db representation into a single
// entry while accounting for sequence numbers, deletion markers, overwrites,
// etc.
class db_iter : public internal::iterator {
public:
    // Which direction is the iterator currently moving?
    // (1) When moving forward, the internal iterator is positioned at the exact
    //     entry that yields this->key(), this->value()
    // (2) When moving backwards the internal iterator is positioned just before
    //     all entries whose user key == this->key().
    enum direction : uint8_t { forward, reverse };

    db_iter(
      std::unique_ptr<internal::iterator> db_iter,
      internal::sequence_number seqno,
      ss::lw_shared_ptr<internal::options> options,
      key_sample_fn sample_fn)
      : _iter(std::move(db_iter))
      , _seqno(seqno)
      , _options(std::move(options))
      , _sample_fn(std::move(sample_fn))
      , _bytes_until_sample(random_compaction_period()) {}

    bool valid() const override { return _valid; }
    internal::key_view key() override {
        dassert(valid(), "iterator must be valid to call key()");
        return _dir == forward ? _iter->key() : _saved_key;
    }
    iobuf value() override {
        dassert(valid(), "iterator must be valid to call value()");
        return _dir == forward ? _iter->value() : _saved_value.share();
    }

    ss::future<> seek_to_first() override {
        _dir = forward;
        _saved_value.clear();
        _saved_key = {};
        co_await _iter->seek_to_first();
        if (_iter->valid()) {
            co_await find_next_user_entry(false);
        } else {
            _valid = false;
        }
    }

    ss::future<> seek_to_last() override {
        _dir = reverse;
        _saved_value.clear();
        _saved_key = {};
        co_await _iter->seek_to_last();
        co_await find_prev_user_entry();
    }

    ss::future<> seek(internal::key_view target) override {
        _dir = forward;
        _saved_value.clear();
        _saved_key = {};
        co_await _iter->seek(target);
        if (_iter->valid()) {
            _saved_key = internal::key(target);
            co_await find_next_user_entry(false);
        } else {
            _valid = false;
        }
    }

    ss::future<> next() override {
        dassert(valid(), "iterator must be valid to call next()");
        if (_dir == reverse) {
            _dir = forward;
            // iter is pointing just before the entries for this->key(),
            // so advance into the range of entries for this->key and then
            // use the normal skipping code below.
            if (!_iter->valid()) {
                co_await _iter->seek_to_first();
            } else {
                co_await _iter->next();
            }
            if (!_iter->valid()) {
                _valid = false;
                _saved_key = {};
                co_return;
            }
            // _saved_key already contains the key to skip past.
        } else {
            // Store in _saved_key the current key so we skip it below.
            _saved_key = internal::key(_iter->key());
            co_await _iter->next();
            if (!_iter->valid()) {
                _valid = false;
                _saved_key = {};
                co_return;
            }
        }
        co_await find_next_user_entry(true);
    }

    ss::future<> prev() override {
        dassert(valid(), "iterator must be valid to call prev()");
        if (_dir == forward) {
            _saved_key = internal::key(_iter->key());
            while (true) {
                co_await _iter->prev();
                if (!_iter->valid()) {
                    _valid = false;
                    _saved_key = {};
                    _saved_value.clear();
                    co_return;
                }
                if (_iter->key() < _saved_key) {
                    break;
                }
            }
            _dir = reverse;
        }
        co_await find_prev_user_entry();
    }

private:
    size_t random_compaction_period() {
        return random_generators::get_int(2 * _options->read_bytes_period);
    }

    ss::future<> find_next_user_entry(bool skipping) {
        do { // NOLINT(*-do-while)
            auto parts = co_await parse_key();
            if (parts.seqno <= _seqno) {
                switch (parts.type) {
                case internal::value_type::tombstone:
                    _saved_key = internal::key(_iter->key());
                    skipping = true;
                    break;
                case internal::value_type::value:
                    if (skipping && parts.key <= _saved_key.user_key()) {
                        // Entry hidden
                    } else {
                        _valid = true;
                        _saved_key = {};
                        co_return;
                    }
                    break;
                }
            }
            co_await _iter->next();
        } while (_iter->valid());
        _saved_key = {};
        _valid = false;
    }

    ss::future<> find_prev_user_entry() {
        constexpr static auto tombstone = internal::value_type::tombstone;
        auto value_type = tombstone;
        while (_iter->valid()) {
            auto parts = co_await parse_key();
            if (parts.seqno <= _seqno) {
                if (
                  value_type != tombstone
                  && parts.key < _saved_key.user_key()) {
                    // We enounted a non-deleted value in entries for previous
                    // keys.
                    break;
                }
                value_type = parts.type;
                if (value_type == tombstone) {
                    _saved_key = {};
                    _saved_value.clear();
                } else {
                    _saved_key = internal::key(_iter->key());
                    _saved_value = _iter->value();
                }
            }
            co_await _iter->prev();
        }
        if (value_type == tombstone) {
            _valid = false;
            _saved_key = {};
            _saved_value.clear();
            _dir = forward;
        } else {
            _valid = true;
        }
    }

    ss::future<internal::key_view::parts> parse_key() {
        auto k = _iter->key();
        size_t bytes_read = k.size() + _iter->value().size_bytes();
        while (_bytes_until_sample < bytes_read) {
            _bytes_until_sample += random_compaction_period();
            co_await _sample_fn(k);
        }
        _bytes_until_sample -= std::min(bytes_read, _bytes_until_sample);
        co_return k.decode();
    }

    std::unique_ptr<internal::iterator> _iter;
    internal::sequence_number _seqno;
    ss::lw_shared_ptr<internal::options> _options;
    internal::key _saved_key;
    iobuf _saved_value;
    key_sample_fn _sample_fn;
    size_t _bytes_until_sample;
    direction _dir = forward;
    bool _valid = false;
};

} // namespace

std::unique_ptr<internal::iterator> create_db_iterator(
  std::unique_ptr<internal::iterator> iter,
  internal::sequence_number seqno,
  ss::lw_shared_ptr<internal::options> options,
  key_sample_fn sample_fn) {
    return std::make_unique<db_iter>(
      std::move(iter), seqno, std::move(options), std::move(sample_fn));
}

} // namespace lsm::db
