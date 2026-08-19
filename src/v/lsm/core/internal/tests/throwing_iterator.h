// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"

#include <seastar/core/future.hh>

#include <map>
#include <stdexcept>
#include <utility>

namespace lsm::internal::testing {

// An in-memory iterator that can be primed to throw on its next mutating
// call. Used to drive the exception-safety paths of higher-level
// iterators (db_iter, two_level_iterator, merging_iterator) without
// needing a real cloud-backed SST.
class throwing_iterator : public lsm::internal::iterator {
public:
    explicit throwing_iterator(std::map<lsm::internal::key, iobuf> data)
      : _data(std::move(data))
      , _it(_data.end()) {}

    // Arm the iterator to fail the next mutating call exactly once.
    void fail_next() { _fail_pending = true; }

    bool valid() const override { return _it != _data.end(); }

    ss::future<> seek_to_first() override {
        if (consume_failure()) {
            return make_failure();
        }
        _it = _data.begin();
        return ss::now();
    }

    ss::future<> seek_to_last() override {
        if (consume_failure()) {
            return make_failure();
        }
        _it = _data.empty() ? _data.end() : std::prev(_data.end());
        return ss::now();
    }

    ss::future<> seek(lsm::internal::key_view target) override {
        if (consume_failure()) {
            return make_failure();
        }
        _it = _data.lower_bound(lsm::internal::key(target));
        return ss::now();
    }

    ss::future<> next() override {
        if (consume_failure()) {
            return make_failure();
        }
        if (_it != _data.end()) {
            ++_it;
        }
        return ss::now();
    }

    ss::future<> prev() override {
        if (consume_failure()) {
            return make_failure();
        }
        if (_it == _data.begin()) {
            _it = _data.end();
        } else if (_it != _data.end()) {
            --_it;
        } else if (!_data.empty()) {
            _it = std::prev(_data.end());
        }
        return ss::now();
    }

    lsm::internal::key_view key() override { return _it->first; }
    iobuf value() override { return _it->second.copy(); }

private:
    bool consume_failure() {
        if (_fail_pending) {
            _fail_pending = false;
            return true;
        }
        return false;
    }
    static ss::future<> make_failure() {
        return ss::make_exception_future<>(
          std::runtime_error("simulated iterator failure"));
    }

    std::map<lsm::internal::key, iobuf> _data;
    std::map<lsm::internal::key, iobuf>::iterator _it;
    bool _fail_pending = false;
};

} // namespace lsm::internal::testing
