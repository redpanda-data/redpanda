/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/access_time_tracker.h"

#include "cloud_storage/logger.h"
#include "serde/serde.h"
#include "units.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <absl/container/btree_map.h>

#include <exception>
#include <variant>

namespace cloud_storage {

void access_time_tracker::add_timestamp(
  std::string_view key, std::chrono::system_clock::time_point ts) {
    uint32_t seconds = std::chrono::time_point_cast<std::chrono::seconds>(ts)
                         .time_since_epoch()
                         .count();
    uint32_t hash = xxhash_32(key.data(), key.size());
    _table.data[hash] = seconds;
    _dirty = true;
}

void access_time_tracker::remove_timestamp(std::string_view key) noexcept {
    try {
        uint32_t hash = xxhash_32(key.data(), key.size());
        _table.data.erase(hash);
        _dirty = true;
    } catch (...) {
        vlog(
          cst_log.debug,
          "Can't remove key {} from access_time_tracker, exception: {}",
          key,
          std::current_exception());
    }
}

void access_time_tracker::remove_others(const access_time_tracker& t) {
    table_t tmp;
    for (auto it : _table.data) {
        if (t._table.data.contains(it.first)) {
            tmp.data.insert(it);
        }
    }
    _table = std::move(tmp);
}

std::optional<std::chrono::system_clock::time_point>
access_time_tracker::estimate_timestamp(std::string_view key) const {
    uint32_t hash = xxhash_32(key.data(), key.size());
    auto it = _table.data.find(hash);
    if (it == _table.data.end()) {
        return std::nullopt;
    }
    auto seconds = std::chrono::seconds(it->second);
    std::chrono::system_clock::time_point ts(seconds);
    return ts;
}

iobuf access_time_tracker::to_iobuf() {
    _dirty = false;
    return serde::to_iobuf(_table);
}

void access_time_tracker::from_iobuf(iobuf b) {
    iobuf_parser parser(std::move(b));
    _table = serde::read<table_t>(parser);
    _dirty = false;
}

bool access_time_tracker::is_dirty() const { return _dirty; }

} // namespace cloud_storage
