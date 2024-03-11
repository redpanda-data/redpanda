/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/lowres_clock.hh>

#include <absl/container/node_hash_map.h>

#include <optional>

namespace kafka {

struct partition_metadata {
    partition_metadata(model::offset so, model::offset hw, model::offset lso)
      : start_offset(so)
      , high_watermark(hw)
      , last_stable_offset(lso) {}

    model::offset start_offset;
    model::offset high_watermark;
    model::offset last_stable_offset;
};

class fetch_metadata_cache {
public:
    explicit fetch_metadata_cache() {
        _eviction_timer.set_callback([this] {
            if (!_stopped) {
                evict();
                _eviction_timer.arm(eviction_timeout);
            }
        });
        _eviction_timer.arm(eviction_timeout);
    }

    fetch_metadata_cache(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache(fetch_metadata_cache&&) = default;

    fetch_metadata_cache& operator=(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache& operator=(fetch_metadata_cache&&) = delete;
    ~fetch_metadata_cache() {
        _stopped = true;
        if (_eviction_timer.armed()) {
            _eviction_timer.cancel();
        }
    }

    void insert_or_assign(
      model::ktp ktp,
      model::offset start_offset,
      model::offset hw,
      model::offset lso) {
        _cache.insert_or_assign(std::move(ktp), entry(start_offset, hw, lso));
    }

    std::optional<partition_metadata> get(const model::ktp& ktp) {
        auto it = _cache.find(ktp);
        return it != _cache.end()
                 ? std::make_optional<partition_metadata>(it->second.md)
                 : std::nullopt;
    }

    /**
     * @brief Return the number of items currently cached.
     */
    size_t size() const { return _cache.size(); }

private:
    struct entry {
        entry(model::offset start_offset, model::offset hw, model::offset lso)
          : md(start_offset, hw, lso)
          , timestamp(ss::lowres_clock::now()) {}

        partition_metadata md;
        ss::lowres_clock::time_point timestamp;
    };

    void evict() {
        auto now = ss::lowres_clock::now();
        for (auto it = _cache.begin(); it != _cache.end();) {
            if (it->second.timestamp + eviction_timeout < now) {
                _cache.erase(it++);
                continue;
            }
            ++it;
        }
    }

    constexpr static std::chrono::seconds eviction_timeout{60};
    bool _stopped = false;
    absl::node_hash_map<model::ktp, entry> _cache;
    ss::timer<> _eviction_timer;
};
} // namespace kafka
