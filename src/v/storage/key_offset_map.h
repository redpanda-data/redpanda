// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "storage/compacted_index.h"
#include "utils/tracking_allocator.h"
#include "vlog.h"

#include <absl/container/btree_map.h>

namespace storage {

// Map containing the latest offsets of each key.
class key_offset_map {
public:
    // Returns true if the put was successful.
    virtual bool put(compaction_key key, model::offset o) = 0;

    // Returns the latest offset for the given key put into the map.
    virtual std::optional<model::offset>
    get(const compaction_key& key) const = 0;

    // Returns the highest offset indexed by this map.
    virtual model::offset max_offset() const = 0;
};

class simple_key_offset_map : public key_offset_map {
public:
    static constexpr size_t default_key_limit = 1000;

    simple_key_offset_map(size_t max_keys = default_key_limit)
      : _memory_tracker(
        ss::make_shared<util::mem_tracker>("simple_key_offset_map"))
      , _map(util::mem_tracked::
               map<absl::btree_map, compaction_key, model::offset>(
                 _memory_tracker))
      , _max_keys(max_keys) {}

    bool put(compaction_key key, model::offset o) override {
        if (_map.size() >= _max_keys) {
            return false;
        }
        _map[key] = std::max(o, _map[key]);
        _max_offset = std::max(_max_offset, o);
        return true;
    }

    std::optional<model::offset> get(const compaction_key& key) const override {
        auto iter = _map.find(key);
        if (iter == _map.end()) {
            return std::nullopt;
        }
        return iter->second;
    }

    model::offset max_offset() const override { return _max_offset; }

private:
    ss::shared_ptr<util::mem_tracker> _memory_tracker;
    util::mem_tracked::map_t<absl::btree_map, compaction_key, model::offset>
      _map;
    model::offset _max_offset;
    size_t _max_keys;
};

} // namespace storage
