/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "storage/key_offset_map.h"

namespace storage {

simple_key_offset_map::simple_key_offset_map(size_t max_keys)
  : _memory_tracker(ss::make_shared<util::mem_tracker>("simple_key_offset_map"))
  , _map(util::mem_tracked::map<absl::btree_map, compaction_key, model::offset>(
      _memory_tracker))
  , _max_keys(max_keys) {}

bool simple_key_offset_map::put(compaction_key key, model::offset o) {
    if (_map.size() >= _max_keys) {
        return false;
    }
    _map[key] = std::max(o, _map[key]);
    _max_offset = std::max(_max_offset, o);
    return true;
}

std::optional<model::offset>
simple_key_offset_map::get(const compaction_key& key) const {
    auto iter = _map.find(key);
    if (iter == _map.end()) {
        return std::nullopt;
    }
    return iter->second;
}

model::offset simple_key_offset_map::max_offset() const { return _max_offset; }

} // namespace storage
