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
#include "storage/logger.h"
#include "utils/base64.h"
#include "utils/tracking_allocator.h"
#include "vlog.h"

#include <absl/container/btree_map.h>

namespace storage {

class key_offset_map {
public:
    virtual bool put(compaction_key key, model::offset o) = 0;
    virtual std::optional<model::offset>
    get(const compaction_key& key) const = 0;
    virtual bool is_full() const = 0;
    virtual model::offset max_offset() const = 0;
    virtual size_t num_keys() const = 0;
};

class simple_key_offset_map : public key_offset_map {
public:
    static constexpr size_t fake_key_limit = 40;

    simple_key_offset_map()
      : _memory_tracker(
        ss::make_shared<util::mem_tracker>("simple_key_offset_map"))
      , _map(util::mem_tracked::
               map<absl::btree_map, compaction_key, model::offset>(
                 _memory_tracker)) {}

    bool put(compaction_key key, model::offset o) override {
        if (_map.size() >= fake_key_limit) {
            vlog(gclog.info, "Full index!");
            return true;
        }
        auto key_str = iobuf_to_base64(bytes_to_iobuf(key));
        vlog(
          gclog.info,
          "Putting key {}, offset {}, curr size {}",
          key_str,
          o,
          _map.size());
        _map[key] = std::max(o, _map[key]);
        _max_offset = std::max(_max_offset, o);
        return false;
    }

    std::optional<model::offset> get(const compaction_key& key) const override {
        auto iter = _map.find(key);
        if (iter == _map.end()) {
            return std::nullopt;
        }
        return iter->second;
    }

    model::offset max_offset() const override { return _max_offset; }
    size_t num_keys() const override { return _map.size(); }

    bool is_full() const override { return _map.size() >= fake_key_limit; }

private:
    ss::shared_ptr<util::mem_tracker> _memory_tracker;
    util::mem_tracked::map_t<absl::btree_map, compaction_key, model::offset>
      _map;
    model::offset _max_offset;
};

} // namespace storage
