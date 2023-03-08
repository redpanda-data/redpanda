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
#include "model/metadata.h"
#include "raft/follower_queue.h"
#include "raft/types.h"
#include "vassert.h"

#include <absl/container/node_hash_map.h>

namespace raft {

class follower_stats {
public:
    using container_t = absl::node_hash_map<vnode, follower_index_metadata>;
    using iterator = container_t::iterator;
    using const_iterator = container_t::const_iterator;

    explicit follower_stats(vnode self, uint32_t max_concurrent_append_entries)
      : _self(self)
      , _max_concurrent_append_entries(max_concurrent_append_entries) {}

    const follower_index_metadata& get(vnode n) const {
        auto it = _followers.find(n);
        vassert(
          it != _followers.end(),
          "Cannot get non-existent follower index {}, key:{}",
          *this,
          n);
        return it->second;
    }
    follower_index_metadata& get(vnode n) {
        auto it = _followers.find(n);
        vassert(
          it != _followers.end(),
          "Cannot get non-existent follower index {}, key:{}",
          *this,
          n);
        return it->second;
    }

    bool contains(vnode n) const {
        return _followers.find(n) != _followers.end();
    }

    iterator emplace(vnode n, follower_index_metadata m) {
        _followers.erase(n);
        auto [it, success] = _followers.emplace(n, std::move(m));
        vassert(success, "could not insert node:{}", n);
        return it;
    }

    iterator find(vnode n) { return _followers.find(n); }
    const_iterator find(vnode n) const { return _followers.find(n); }

    iterator begin() { return _followers.begin(); }
    iterator end() { return _followers.end(); }
    const_iterator begin() const { return _followers.begin(); }
    const_iterator end() const { return _followers.end(); }

    size_t size() const { return _followers.size(); }

    ss::future<ssx::semaphore_units> get_append_entries_unit(vnode);

    void return_append_entries_units(vnode);

    void update_with_configuration(const group_configuration&);

    void reset() {
        for (auto& [_, meta] : _followers) {
            meta.reset();
        }
    }

private:
    friend std::ostream& operator<<(std::ostream&, const follower_stats&);
    vnode _self;
    uint32_t _max_concurrent_append_entries;
    container_t _followers;
    absl::node_hash_map<vnode, follower_queue> _queues;
};

} // namespace raft
