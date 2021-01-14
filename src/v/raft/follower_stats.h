/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/metadata.h"
#include "raft/types.h"
#include "vassert.h"

#include <absl/container/node_hash_map.h>

namespace raft {

class follower_stats {
public:
    using container_t = absl::node_hash_map<vnode, follower_index_metadata>;
    using iterator = container_t::iterator;
    using const_iterator = container_t::const_iterator;

    explicit follower_stats(vnode self)
      : _self(self) {}

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
        auto [it, success] = _followers.insert_or_assign(n, std::move(m));
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

    void update_with_configuration(const group_configuration&);

private:
    friend std::ostream& operator<<(std::ostream&, const follower_stats&);
    vnode _self;
    container_t _followers;
};

} // namespace raft
