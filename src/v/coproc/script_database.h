/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "coproc/types.h"

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <vector>

namespace coproc::wasm {
/// In memory cache of all coprocessors of all coprocessors
///
/// Useful when its necessary to redeploy or reference a shutdown or partitally
/// shutdown coprocessor for its metadata
class script_database {
public:
    struct script_metadata {
        iobuf source;
        std::vector<topic_namespace_policy> inputs;
    };

    using underlying_t = absl::node_hash_map<script_id, script_metadata>;
    using inverted_lookup_t = absl::node_hash_map<
      model::topic_namespace,
      absl::node_hash_set<script_id>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;
    using const_iterator = underlying_t::const_iterator;
    using const_inv_iterator = inverted_lookup_t::const_iterator;

    /// Querying
    /// ... by scripts
    bool exists(script_id id) { return _db.contains(id); }
    const_iterator find(script_id id) const { return _db.find(id); }
    const_iterator cbegin() const { return _db.cbegin(); }
    const_iterator cend() const { return _db.cend(); }

    /// ... by topic/namespace
    bool exists(model::topic_namespace_view tnv) const {
        return _by_input.contains(tnv);
    }
    const_inv_iterator find(model::topic_namespace_view tnv) const {
        return _by_input.find(tnv);
    }
    const_inv_iterator inv_cbegin() const { return _by_input.cbegin(); }
    const_inv_iterator inv_cend() const { return _by_input.cend(); }

    underlying_t::size_type size() const { return _db.size(); }
    inverted_lookup_t::size_type unique_inputs() const {
        return _by_input.size();
    }

    /// Removal
    bool remove_script(script_id id) {
        auto found = _db.find(id);
        if (found == _db.end()) {
            return false;
        }
        for (const auto& e : found->second.inputs) {
            _by_input.erase(e.tn);
        }
        _db.erase(found);
        return true;
    }
    void clear() {
        _db.clear();
        _by_input.clear();
    }

    /// Addition
    bool add_script(
      script_id id, iobuf source, std::vector<topic_namespace_policy> inputs) {
        script_metadata meta{.source = std::move(source), .inputs = inputs};
        auto [_, success] = _db.emplace(id, std::move(meta));
        if (!success) {
            return false;
        }
        for (auto& input : inputs) {
            auto found = _by_input.find(input.tn);
            if (found == _by_input.end()) {
                absl::node_hash_set<script_id> ns;
                ns.emplace(id);
                _by_input.emplace(std::move(input.tn), std::move(ns));
            } else {
                found->second.emplace(id);
            }
        }
        return true;
    }

private:
    underlying_t _db;
    inverted_lookup_t _by_input;
};

/// \brief script_database is a sharded service that only exists on shard 0.
/// Use this as its 'home_shard' when calling 'invoke_on'
static constexpr ss::shard_id script_database_main_shard = 0;

} // namespace coproc::wasm
