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

#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/concepts-enabled.h"
#include "utils/expiring_promise.h"

#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

namespace cluster {

/// Partition leaders contains information about currently elected partition
/// leaders. It allows user to wait for ntp leader to be elected. Partition
/// leaders are instanciated on each core i.e. each core contains copy op
/// partition leaders. Partition leaders are updated through notification
/// received by cluster::metadata_dissemination_service.
class partition_leaders_table {
public:
    explicit partition_leaders_table(ss::sharded<topic_table>&);

    ss::future<> stop();

    std::optional<model::node_id> get_leader(const model::ntp&) const;

    std::optional<model::node_id>
      get_leader(model::topic_namespace_view, model::partition_id) const;

    ss::future<model::node_id> wait_for_leader(
      const model::ntp&,
      ss::lowres_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    // clang-format off
    template<typename Func>
    CONCEPT(requires requires(
      Func f, 
      model::topic_namespace_view tp_ns, 
      model::partition_id pid, 
      std::optional<model::node_id> leader, 
      model::term_id term) {
            { f(tp_ns, pid, leader, term) } -> std::same_as<void>;
    })
    // clang-format on
    void for_each_leader(Func&& f) const {
        for (auto& [k, v] : _leaders) {
            f(k.tp_ns, k.pid, v.id, v.update_term);
        }
    }

    void remove_leader(const model::ntp& ntp) {
        _leaders.erase(
          leader_key_view{model::topic_namespace_view(ntp), ntp.tp.partition});
    }

    void update_partition_leader(
      const model::ntp&, model::term_id, std::optional<model::node_id>);

private:
    // optimized to reduce number of ntp copies
    struct leader_key {
        model::topic_namespace tp_ns;
        model::partition_id pid;
        template<typename H>
        friend H AbslHashValue(H h, const leader_key& lkv) {
            return H::combine(std::move(h), lkv.tp_ns, lkv.pid);
        }
    };
    struct leader_key_view {
        model::topic_namespace_view tp_ns;
        model::partition_id pid;

        template<typename H>
        friend H AbslHashValue(H h, const leader_key_view& lkv) {
            return H::combine(std::move(h), lkv.tp_ns, lkv.pid);
        }
    };
    // make leader_key queryable with leader_key_view
    struct leader_key_hash {
        using is_transparent = void;

        size_t operator()(leader_key_view v) const {
            return absl::Hash<leader_key_view>{}(v);
        }

        size_t operator()(const leader_key& v) const {
            return absl::Hash<leader_key>{}(v);
        }
    };

    struct leader_key_eq {
        using is_transparent = void;

        bool operator()(leader_key_view lhs, leader_key_view rhs) const {
            return lhs.tp_ns.ns == rhs.tp_ns.ns && lhs.tp_ns.tp == rhs.tp_ns.tp
                   && lhs.pid == rhs.pid;
        }

        bool operator()(const leader_key& lhs, const leader_key& rhs) const {
            return lhs.tp_ns.ns == rhs.tp_ns.ns && lhs.tp_ns.tp == rhs.tp_ns.tp
                   && lhs.pid == rhs.pid;
        }

        bool operator()(const leader_key& lhs, leader_key_view rhs) const {
            return lhs.tp_ns.ns == rhs.tp_ns.ns && lhs.tp_ns.tp == rhs.tp_ns.tp
                   && lhs.pid == rhs.pid;
        }

        bool operator()(leader_key_view lhs, const leader_key& rhs) const {
            return lhs.tp_ns.ns == rhs.tp_ns.ns && lhs.tp_ns.tp == rhs.tp_ns.tp
                   && lhs.pid == rhs.pid;
        }
    };

    // in order to filter out reordered requests we store last update term
    struct leader_meta {
        std::optional<model::node_id> id;
        model::term_id update_term;
    };

    absl::flat_hash_map<leader_key, leader_meta, leader_key_hash, leader_key_eq>
      _leaders;

    // per-ntp notifications for leadership election. note that the
    // namespace is currently ignored pending an update to the metadata
    // cache that attaches a namespace to all topics partition references.
    int32_t _promise_id = 0;
    using promises_t = absl::node_hash_map<
      model::ntp,
      absl::node_hash_map<int32_t, expiring_promise<model::node_id>>>;

    promises_t _leader_promises;

    ss::sharded<topic_table>& _topic_table;
};

} // namespace cluster
