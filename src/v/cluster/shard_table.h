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

#include "base/seastarx.h"
#include "cluster/logger.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "raft/fundamental.h"

#include <seastar/core/reactor.hh> // shard_id

#include <absl/container/node_hash_map.h>

namespace cluster {
/// \brief this is populated by consensus::controller
/// every core will have a _full_ copy of all indexes
class shard_table final {
    struct shard_revision {
        ss::shard_id shard;
        model::shard_revision_id revision;
    };

public:
    std::optional<ss::shard_id> shard_for(const raft::group_id& group) {
        if (auto it = _group_idx.find(group); it != _group_idx.end()) {
            return it->second.shard;
        }
        return std::nullopt;
    }

    /**
     * \brief Lookup the owning shard for an ntp.
     */
    template<model::any_ntp T>
    std::optional<ss::shard_id> shard_for(const T& ntp) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            return it->second.shard;
        }
        return std::nullopt;
    }

    void update(
      const model::ntp& ntp,
      raft::group_id g,
      ss::shard_id shard,
      model::shard_revision_id rev) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            if (it->second.revision > rev) {
                return;
            }
        }
        if (auto it = _group_idx.find(g); it != _group_idx.end()) {
            if (it->second.revision > rev) {
                return;
            }
        }

        vlog(
          clusterlog.trace,
          "[{}] updating shard table, shard_id: {}, rev: {}",
          ntp,
          shard,
          rev);
        _ntp_idx.insert_or_assign(ntp, shard_revision{shard, rev});
        _group_idx.insert_or_assign(g, shard_revision{shard, rev});
    }

    void erase(
      const model::ntp& ntp, raft::group_id g, model::shard_revision_id rev) {
        // Sometimes we erase with the same revision as stored (e.g. if a
        // partition gets disabled). This is not a problem during a
        // cross-shard transfer because even though corresponding erase() and
        // update() will have the same shard_revision_id, update() will always
        // come after erase(). Therefore only erases with it->second.revision >
        // rev needs to be rejected.
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            if (it->second.revision > rev) {
                return;
            }
        }
        if (auto it = _group_idx.find(g); it != _group_idx.end()) {
            if (it->second.revision > rev) {
                return;
            }
        }

        vlog(
          clusterlog.trace, "[{}] erasing from shard table, rev: {}", ntp, rev);
        _ntp_idx.erase(ntp);
        _group_idx.erase(g);
    }

private:
    /**
     * Controller backend executes per NTP reconciliation loop on every core of
     * every node in the cluster. Depending on requested replica set update and
     * current state it performs operations, without coordination with other
     * cores. Reconciliation is initiated by controller state machine
     * propagating requested deltas to all controller backend instances. Only
     * point where all controller backend instances on single node have to
     * synchronize is cluster::shard_table. Shard table contains mapping from
     * model::ntp and group id to shard where instances of consensus and log are
     * instantiated. In order to synchronize updates coming from different
     * shards we use MVCC i.e. stale updates are ignored. This way even if one
     * of the cores is behind with its reconciliation loop we can guarantee that
     * cluster::shard_table state isn't corrupted.
     */

    // kafka index
    absl::node_hash_map<
      model::ntp,
      shard_revision,
      model::ktp_hash_eq,
      model::ktp_hash_eq>
      _ntp_idx;
    // raft index
    absl::node_hash_map<raft::group_id, shard_revision> _group_idx;
};
} // namespace cluster
