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

#include "cluster/logger.h"
#include "model/fundamental.h"
#include "raft/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh> // shard_id

#include <absl/container/node_hash_map.h>

namespace cluster {
/// \brief this is populated by consensus::controller
/// every core will have a _full_ copy of all indexes
class shard_table final {
    struct shard_revision {
        ss::shard_id shard;
        model::revision_id revision;
        /// Only used for additional sanity checks
        bool non_replicable;
    };

public:
    bool contains(const raft::group_id& group) {
        return _group_idx.find(group) != _group_idx.end();
    }
    ss::shard_id shard_for(const raft::group_id& group) {
        return _group_idx.find(group)->second.shard;
    }

    std::optional<model::revision_id> revision_for(const model::ntp& ntp) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            return it->second.revision;
        }
        return std::nullopt;
    }

    /**
     * \brief Lookup the owning shard for an ntp.
     */
    std::optional<ss::shard_id> shard_for(const model::ntp& ntp) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            return it->second.shard;
        }
        return std::nullopt;
    }

    bool update_shard(
      const model::ntp& ntp, ss::shard_id i, model::revision_id rev) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            if (it->second.revision > rev) {
                return false;
            }
            vassert(
              it->second.non_replicable,
              "Attempting to update replicable entry from non_replicable "
              "interface");
        }
        vlog(
          clusterlog.trace,
          "[{}] (non_replicable) updating shard table, shard_id: {}, rev: {}",
          ntp,
          i,
          rev);
        _ntp_idx.insert_or_assign(ntp, shard_revision{i, rev, true});
        return true;
    }

    void update(
      const model::ntp& ntp,
      raft::group_id g,
      ss::shard_id shard,
      model::revision_id rev) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            if (it->second.revision > rev) {
                return;
            }
            vassert(
              !it->second.non_replicable,
              "Attempting to update non_replicable entry from replicable "
              "interface");
        }
        if (auto it = _group_idx.find(g); it != _group_idx.end()) {
            if (it->second.revision > rev) {
                return;
            }
            vassert(
              !it->second.non_replicable,
              "Attempting to update non_replicable entry from replicable "
              "interface");
        }

        vlog(
          clusterlog.trace,
          "[{}] updating shard table, shard_id: {}, rev: {}",
          ntp,
          shard,
          rev);
        _ntp_idx.insert_or_assign(ntp, shard_revision{shard, rev, false});
        _group_idx.insert_or_assign(g, shard_revision{shard, rev, false});
    }

    void
    erase(const model::ntp& ntp, raft::group_id g, model::revision_id rev) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            // The check on exact revision match (= rev case) below is a special
            // case. This only happens when bootstrapping cross core movements
            // on multiple shards at the same time. Bootstrapping across shards
            // is not coordinated and can result in parallel updates to the
            // shard table.
            //
            // A general invariant of the shard table (excluding parallel
            // bootstrapping) is that the command revision of the shutdown `rev`
            // (which invokes erase from the shard table) is strictly greater
            // than the command that added it the table, with highest version
            // eventually prevailing.
            //
            // This is violated during parallel bootstrap with cross core
            // movements where there can be interleaved updates.In this process
            // if there is an exact revision match, it indicates that an other
            // shard already updated the shard table and is starting up the
            // partition and the calling shard should not remove it.
            //
            // Example: consider this sequence o commands [add(ntp),
            // update(shard 0 -> shard 1)]
            // shard 0 bootstraps with [add, update]
            // shard 1 bootstraps with [update]

            // shard 0 treats update as a shutdown on core 0 while shard 1
            // treats it as a bootstrap on shard 1, however both of them use the
            // same command revision of the update.
            // This translates to the following shard table updates.
            // shard 0: (erase ntp, update_rev)
            // shard 1: (add ntp, update_rev)

            // If shard 0 update ends up running after shard 1, we will end up
            // with no shard table entries for this ntp resulting in an
            // availability loss. The equality check guards against these racy
            // update siutations.

            if (it->second.revision >= rev) {
                return;
            }
            vassert(
              !it->second.non_replicable,
              "erasing non_replicable entry from replicable erase interface");
        }
        if (auto it = _group_idx.find(g); it != _group_idx.end()) {
            if (it->second.revision > rev) {
                return;
            }
            vassert(
              !it->second.non_replicable,
              "erasing non_replicable entry from replicable erase interface");
        }

        vlog(
          clusterlog.trace, "[{}] erasing from shard table, rev: {}", ntp, rev);
        _ntp_idx.erase(ntp);
        _group_idx.erase(g);
    }

    void erase(const model::ntp& ntp, model::revision_id rev) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            if (it->second.revision >= rev) {
                return;
            }
            vassert(
              it->second.non_replicable,
              "erasing replicable entry from non_replicable erase interface");
            vlog(
              clusterlog.trace,
              "[{}] erasing from shard table, rev: {} ",
              ntp,
              rev);
            _ntp_idx.erase(it);
        }
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
    absl::node_hash_map<model::ntp, shard_revision> _ntp_idx;
    // raft index
    absl::node_hash_map<raft::group_id, shard_revision> _group_idx;
};
} // namespace cluster
