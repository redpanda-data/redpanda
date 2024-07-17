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
#include "cluster/notification.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "raft/fundamental.h"
#include "utils/notification_list.h"

#include <seastar/core/reactor.hh> // shard_id

#include <absl/container/node_hash_map.h>

namespace cluster {
/// \brief this is populated by consensus::controller
/// every core will have a _full_ copy of all indexes
class shard_table final {
    struct shard_revision {
        ss::shard_id shard;
        model::revision_id log_revision;
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
      model::revision_id log_rev) {
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            if (it->second.log_revision > log_rev) {
                return;
            }
        }
        if (auto it = _group_idx.find(g); it != _group_idx.end()) {
            if (it->second.log_revision > log_rev) {
                return;
            }
        }

        vlog(
          clusterlog.trace,
          "[{}] updating shard table, shard_id: {}, log_rev: {}",
          ntp,
          shard,
          log_rev);
        _ntp_idx.insert_or_assign(ntp, shard_revision{shard, log_rev});
        _group_idx.insert_or_assign(g, shard_revision{shard, log_rev});

        _notification_list.notify(ntp, g, shard);
    }

    void
    erase(const model::ntp& ntp, raft::group_id g, model::revision_id log_rev) {
        // Revision check protects against race conditions between operations
        // on instances of the same ntp with different log revisions (e.g. after
        // a topic was deleted and then re-created). These operations can happen
        // on different shards, therefore erase() corresponding to the old
        // instance can happen after update() corresponding to the new one. Note
        // that concurrent updates are not a problem during cross-shard
        // transfers because even though corresponding erase() and update() will
        // have the same log_revision, update() will always come after erase().
        if (auto it = _ntp_idx.find(ntp); it != _ntp_idx.end()) {
            if (it->second.log_revision > log_rev) {
                return;
            }
        }
        if (auto it = _group_idx.find(g); it != _group_idx.end()) {
            if (it->second.log_revision > log_rev) {
                return;
            }
        }

        vlog(
          clusterlog.trace,
          "[{}] erasing from shard table, log_rev: {}",
          ntp,
          log_rev);
        _ntp_idx.erase(ntp);
        _group_idx.erase(g);

        _notification_list.notify(ntp, g, std::nullopt);
    }

    using change_cb_t = ss::noncopyable_function<void(
      const model::ntp& ntp,
      raft::group_id g,
      std::optional<ss::shard_id> shard)>;

    notification_id_type register_notification(change_cb_t&& cb) {
        return _notification_list.register_cb(std::move(cb));
    }

    void unregister_delta_notification(cluster::notification_id_type id) {
        _notification_list.unregister_cb(id);
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
    chunked_hash_map<
      model::ntp,
      shard_revision,
      model::ktp_hash_eq,
      model::ktp_hash_eq>
      _ntp_idx;
    // raft index
    chunked_hash_map<raft::group_id, shard_revision> _group_idx;

    notification_list<change_cb_t, notification_id_type> _notification_list;
};
} // namespace cluster
