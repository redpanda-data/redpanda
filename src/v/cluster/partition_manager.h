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

#include "cluster/ntp_callbacks.h"
#include "cluster/partition.h"
#include "model/metadata.h"
#include "raft/consensus_client_protocol.h"
#include "raft/group_manager.h"
#include "raft/heartbeat_manager.h"
#include "storage/api.h"
#include "utils/named_type.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {
class partition_manager {
public:
    partition_manager(
      ss::sharded<storage::api>&, ss::sharded<raft::group_manager>&);

    using manage_cb_t
      = ss::noncopyable_function<void(ss::lw_shared_ptr<partition>)>;

    inline ss::lw_shared_ptr<partition> get(const model::ntp& ntp) const {
        if (auto it = _ntp_table.find(ntp); it != _ntp_table.end()) {
            return it->second;
        }
        return nullptr;
    }

    /// \brief raw api for raft/service.h
    inline consensus_ptr consensus_for(raft::group_id group) const {
        if (auto it = _raft_table.find(group); it != _raft_table.end()) {
            return it->second->raft();
        }
        return nullptr;
    }

    inline ss::lw_shared_ptr<partition>
    partition_for(raft::group_id group) const {
        if (auto it = _raft_table.find(group); it != _raft_table.end()) {
            return it->second;
        }
        return nullptr;
    }

    ss::future<> start() { return ss::now(); }
    ss::future<> stop();
    ss::future<consensus_ptr>
      manage(storage::ntp_config, raft::group_id, std::vector<model::broker>);

    ss::future<> remove(const model::ntp& ntp);

    std::optional<storage::log> log(const model::ntp& ntp) {
        return _storage.log_mgr().get(ntp);
    }

    /*
     * register for notification of new partitions within the specific topic
     * being managed. this will invoke the callback for existing partitions
     * synchronously so the caller must be prepared for that.
     *
     * the callback must not block.
     *
     * we don't currently have any mechanism for un-managing partitions, so that
     * interface is non-existent.
     */
    notification_id_type register_manage_notification(
      const model::ns& ns, const model::topic& topic, manage_cb_t cb) {
        /*
         * first create a cb filter and apply all existing partitions. this
         * ensures that the caller initializes its state with existing
         * partitions.
         */
        ntp_callbacks<manage_cb_t> init;
        init.register_notify(
          ns, topic, [&cb](ss::lw_shared_ptr<partition> p) { cb(p); });
        for (auto& e : _ntp_table) {
            init.notify(e.first, e.second);
        }

        // now setup the permenant callback for new partitions
        return _manage_watchers.register_notify(ns, topic, std::move(cb));
    }

    void unregister_manage_notification(notification_id_type id) {
        _manage_watchers.unregister_notify(id);
    }

private:
    storage::api& _storage;
    /// used to wait for concurrent recoveries
    ss::sharded<raft::group_manager>& _raft_manager;

    ntp_callbacks<manage_cb_t> _manage_watchers;
    // XXX use intrusive containers here
    absl::flat_hash_map<model::ntp, ss::lw_shared_ptr<partition>> _ntp_table;
    absl::flat_hash_map<raft::group_id, ss::lw_shared_ptr<partition>>
      _raft_table;

    friend std::ostream& operator<<(std::ostream&, const partition_manager&);
};
} // namespace cluster
