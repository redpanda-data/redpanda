#pragma once

#include "cluster/ntp_callbacks.h"
#include "cluster/partition.h"
#include "cluster/shard_table.h"
#include "model/metadata.h"
#include "raft/consensus_client_protocol.h"
#include "raft/heartbeat_manager.h"
#include "storage/log_manager.h"
#include "utils/named_type.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {
class partition_manager {
public:
    partition_manager(
      model::timeout_clock::duration disk_timeout,
      ss::sharded<rpc::connection_cache>& clients);

    using leader_cb_t = ss::noncopyable_function<void(
      ss::lw_shared_ptr<partition>,
      model::term_id,
      std::optional<model::node_id>)>;

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

    ss::future<> start();
    ss::future<> stop();
    ss::future<consensus_ptr> manage(
      storage::ntp_config,
      raft::group_id,
      std::vector<model::broker>,
      std::optional<raft::consensus::append_entries_cb_t>);

    notification_id_type register_leadership_notification(leader_cb_t cb) {
        auto id = _notification_id++;
        _notifications.push_back(std::make_pair(id, std::move(cb)));
        return id;
    }

    void unregister_leadership_notification(notification_id_type id) {
        auto it = std::find_if(
          _notifications.begin(),
          _notifications.end(),
          [id](const std::pair<notification_id_type, leader_cb_t>& n) {
              return n.first == id;
          });
        if (it != _notifications.end()) {
            _notifications.erase(it);
        }
    }

    std::optional<storage::log> log(const model::ntp& ntp) {
        return _mngr.get(ntp);
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
    void trigger_leadership_notification(raft::leadership_status);
    ss::lw_shared_ptr<raft::consensus> make_consensus(
      raft::group_id,
      std::vector<model::broker>,
      storage::log,
      std::optional<raft::consensus::append_entries_cb_t>);
    model::node_id _self;
    model::timeout_clock::duration _disk_timeout;

    storage::log_manager _mngr;
    raft::consensus_client_protocol _client;
    raft::heartbeat_manager _hbeats;
    /// used to wait for concurrent recoveries
    ss::gate _bg;

    notification_id_type _notification_id{0};
    std::vector<std::pair<notification_id_type, leader_cb_t>> _notifications;
    ntp_callbacks<manage_cb_t> _manage_watchers;
    // XXX use intrusive containers here
    absl::flat_hash_map<model::ntp, ss::lw_shared_ptr<partition>> _ntp_table;
    absl::flat_hash_map<raft::group_id, ss::lw_shared_ptr<partition>>
      _raft_table;

    friend std::ostream& operator<<(std::ostream&, const partition_manager&);
};
} // namespace cluster
