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

#include "cluster/ntp_callbacks.h"
#include "cluster/partition.h"
#include "coproc/partition.h"
#include "storage/fwd.h"

#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

namespace coproc {

class partition_manager {
public:
    using ntp_table_container
      = absl::flat_hash_map<model::ntp, ss::lw_shared_ptr<partition>>;
    using manage_cb_t
      = ss::noncopyable_function<void(ss::lw_shared_ptr<partition>)>;
    using unmanage_cb_t = ss::noncopyable_function<void(model::partition_id)>;

    explicit partition_manager(ss::sharded<storage::api>& storage) noexcept;

    ss::future<> start() { return ss::now(); }
    ss::future<> stop_partitions();

    ss::lw_shared_ptr<partition> get(const model::ntp& ntp) const;
    ss::future<>
      manage(storage::ntp_config, ss::lw_shared_ptr<cluster::partition>);

    ss::future<> remove(const model::ntp&);

    /// Recieve updates when a partition with matching ns, topic is added
    cluster::notification_id_type register_manage_notification(
      const model::ns& ns, const model::topic& topic, manage_cb_t cb);

    /// Recieve updates when a partition with matching ns, topic is removed
    cluster::notification_id_type register_unmanage_notification(
      const model::ns& ns, const model::topic& topic, unmanage_cb_t cb) {
        return _unmanage_watchers.register_notify(ns, topic, std::move(cb));
    }
    void unregister_manage_notification(cluster::notification_id_type id) {
        _manage_watchers.unregister_notify(id);
    }
    void unregister_unmanage_notification(cluster::notification_id_type id) {
        _unmanage_watchers.unregister_notify(id);
    }

    /*
     * read-only interface to partitions.
     *
     * note that users of this interface must take care not to hold iterators
     * across scheduling events as the underlying table may be modified and
     * invalidate iterators.
     */
    const ntp_table_container& partitions() const { return _ntp_table; }

private:
    ss::future<> do_shutdown(ss::lw_shared_ptr<partition>);

private:
    ntp_table_container _ntp_table;
    cluster::ntp_callbacks<manage_cb_t> _manage_watchers;
    cluster::ntp_callbacks<unmanage_cb_t> _unmanage_watchers;

    ss::gate _gate;
    storage::api& _storage;

    friend std::ostream& operator<<(std::ostream&, const partition_manager&);
};

} // namespace coproc
