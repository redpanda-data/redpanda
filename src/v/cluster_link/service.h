/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "cluster/cluster_link/fwd.h"
#include "cluster/fwd.h"
#include "cluster/utils/partition_change_notifier.h"
#include "cluster_link/fwd.h"
#include "model/fundamental.h"
#include "raft/fundamental.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace cluster_link {
/**
 * @brief API access for cluster link service
 */
class service {
public:
    service(
      ::model::node_id self,
      ss::sharded<::cluster::cluster_link::frontend>* plf,
      std::unique_ptr<cluster::partition_change_notifier> notifications,
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<cluster::partition_leaders_table>* partition_leaders_table,
      ss::sharded<cluster::shard_table>* shard_table,
      ss::sharded<cluster::metadata_cache>* metadata_cache,
      ss::smp_service_group smp_group);

    service(const service&) = delete;
    service(service&&) = delete;
    service& operator=(const service&) = delete;
    service& operator=(service&&) = delete;
    virtual ~service();

    ss::future<> start();
    ss::future<> stop();

private:
    void register_notifications();
    void unregister_notifications();

private:
    ss::gate _gate;
    model::node_id _self;
    ss::sharded<::cluster::cluster_link::frontend>* _plf;
    std::unique_ptr<cluster::partition_change_notifier> _notifications;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<cluster::partition_leaders_table>* _partition_leaders_table;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    ss::smp_service_group _smp_group;
    std::unique_ptr<manager> _manager;
    std::vector<ss::deferred_action<ss::noncopyable_function<void()>>>
      _notification_cleanups;
};
} // namespace cluster_link
