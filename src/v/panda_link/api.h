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

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "model/panda_link.h"
#include "panda_link/errc.h"
#include "panda_link/fwd.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"
#include "ssx/work_queue.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace panda_link {
class service : public ss::peering_sharded_service<service> {
public:
    static constexpr ss::shard_id manager_shard = 0;
    service(
      model::node_id,
      ss::sharded<cluster::panda_link_frontend>* pl_frontend,
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<raft::group_manager>* group_manager);

    service(const service&) = delete;
    service& operator=(const service&) = delete;
    service(service&&) = delete;
    service& operator=(service&&) = delete;
    ~service();

    ss::future<> start();
    ss::future<> stop();

    ss::future<result<void>> create_link(model::panda_link_metadata);

private:
    void register_notifications();
    void unregister_notifications();

    void on_leadership_notification(
      raft::group_id, model::term_id, std::optional<model::node_id>);
    ss::future<> handle_on_leadership_notification(
      ss::foreign_ptr<ss::lw_shared_ptr<cluster::partition>>,
      std::optional<model::node_id>);

    void on_unmanage_notification(model::topic_partition_view tp);
    ss::future<> handle_on_unmanage_notification(model::ntp ntp);

    void on_manage_notification(const ss::lw_shared_ptr<cluster::partition>&);
    ss::future<> handle_on_manage_notification(
      ss::foreign_ptr<ss::lw_shared_ptr<cluster::partition>>);

private:
    ss::gate _gate;
    model::node_id _self;
    ss::sharded<cluster::panda_link_frontend>* _pl_frontend;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<raft::group_manager>* _group_manager;
    std::unique_ptr<manager> _manager;
    std::vector<ss::deferred_action<ss::noncopyable_function<void()>>>
      _notification_cleanups;
    ssx::work_queue _queue;
};
} // namespace panda_link
