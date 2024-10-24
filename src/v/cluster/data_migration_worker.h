/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "cluster/data_migration_types.h"
#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "container/chunked_hash_map.h"
#include "errc.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

namespace cluster::data_migrations {

/*
 * This service performs data migration operations on individual partitions
 */
class worker : public ss::peering_sharded_service<worker> {
public:
    worker(
      model::node_id,
      partition_leaders_table&,
      partition_manager&,
      ss::abort_source&);
    ss::future<> stop();

    ss::future<errc>
    perform_partition_work(model::ntp&& ntp, partition_work&& work);
    void
    abort_partition_work(model::ntp&& ntp, id migration_id, state sought_state);

private:
    struct ntp_state {
        bool is_leader;
        bool is_running = false;
        partition_work work;
        notification_id_type leadership_subscription;
        ss::lw_shared_ptr<ss::promise<errc>> promise
          = ss::make_lw_shared<ss::promise<errc>>();
        ss::lw_shared_ptr<seastar::abort_source> as;

        ntp_state(const ntp_state&) = delete;
        ntp_state& operator=(const ntp_state&) = delete;
        ntp_state(ntp_state&&) = default;
        ntp_state& operator=(ntp_state&&) = default;

        ntp_state(
          bool is_leader,
          partition_work&& work,
          notification_id_type leadership_subscription);
        ~ntp_state() = default;
    };
    using managed_ntps_map_t = chunked_hash_map<model::ntp, ntp_state>;
    using managed_ntp_it = managed_ntps_map_t::iterator;
    using managed_ntp_cit = managed_ntps_map_t::const_iterator;

    ss::future<> handle_operation_result(
      model::ntp ntp, id migration_id, state desired_state, errc ec);
    void handle_leadership_update(const model::ntp& ntp, bool is_leader);
    void unmanage_ntp(managed_ntp_cit it, errc result);
    void spawn_work_if_leader(managed_ntp_it it);

    // also resulting future cannot throw when co_awaited
    ss::future<errc> do_work(managed_ntp_cit it) noexcept;
    ss::future<errc> do_work(
      const model::ntp& ntp,
      state sought_state,
      const inbound_partition_work_info& pwi);
    ss::future<errc> do_work(
      const model::ntp& ntp,
      state sought_state,
      const outbound_partition_work_info&);

    ss::future<errc> block(ss::lw_shared_ptr<partition> partition, bool block);
    ss::future<errc> flush(ss::lw_shared_ptr<partition> partition);

    model::node_id _self;
    partition_leaders_table& _leaders_table;
    partition_manager& _partition_manager;
    ss::abort_source& _as;
    std::chrono::milliseconds _operation_timeout;

    managed_ntps_map_t _managed_ntps;
    ss::gate _gate;
};

} // namespace cluster::data_migrations
