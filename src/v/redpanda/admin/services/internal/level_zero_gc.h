/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/health_monitor_frontend.h"
#include "cluster/topic_table.h"
#include "proto/redpanda/core/admin/internal/cloud_topics/v1/level_zero_gc.proto.h"

#include <seastar/core/distributed.hh>

namespace admin {

class level_zero_gc_service_impl
  : public proto::admin::level_zero_gc::level_zero_gc_service {
public:
    explicit level_zero_gc_service_impl(
      ss::sharded<cluster::topic_table>* tt,
      ss::sharded<cluster::health_monitor_frontend>* hm)
      : _topic_table(tt)
      , _health_monitor(hm) {}

    seastar::future<proto::admin::level_zero_gc::advance_epoch_response>
    advance_epoch(
      serde::pb::rpc::context,
      proto::admin::level_zero_gc::advance_epoch_request) override;

    seastar::future<proto::admin::level_zero_gc::get_epoch_response> get_epoch(
      serde::pb::rpc::context,
      proto::admin::level_zero_gc::get_epoch_request) override;

private:
    using partition_epoch_map
      = chunked_hash_map<model::partition_id, std::optional<int64_t>>;
    using topic_partition_epoch_map
      = chunked_hash_map<model::topic, partition_epoch_map>;
    ss::future<topic_partition_epoch_map>
      populate_epochs(topic_partition_epoch_map);
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cluster::health_monitor_frontend>* _health_monitor;
};

} // namespace admin
