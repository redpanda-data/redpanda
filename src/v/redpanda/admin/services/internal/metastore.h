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

#include "cloud_topics/level_one/domain/domain_supervisor.h"
#include "cloud_topics/level_one/metastore/leader_router.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "model/fundamental.h"
#include "proto/redpanda/core/admin/internal/cloud_topics/v1/metastore.proto.h"
#include "redpanda/admin/proxy/client.h"

#include <seastar/core/distributed.hh>

namespace cluster {
class metadata_cache;
class shard_table;
} // namespace cluster

namespace admin {

class metastore_service_impl
  : public proto::admin::metastore::metastore_service {
public:
    explicit metastore_service_impl(
      admin::proxy::client proxy_client,
      ss::sharded<cloud_topics::l1::replicated_metastore>* m,
      ss::sharded<cluster::topic_table>* tt,
      ss::sharded<cluster::metadata_cache>* metadata_cache,
      ss::sharded<cluster::shard_table>* shard_table,
      ss::sharded<cloud_topics::l1::domain_supervisor>* domain_supervisor,
      ss::sharded<cloud_topics::l1::leader_router>* leader_router)
      : _proxy_client(std::move(proxy_client))
      , _topic_table(tt)
      , _metastore(m)
      , _metadata_cache(metadata_cache)
      , _shard_table(shard_table)
      , _domain_supervisor(domain_supervisor)
      , _leader_router(leader_router) {}

    seastar::future<proto::admin::metastore::get_offsets_response> get_offsets(
      serde::pb::rpc::context,
      proto::admin::metastore::get_offsets_request) override;

    seastar::future<proto::admin::metastore::get_size_response> get_size(
      serde::pb::rpc::context,
      proto::admin::metastore::get_size_request) override;

    seastar::future<proto::admin::metastore::get_database_stats_response>
      get_database_stats(
        serde::pb::rpc::context,
        proto::admin::metastore::get_database_stats_request) override;

    seastar::future<proto::admin::metastore::write_rows_response> write_rows(
      serde::pb::rpc::context,
      proto::admin::metastore::write_rows_request) override;

    seastar::future<proto::admin::metastore::read_rows_response> read_rows(
      serde::pb::rpc::context,
      proto::admin::metastore::read_rows_request) override;

    seastar::future<proto::admin::metastore::validate_partition_response>
      validate_partition(
        serde::pb::rpc::context,
        proto::admin::metastore::validate_partition_request) override;

    seastar::future<proto::admin::metastore::list_cloud_topics_response>
      list_cloud_topics(
        serde::pb::rpc::context,
        proto::admin::metastore::list_cloud_topics_request) override;

private:
    admin::proxy::client _proxy_client;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cloud_topics::l1::replicated_metastore>* _metastore;
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cloud_topics::l1::domain_supervisor>* _domain_supervisor;
    ss::sharded<cloud_topics::l1::leader_router>* _leader_router;
};

} // namespace admin
