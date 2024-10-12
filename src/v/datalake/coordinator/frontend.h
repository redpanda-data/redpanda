/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "datalake/coordinator/rpc_service.h"
#include "datalake/coordinator/types.h"
#include "datalake/fwd.h"
#include "model/namespace.h"
#include "raft/fwd.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace datalake::coordinator {

/*
 * Frontend is the gateway into the coordinator state machines on a given shard.
 * One frontend instance per shard.
 */
class frontend : public ss::peering_sharded_service<frontend> {
public:
    using local_only = ss::bool_class<struct local_only>;

    frontend(
      model::node_id self,
      ss::sharded<coordinator_manager>*,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<cluster::topics_frontend>*,
      ss::sharded<cluster::metadata_cache>*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*);

    ss::future<> stop();

    ss::future<add_translated_data_files_reply> add_translated_data_files(
      add_translated_data_files_request, local_only = local_only::no);

    ss::future<fetch_latest_data_file_reply> fetch_latest_data_file(
      fetch_latest_data_file_request, local_only = local_only::no);

private:
    using proto_t = datalake::coordinator::rpc::impl::
      datalake_coordinator_rpc_client_protocol;
    using client = datalake::coordinator::rpc::impl::
      datalake_coordinator_rpc_client_protocol;

    static constexpr std::chrono::seconds rpc_timeout{5};

    // utilities for boiler plate RPC code.

    template<auto Func, typename req_t>
    requires requires(proto_t f, req_t req, ::rpc::client_opts opts) {
        (f.*Func)(std::move(req), std::move(opts));
    }
    auto remote_dispatch(req_t request, model::node_id leader_id);

    template<auto LocalFunc, auto RemoteFunc, typename req_t>
    requires requires(
      datalake::coordinator::frontend f, const model::ntp& ntp, req_t req) {
        (f.*LocalFunc)(std::move(req), ntp, ss::shard_id{0});
    }
    auto process(req_t req, bool local_only);

    ss::future<bool> ensure_topic_exists();

    std::optional<model::partition_id>
    coordinator_partition(const model::topic_partition&) const;

    ss::future<add_translated_data_files_reply>
    add_translated_data_files_locally(
      add_translated_data_files_request,
      const model::ntp& coordinator_partition,
      ss::shard_id);

    ss::future<fetch_latest_data_file_reply> fetch_latest_data_file_locally(
      fetch_latest_data_file_request,
      const model::ntp& coordinator_partition,
      ss::shard_id);

    model::node_id _self;
    ss::sharded<coordinator_manager>* _coordinator_mgr;
    ss::sharded<raft::group_manager>* _group_mgr;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topics_frontend>* _topics_frontend;
    ss::sharded<cluster::metadata_cache>* _metadata;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<::rpc::connection_cache>* _connection_cache;
    ss::gate _gate;
};
} // namespace datalake::coordinator
