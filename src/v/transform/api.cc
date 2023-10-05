/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/api.h"

#include <stdexcept>

namespace transform {

service::service(
  wasm::runtime* runtime,
  model::node_id self,
  ss::sharded<cluster::plugin_frontend>* plugin_frontend,
  ss::sharded<features::feature_table>* feature_table,
  ss::sharded<raft::group_manager>* group_manager,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<rpc::client>* rpc_client)
  : _runtime(runtime)
  , _self(self)
  , _plugin_frontend(plugin_frontend)
  , _feature_table(feature_table)
  , _group_manager(group_manager)
  , _partition_manager(partition_manager)
  , _rpc_client(rpc_client) {}

service::~service() = default;

ss::future<> service::start() { throw std::runtime_error("unimplemented"); }

ss::future<> service::stop() { throw std::runtime_error("unimplemented"); }

ss::future<cluster::errc> service::delete_transform(model::transform_name) {
    throw std::runtime_error("unimplemented");
}

ss::future<cluster::errc>
service::deploy_transform(model::transform_metadata, iobuf) {
    throw std::runtime_error("unimplemented");
}

} // namespace transform
