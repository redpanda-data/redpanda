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

#include "cluster/errc.h"
#include "cluster/plugin_frontend.h"
#include "features/feature_table.h"
#include "model/timeout_clock.h"
#include "transform/logger.h"
#include "transform/rpc/client.h"

#include <seastar/coroutine/as_future.hh>

namespace transform {

namespace {
constexpr auto wasm_binary_timeout = std::chrono::seconds(3);
constexpr auto metadata_timeout = std::chrono::seconds(1);
} // namespace

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
service::deploy_transform(model::transform_metadata meta, iobuf binary) {
    if (!_feature_table->local().is_active(
          features::feature::wasm_transforms)) {
        co_return cluster::errc::feature_disabled;
    }
    auto _ = _gate.hold();

    vlog(
      tlog.info,
      "deploying wasm binary (size={}) for transform {}",
      binary.size_bytes(),
      meta.name);
    // TODO(rockwood): Validate that the wasm adheres to our ABI
    auto result = co_await _rpc_client->local().store_wasm_binary(
      std::move(binary), wasm_binary_timeout);
    if (result.has_error()) {
        vlog(
          tlog.warn, "storing wasm binary for transform {} failed", meta.name);
        co_return result.error();
    }
    auto [key, offset] = result.value();
    meta.uuid = key;
    meta.source_ptr = offset;
    vlog(
      tlog.debug,
      "stored wasm binary for transform {} at offset {}",
      meta.name,
      offset);
    cluster::errc ec = co_await _plugin_frontend->local().upsert_transform(
      meta, model::timeout_clock::now() + metadata_timeout);
    vlog(
      tlog.debug,
      "deploying transform {} result: {}",
      meta.name,
      cluster::error_category().message(int(ec)));
    if (ec != cluster::errc::success) {
        // TODO(rockwood): This is a best effort cleanup, we should also have
        // some sort of GC process as well.
        auto result = co_await ss::coroutine::as_future<cluster::errc>(
          _rpc_client->local().delete_wasm_binary(key, wasm_binary_timeout));
        if (result.failed()) {
            vlog(
              tlog.debug,
              "cleaning up wasm binary failed: {}",
              result.get_exception());
        } else if (auto ec = result.get(); ec != cluster::errc::success) {
            vlog(
              tlog.debug,
              "cleaning up wasm binary failed: {}",
              cluster::error_category().message(int(ec)));
        }
    }
    co_return ec;
}

} // namespace transform
