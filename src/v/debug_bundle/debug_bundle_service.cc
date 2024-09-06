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

#include "debug_bundle_service.h"

#include "config/configuration.h"
#include "debug_bundle/error.h"
#include "debug_bundle/types.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>

namespace debug_bundle {
static ss::logger lg{"debug-bundle-service"};

debug_bundle_service::debug_bundle_service(
  const std::filesystem::path& data_dir)
  : _debug_bundle_dir(data_dir / debug_bundle_dir_name)
  , _rpk_path_binding(config::shard_local_cfg().rpk_path.bind())
  , _rpk_path(config::shard_local_cfg().rpk_path()) {
    _rpk_path_binding.watch([this] {
        _rpk_path = config::shard_local_cfg().rpk_path();
        lg.trace("Changing rpk path to {}", _rpk_path);
    });
}

ss::future<> debug_bundle_service::start() {
    if (ss::this_shard_id() != service_shard) {
        co_return;
    }

    try {
        lg.trace("Creating {}", _debug_bundle_dir);
        co_await ss::recursive_touch_directory(_debug_bundle_dir.native());
    } catch (const std::exception& e) {
        throw std::system_error(
          make_error_code(error_code::internal_error), e.what());
    }

    if (!co_await ss::file_exists(_rpk_path.native())) {
        lg.warn(
          "Current specified RPK location {} does not exist!  Debug "
          "bundle creation is not available until this is fixed!",
          _rpk_path.native());
    }

    lg.debug("Service started");
}

ss::future<> debug_bundle_service::stop() {
    if (ss::this_shard_id() != service_shard) {
        co_return;
    }
    lg.debug("Service stopping");
    co_return;
}

ss::future<result<void>>
debug_bundle_service::initiate_rpk_debug_bundle_collection(
  uuid_t, debug_bundle_parameters) {
    co_return error_info(error_code::internal_error, "Not yet implemented");
}

ss::future<result<void>> debug_bundle_service::cancel_rpk_debug_bundle(uuid_t) {
    co_return error_info(error_code::debug_bundle_process_never_started);
}

ss::future<result<debug_bundle_status_data>>
debug_bundle_service::rpk_debug_bundle_status() {
    co_return error_info(error_code::debug_bundle_process_never_started);
}

ss::future<result<std::filesystem::path>>
debug_bundle_service::rpk_debug_bundle_path() {
    co_return error_info(error_code::debug_bundle_process_never_started);
}

ss::future<result<void>> debug_bundle_service::delete_rpk_debug_bundle() {
    co_return error_info(error_code::debug_bundle_process_never_started);
}

} // namespace debug_bundle
