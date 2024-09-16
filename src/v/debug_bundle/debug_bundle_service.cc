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

service::service(const std::filesystem::path& data_dir)
  : _debug_bundle_dir(data_dir / debug_bundle_dir_name)
  , _rpk_path_binding(config::shard_local_cfg().rpk_path.bind()) {}

ss::future<> service::start() {
    if (ss::this_shard_id() != service_shard) {
        co_return;
    }

    try {
        lg.trace("Creating {}", _debug_bundle_dir);
        co_await ss::recursive_touch_directory(_debug_bundle_dir.native());
    } catch (const std::exception& e) {
        throw std::system_error(error_code::internal_error, e.what());
    }

    if (!co_await ss::file_exists(_rpk_path_binding().native())) {
        lg.error(
          "Current specified RPK location {} does not exist!  Debug "
          "bundle creation is not available until this is fixed!",
          _rpk_path_binding().native());
    }

    lg.debug("Service started");
}

ss::future<> service::stop() {
    if (ss::this_shard_id() != service_shard) {
        co_return;
    }
    lg.debug("Service stopping");
}

ss::future<result<void>> service::initiate_rpk_debug_bundle_collection(
  job_id_t job_id, debug_bundle_parameters params) {
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(
          service_shard,
          [job_id, params = std::move(params)](service& s) mutable {
              return s.initiate_rpk_debug_bundle_collection(
                job_id, std::move(params));
          });
    }
    co_return error_info(error_code::internal_error, "Not yet implemented");
}

ss::future<result<void>> service::cancel_rpk_debug_bundle(job_id_t job_id) {
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(
          service_shard,
          [job_id](service& s) { return s.cancel_rpk_debug_bundle(job_id); });
    }
    co_return error_info(error_code::debug_bundle_process_never_started);
}

ss::future<result<debug_bundle_status_data>>
service::rpk_debug_bundle_status() {
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(service_shard, [](service& s) {
            return s.rpk_debug_bundle_status();
        });
    }
    co_return error_info(error_code::debug_bundle_process_never_started);
}

ss::future<result<std::filesystem::path>> service::rpk_debug_bundle_path() {
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(
          service_shard, [](service& s) { return s.rpk_debug_bundle_path(); });
    }
    co_return error_info(error_code::debug_bundle_process_never_started);
}

ss::future<result<void>> service::delete_rpk_debug_bundle() {
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(service_shard, [](service& s) {
            return s.delete_rpk_debug_bundle();
        });
    }
    co_return error_info(error_code::debug_bundle_process_never_started);
}

} // namespace debug_bundle
