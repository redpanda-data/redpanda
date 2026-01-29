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

#include "redpanda/admin/services/internal/debug.h"

#include "base/vlog.h"
#include "finjector/stress_fiber.h"
#include "serde/protobuf/rpc.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger log{"admin_api_server/internal_debug_service"};
} // namespace

seastar::future<proto::admin::throw_structured_exception_response>
debug_service_impl::throw_structured_exception(
  serde::pb::rpc::context ctx,
  proto::admin::throw_structured_exception_request req) {
    auto target = model::node_id(req.get_node_id());
    if (target != model::node_id(-1) && target != _client.self_node_id()) {
        co_return co_await _client
          .make_client_for_node<proto::admin::debug_service_client>(target)
          .throw_structured_exception(ctx, std::move(req));
    }
    serde::pb::rpc::error_info info;
    info.reason = std::move(req.get_reason());
    if (info.reason.empty()) {
        info.reason = "UNKNOWN";
    }
    info.metadata = std::move(req.get_metadata());
    throw serde::pb::rpc::unknown_exception("test exception", std::move(info));
}

seastar::future<proto::start_stress_fiber_response>
debug_service_impl::start_stress_fiber(
  serde::pb::rpc::context, proto::start_stress_fiber_request req) {
    auto validate_min_max_settings =
      [](std::string_view name, int32_t min, int32_t max) -> bool {
        bool has_min = min != 0;
        bool has_max = max != 0;
        if (has_min != has_max) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Both min and max settings for {} must be set or unset "
                "together.",
                name));
        }
        if (!has_min) {
            return false;
        }
        if (min < 0) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format("Min setting for {} must be non-negative.", name));
        }
        if (max < 0) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format("Max setting for {} must be non-negative.", name));
        }
        if (min > max) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Min setting for {} must not exceed max setting.", name));
        }
        return true;
    };
    bool has_spins = validate_min_max_settings(
      "spins_per_scheduling_point",
      req.get_min_spins_per_scheduling_point(),
      req.get_max_spins_per_scheduling_point());
    bool has_ms = validate_min_max_settings(
      "ms_per_scheduling_point",
      req.get_min_ms_per_scheduling_point(),
      req.get_max_ms_per_scheduling_point());
    if (has_spins == has_ms) {
        throw serde::pb::rpc::invalid_argument_exception(
          "One of spins or ms settings for scheduling points must be "
          "set.");
    }
    stress_config cfg{
      .num_fibers = 1,
    };
    if (req.get_fiber_count() > 0) {
        cfg.num_fibers = req.get_fiber_count();
    }
    if (req.get_stack_depth() > 0) {
        cfg.stack_depth = req.get_stack_depth();
    }
    if (has_spins) {
        cfg.min_spins_per_scheduling_point
          = req.get_min_spins_per_scheduling_point();
        cfg.max_spins_per_scheduling_point
          = req.get_max_spins_per_scheduling_point();
    } else {
        cfg.min_ms_per_scheduling_point = req.get_min_ms_per_scheduling_point();
        cfg.max_ms_per_scheduling_point = req.get_max_ms_per_scheduling_point();
    }
    co_await _stress_fiber_manager.invoke_on_all(
      [cfg](stress_fiber_manager& m) {
          bool started = m.start(cfg);
          if (started) {
              vlog(log.info, "Started stress fiber with config: {}", cfg);
          } else {
              vlog(log.info, "Stress fiber manager is already running...");
          }
      });
    co_return proto::start_stress_fiber_response{};
}

seastar::future<proto::stop_stress_fiber_response>
debug_service_impl::stop_stress_fiber(
  serde::pb::rpc::context, proto::stop_stress_fiber_request) {
    co_await _stress_fiber_manager.invoke_on_all(
      [](stress_fiber_manager& m) { return m.stop(); });
    co_return proto::stop_stress_fiber_response{};
}

seastar::future<proto::admin::log_message_response>
debug_service_impl::log_message(
  serde::pb::rpc::context, proto::admin::log_message_request req) {
    auto msg = req.get_message();
    auto level = req.get_level();

    using enum proto::admin::log_level;

    ss::log_level ss_level = [=]() {
        switch (level) {
        case trace:
            return ss::log_level::trace;
        case debug:
            return ss::log_level::debug;
        case info:
            return ss::log_level::info;
        case warn:
            return ss::log_level::warn;
        case error:
            return ss::log_level::error;
        case unspecified:
        default:
            throw serde::pb::rpc::invalid_argument_exception(
              "Invalid log level specified");
        }
    }();

    log.log(ss_level, "{}", msg);
    co_return proto::admin::log_message_response{};
}

} // namespace admin
