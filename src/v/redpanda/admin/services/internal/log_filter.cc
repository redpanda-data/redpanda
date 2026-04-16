/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/internal/log_filter.h"

#include "base/vlog.h"
#include "base/vlog_callsite.h"
#include "base/vlog_filter.h"

#include <seastar/core/coroutine.hh>

#include <fnmatch.h>

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger log{"admin_api_server/log_filter_service"};

vlog::rule to_internal_rule(proto::admin::log_filter_rule& wire) {
    vlog::rule r;
    if (wire.has_file()) {
        r.file = std::move(wire.get_file());
    }
    if (wire.has_line()) {
        r.line = wire.get_line();
    }
    if (wire.has_contains()) {
        r.contains = std::move(wire.get_contains());
    }
    r.enabled = wire.get_enabled();
    return r;
}

} // namespace

seastar::future<proto::admin::set_log_filter_response>
log_filter_service_impl::set_log_filter(
  serde::pb::rpc::context, proto::admin::set_log_filter_request req) {
    std::vector<vlog::rule> rules;
    auto& wire_rules = req.get_rules();
    rules.reserve(wire_rules.size());
    for (auto& r : wire_rules) {
        rules.push_back(to_internal_rule(r));
    }
    vlog(log.info, "Applying {} vlog callsite filter rule(s)", rules.size());
    vlog::apply_rules(std::move(rules));
    co_return proto::admin::set_log_filter_response{};
}

seastar::future<proto::admin::reset_log_filter_response>
log_filter_service_impl::reset_log_filter(
  serde::pb::rpc::context, proto::admin::reset_log_filter_request) {
    vlog(log.info, "Reset all vlog callsite filters");
    vlog::reset_rules();
    co_return proto::admin::reset_log_filter_response{};
}

seastar::future<proto::admin::list_log_callsites_response>
log_filter_service_impl::list_log_callsites(
  serde::pb::rpc::context, proto::admin::list_log_callsites_request req) {
    std::optional<std::string> file_filter;
    if (req.has_file_filter()) {
        file_filter = std::move(req.get_file_filter());
    }

    chunked_vector<proto::admin::log_callsite_info> out;
    vlog::for_each_callsite(
      [&out, &file_filter](const vlog::detail::callsite& cs) {
          if (file_filter && fnmatch(file_filter->c_str(), cs.file(), 0) != 0) {
              return;
          }
          proto::admin::log_callsite_info info;
          info.set_file(ss::sstring{cs.file()});
          info.set_line(cs.line());
          info.set_fmt(
            cs.fmt() != nullptr ? ss::sstring{cs.fmt()} : ss::sstring{});
          info.set_enabled(cs.enabled());
          out.push_back(std::move(info));
      });

    proto::admin::list_log_callsites_response resp;
    resp.set_callsites(std::move(out));
    co_return std::move(resp);
}

} // namespace admin
