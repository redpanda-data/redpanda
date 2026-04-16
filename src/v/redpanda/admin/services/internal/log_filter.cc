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

proto::admin::log_filter_state
to_wire_state(vlog::detail::callsite_base::state s) {
    using cs_state = vlog::detail::callsite_base::state;
    switch (s) {
    case cs_state::default_:
        return proto::admin::log_filter_state::inherited;
    case cs_state::force_on:
        return proto::admin::log_filter_state::force_on;
    case cs_state::force_off:
        return proto::admin::log_filter_state::force_off;
    case cs_state::uninit:
        // resolved_state() never returns uninit post-slow_init.
        return proto::admin::log_filter_state::inherited;
    }
    return proto::admin::log_filter_state::inherited;
}

vlog::detail::callsite_base::state
to_internal_state(proto::admin::log_filter_state s) {
    using cs_state = vlog::detail::callsite_base::state;
    switch (s) {
    case proto::admin::log_filter_state::inherited:
        return cs_state::default_;
    case proto::admin::log_filter_state::force_on:
        return cs_state::force_on;
    case proto::admin::log_filter_state::force_off:
        return cs_state::force_off;
    case proto::admin::log_filter_state::unspecified:
        throw serde::pb::rpc::invalid_argument_exception(
          "log_filter_rule.state must not be LOG_FILTER_STATE_UNSPECIFIED");
    }
    throw serde::pb::rpc::invalid_argument_exception(
      fmt::format(
        "log_filter_rule.state carries unknown enum value {}",
        static_cast<int>(s)));
}

proto::admin::log_filter_rule to_wire_rule(const vlog::rule& r) {
    proto::admin::log_filter_rule wire;
    if (r.file) {
        wire.set_file(ss::sstring{*r.file});
    }
    if (r.line) {
        const auto [lo, hi] = *r.line;
        chunked_vector<uint32_t> line_vec;
        line_vec.push_back(lo);
        if (lo != hi) {
            line_vec.push_back(hi);
        }
        wire.set_line(std::move(line_vec));
    }
    if (r.contains) {
        wire.set_contains(ss::sstring{*r.contains});
    }
    wire.set_state(to_wire_state(r.state));
    return wire;
}

vlog::rule to_internal_rule(proto::admin::log_filter_rule& wire) {
    vlog::rule r;
    if (wire.has_file()) {
        r.file = std::move(wire.get_file());
    }
    const auto& line = wire.get_line();
    switch (line.size()) {
    case 0:
        break;
    case 1:
        r.line = std::pair{line[0], line[0]};
        break;
    case 2:
        if (line[0] > line[1]) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "log_filter_rule.line range [{}, {}] is inverted",
                line[0],
                line[1]));
        }
        r.line = std::pair{line[0], line[1]};
        break;
    default:
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format(
            "log_filter_rule.line accepts 0, 1, or 2 entries; got {}",
            line.size()));
    }
    if (wire.has_contains()) {
        r.contains = std::move(wire.get_contains());
    }
    r.state = to_internal_state(wire.get_state());
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

seastar::future<proto::admin::get_log_filter_response>
log_filter_service_impl::get_log_filter(
  serde::pb::rpc::context, proto::admin::get_log_filter_request) {
    auto rules = vlog::get_rules();
    chunked_vector<proto::admin::log_filter_rule> out;
    out.reserve(rules.size());
    for (const auto& r : rules) {
        out.push_back(to_wire_rule(r));
    }
    proto::admin::get_log_filter_response resp;
    resp.set_rules(std::move(out));
    co_return std::move(resp);
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
      [&out, &file_filter](vlog::detail::callsite_base& cs) {
          if (file_filter && fnmatch(file_filter->c_str(), cs.file(), 0) != 0) {
              return;
          }
          proto::admin::log_callsite_info info;
          info.set_file(ss::sstring{cs.file()});
          info.set_line(cs.line());
          info.set_fmt(
            cs.fmt() != nullptr ? ss::sstring{cs.fmt()} : ss::sstring{});
          info.set_state(to_wire_state(cs.resolved_state()));
          out.push_back(std::move(info));
      });

    proto::admin::list_log_callsites_response resp;
    resp.set_callsites(std::move(out));
    co_return std::move(resp);
}

} // namespace admin
