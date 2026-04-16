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

#pragma once

#include "proto/redpanda/core/admin/internal/v1/log_filter.proto.h"

#include <seastar/core/future.hh>

namespace admin {

// Admin service that drives the per-callsite vlog filter on this node.
// State (the rule set and the per-callsite enabled flags) is process-
// global, so no sharded dependencies are needed — the service just
// translates between the wire types and the vlog::apply_rules /
// vlog::reset_rules / vlog::get_rules / vlog::for_each_callsite API in
// src/v/base.
class log_filter_service_impl : public proto::admin::log_filter_service {
public:
    log_filter_service_impl() = default;

    seastar::future<proto::admin::set_log_filter_response> set_log_filter(
      serde::pb::rpc::context, proto::admin::set_log_filter_request) override;

    seastar::future<proto::admin::reset_log_filter_response> reset_log_filter(
      serde::pb::rpc::context, proto::admin::reset_log_filter_request) override;

    seastar::future<proto::admin::get_log_filter_response> get_log_filter(
      serde::pb::rpc::context, proto::admin::get_log_filter_request) override;

    seastar::future<proto::admin::list_log_callsites_response>
      list_log_callsites(
        serde::pb::rpc::context,
        proto::admin::list_log_callsites_request) override;
};

} // namespace admin
