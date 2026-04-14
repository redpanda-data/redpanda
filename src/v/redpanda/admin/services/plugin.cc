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

#include "redpanda/admin/services/plugin.h"

#include "cluster/errc.h"
#include "model/compression.h"
#include "model/transform.h"
#include "proto/redpanda/core/admin/v2/plugin.proto.h"
#include "proto/redpanda/core/common/v1/compression.proto.h"
#include "serde/protobuf/rpc.h"
#include "transform/api.h"
#include "utils/uuid.h"
#include "wasm/errc.h"

#include <seastar/core/coroutine.hh>

#include <fmt/core.h>
#include <limits>

using namespace std::chrono_literals;

namespace admin {

namespace {

// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger pluginlog{"admin_api_server/plugin_service"};

void check_transforms_enabled(
  const ss::sharded<transform::service>* transform_service) {
    if (!transform_service->local_is_initialized()) {
        throw serde::pb::rpc::failed_precondition_exception(
          "data transforms disabled - use `rpk cluster config set "
          "data_transforms_enabled true` to enable");
    }
}

void throw_on_transform_error(
  std::string_view context, const std::error_code& ec) {
    if (!ec) {
        return;
    }
    if (ec.category() == cluster::error_category()) {
        switch (static_cast<cluster::errc>(ec.value())) {
        case cluster::errc::transform_does_not_exist:
            throw serde::pb::rpc::not_found_exception(
              fmt::format("{}: transform not found", context));
        case cluster::errc::transform_invalid_update:
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format("{}: invalid update", context));
        case cluster::errc::feature_disabled:
            throw serde::pb::rpc::failed_precondition_exception(
              fmt::format("{}: feature disabled", context));
        default:
            break;
        }
    } else if (ec.category() == wasm::error_category()) {
        std::string_view msg;
        switch (wasm::errc(ec.value())) {
        case wasm::errc::invalid_module_missing_abi:
            msg = "Invalid WebAssembly - the binary is missing required "
                  "transform functions. Check the broker support for the "
                  "version of the Data Transforms SDK being used.";
            break;
        case wasm::errc::invalid_module_unsupported_sr:
            msg = "Invalid WebAssembly - the binary is using an unsupported "
                  "Schema Registry client. Does the broker support this "
                  "version of the Data Transforms Schema Registry SDK?";
            break;
        case wasm::errc::invalid_module_missing_wasi:
            msg = "invalid WebAssembly - missing required WASI functions";
            break;
        case wasm::errc::invalid_module:
            msg = "invalid WebAssembly module";
            break;
        default:
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format("{}: invalid binary: {}", context, ec.message()));
        }
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format("{}: invalid binary: {}", context, msg));
    }
    throw serde::pb::rpc::internal_exception(
      fmt::format("{}: {}", context, ec.message()));
}

proto::admin::transform_partition_state
to_proto_state(model::transform_report::processor::state s) {
    using ms = model::transform_report::processor::state;
    using ps = proto::admin::transform_partition_state;
    switch (s) {
    case ms::running:
        return ps::running;
    case ms::inactive:
        return ps::inactive;
    case ms::errored:
        return ps::errored;
    case ms::unknown:
        return ps::unknown;
    }
    return ps::unknown;
}

proto::common::compression_mode to_proto_compression(model::compression c) {
    using mc = model::compression;
    using pc = proto::common::compression_mode;
    switch (c) {
    case mc::none:
        return pc::none;
    case mc::gzip:
        return pc::gzip;
    case mc::snappy:
        return pc::snappy;
    case mc::lz4:
        return pc::lz4;
    case mc::zstd:
        return pc::zstd;
    case mc::count:
    case mc::producer:
        return pc::none;
    }
    return pc::none;
}

model::compression from_proto_compression(proto::common::compression_mode c) {
    using mc = model::compression;
    using pc = proto::common::compression_mode;
    switch (c) {
    case pc::gzip:
        return mc::gzip;
    case pc::snappy:
        return mc::snappy;
    case pc::lz4:
        return mc::lz4;
    case pc::zstd:
        return mc::zstd;
    case pc::none:
    case pc::unspecified:
        return mc::none;
    }
    return mc::none;
}

proto::admin::transform build_transform(const model::transform_report& report) {
    proto::admin::transform t;
    t.set_name(ss::sstring(report.metadata.name()));
    t.set_input_topic(ss::sstring(report.metadata.input_topic.tp()));
    for (const auto& ot : report.metadata.output_topics) {
        t.get_output_topics().push_back(ss::sstring(ot.tp()));
    }
    for (const auto& [k, v] : report.metadata.environment) {
        proto::admin::transform_environment_variable ev;
        ev.set_key(ss::sstring(k));
        ev.set_value(ss::sstring(v));
        t.get_environment().push_back(std::move(ev));
    }
    t.set_compression(to_proto_compression(report.metadata.compression_mode));
    t.set_is_paused(bool(report.metadata.paused));
    for (const auto& [_, proc] : report.processors) {
        proto::admin::transform_partition_status s;
        s.set_node_id(proc.node());
        s.set_partition(proc.id());
        s.set_status(to_proto_state(proc.status));
        s.set_lag(proc.lag);
        t.get_status().push_back(std::move(s));
    }
    return t;
}

} // namespace

plugin_service_impl::plugin_service_impl(
  admin::proxy::client proxy_client,
  ss::sharded<transform::service>* transform_service)
  : _proxy_client(std::move(proxy_client))
  , _transform_service(transform_service) {}

// --- transforms resource ---

ss::future<proto::admin::create_transform_response>
plugin_service_impl::create_transform(
  serde::pb::rpc::context, proto::admin::create_transform_request req) {
    check_transforms_enabled(_transform_service);

    if (req.get_name().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "name must not be empty");
    }
    if (req.get_input_topic().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "input_topic must not be empty");
    }
    if (req.get_output_topics().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "at least one output_topic is required");
    }
    if (req.get_binary_id().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "binary_id must not be empty");
    }

    // binary_id has the format "<uuid>:<source_ptr_offset>" as returned by
    // CreateBinary.
    uuid_t binary_uuid;
    model::offset binary_source_ptr;
    try {
        const auto& bid = req.get_binary_id();
        auto sep = bid.find(':');
        if (sep == ss::sstring::npos) {
            throw std::invalid_argument("missing separator");
        }
        binary_uuid = uuid_t::from_string(bid.substr(0, sep));
        binary_source_ptr = model::offset{
          std::stoll(std::string(bid.substr(sep + 1)))};
    } catch (...) {
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format("invalid binary_id: {}", req.get_binary_id()));
    }

    model::transform_metadata meta;
    meta.name = model::transform_name(req.get_name());
    meta.input_topic = model::topic_namespace(
      model::kafka_namespace, model::topic(req.get_input_topic()));
    for (const auto& ot : req.get_output_topics()) {
        meta.output_topics.emplace_back(
          model::kafka_namespace, model::topic(ot));
    }
    for (const auto& ev : req.get_environment()) {
        meta.environment.insert_or_assign(ev.get_key(), ev.get_value());
    }
    meta.compression_mode = from_proto_compression(req.get_compression());
    meta.uuid = binary_uuid;
    meta.source_ptr = binary_source_ptr;

    if (const auto& co = req.get_consume_offset(); co.has_position()) {
        if (co.has_from_start()) {
            if (co.get_from_start() < 0) {
                throw serde::pb::rpc::invalid_argument_exception(
                  "consume_offset.from_start must be >= 0");
            }
            meta.offset_options.position = model::transform_from_start{
              kafka::offset_delta{co.get_from_start()}};
        } else if (co.has_from_end()) {
            if (co.get_from_end() < 0) {
                throw serde::pb::rpc::invalid_argument_exception(
                  "consume_offset.from_end must be >= 0");
            }
            meta.offset_options.position = model::transform_from_end{
              kafka::offset_delta{co.get_from_end()}};
        } else if (co.has_timestamp_ms()) {
            const auto ts_ms = co.get_timestamp_ms();
            if (ts_ms < 0) {
                throw serde::pb::rpc::invalid_argument_exception(
                  "consume_offset.timestamp_ms must be >= 0");
            }
            // model::timestamp::max() (INT64_MAX) is reserved as a sentinel.
            if (ts_ms == std::numeric_limits<int64_t>::max()) {
                throw serde::pb::rpc::invalid_argument_exception(
                  "consume_offset.timestamp_ms is out of range");
            }
            meta.offset_options.position = model::timestamp{ts_ms};
        }
    }

    vlog(pluginlog.info, "create_transform: {}", req.get_name());
    auto ec = co_await _transform_service->local().deploy_plugin(
      std::move(meta));
    throw_on_transform_error("create_transform", ec);
    co_return proto::admin::create_transform_response{};
}

ss::future<proto::admin::get_transform_response>
plugin_service_impl::get_transform(
  serde::pb::rpc::context, proto::admin::get_transform_request req) {
    check_transforms_enabled(_transform_service);

    if (req.get_name().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "name must not be empty");
    }

    auto report = co_await _transform_service->local().list_transforms();
    for (const auto& [_, entry] : report.transforms) {
        if (entry.metadata.name() == req.get_name()) {
            proto::admin::get_transform_response resp;
            resp.set_transform(build_transform(entry));
            co_return resp;
        }
    }

    throw serde::pb::rpc::not_found_exception(
      fmt::format("transform '{}' not found", req.get_name()));
}

ss::future<proto::admin::list_transforms_response>
plugin_service_impl::list_transforms(
  serde::pb::rpc::context, proto::admin::list_transforms_request) {
    check_transforms_enabled(_transform_service);

    auto report = co_await _transform_service->local().list_transforms();
    proto::admin::list_transforms_response resp;
    for (const auto& [_, entry] : report.transforms) {
        resp.get_transforms().push_back(build_transform(entry));
    }
    co_return resp;
}

ss::future<proto::admin::update_transform_response>
plugin_service_impl::update_transform(
  serde::pb::rpc::context, proto::admin::update_transform_request req) {
    check_transforms_enabled(_transform_service);

    if (req.get_name().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "name must not be empty");
    }

    model::transform_metadata_patch patch;
    if (req.get_has_environment()) {
        patch.env.emplace();
        for (const auto& ev : req.get_environment()) {
            patch.env->insert_or_assign(ev.get_key(), ev.get_value());
        }
    }
    if (req.get_has_is_paused()) {
        patch.paused.emplace(model::is_transform_paused(req.get_is_paused()));
    }
    if (req.get_has_compression()) {
        patch.compression_mode.emplace(
          from_proto_compression(req.get_compression()));
    }

    if (patch.empty()) {
        vlog(
          pluginlog.debug,
          "update_transform: empty patch for {}",
          req.get_name());
        co_return proto::admin::update_transform_response{};
    }

    vlog(pluginlog.info, "update_transform: {}", req.get_name());
    auto ec = co_await _transform_service->local().patch_transform_metadata(
      model::transform_name(req.get_name()), std::move(patch));
    throw_on_transform_error("update_transform", ec);
    co_return proto::admin::update_transform_response{};
}

ss::future<proto::admin::delete_transform_response>
plugin_service_impl::delete_transform(
  serde::pb::rpc::context, proto::admin::delete_transform_request req) {
    check_transforms_enabled(_transform_service);

    if (req.get_name().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "name must not be empty");
    }

    vlog(pluginlog.info, "delete_transform: {}", req.get_name());
    auto ec = co_await _transform_service->local().delete_transform(
      model::transform_name(req.get_name()));
    throw_on_transform_error("delete_transform", ec);
    co_return proto::admin::delete_transform_response{};
}

// --- binaries resource ---

ss::future<proto::admin::create_binary_response>
plugin_service_impl::create_binary(
  serde::pb::rpc::context, proto::admin::create_binary_request req) {
    check_transforms_enabled(_transform_service);

    if (req.get_binary().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "binary must not be empty");
    }

    const auto binary_size = req.get_binary().size_bytes();
    auto wasm_binary = model::wasm_binary_iobuf(
      std::make_unique<iobuf>(std::move(req.get_binary())));

    vlog(pluginlog.info, "create_binary: {} bytes", binary_size);
    auto result = co_await _transform_service->local().store_wasm_binary(
      std::move(wasm_binary));
    if (result.has_error()) {
        throw_on_transform_error("create_binary", result.error());
    }

    const auto& stored = result.value();
    proto::admin::create_binary_response resp;
    resp.set_binary_id(
      fmt::format("{}:{}", ss::sstring(stored.uuid), stored.source_ptr()));
    co_return resp;
}

ss::future<proto::admin::list_binaries_response>
plugin_service_impl::list_binaries(
  serde::pb::rpc::context, proto::admin::list_binaries_request) {
    check_transforms_enabled(_transform_service);

    auto report = co_await _transform_service->local().list_transforms();

    // Build a map from binary_id string → list of transform names.
    // The binary_id format is "<uuid>:<source_ptr_offset>".
    absl::flat_hash_map<ss::sstring, std::vector<ss::sstring>> by_binary;
    for (const auto& [_, entry] : report.transforms) {
        auto id = fmt::format(
          "{}:{}", ss::sstring(entry.metadata.uuid), entry.metadata.source_ptr);
        by_binary[id].emplace_back(ss::sstring(entry.metadata.name()));
    }

    proto::admin::list_binaries_response resp;
    for (auto& [id, names] : by_binary) {
        proto::admin::transform_binary b;
        b.set_binary_id(ss::sstring(id));
        for (auto& n : names) {
            b.get_referenced_by().push_back(n);
        }
        resp.get_binaries().push_back(std::move(b));
    }
    co_return resp;
}

ss::future<proto::admin::delete_binary_response>
plugin_service_impl::delete_binary(
  serde::pb::rpc::context, proto::admin::delete_binary_request req) {
    check_transforms_enabled(_transform_service);

    if (req.get_binary_id().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "binary_id must not be empty");
    }

    // binary_id has the format "<uuid>:<source_ptr_offset>" as returned by
    // CreateBinary and ListBinaries.
    uuid_t binary_uuid;
    try {
        const auto& bid = req.get_binary_id();
        auto sep = bid.find(':');
        if (sep == ss::sstring::npos) {
            throw std::invalid_argument("missing separator");
        }
        binary_uuid = uuid_t::from_string(bid.substr(0, sep));
    } catch (...) {
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format("invalid binary_id: {}", req.get_binary_id()));
    }

    // Reject the delete if any active transform still references this binary.
    auto report = co_await _transform_service->local().list_transforms();
    for (const auto& [_, entry] : report.transforms) {
        if (entry.metadata.uuid == binary_uuid) {
            throw serde::pb::rpc::failed_precondition_exception(fmt::format(
              "binary '{}' is still referenced by transform '{}'",
              req.get_binary_id(),
              entry.metadata.name()));
        }
    }

    vlog(pluginlog.info, "delete_binary: {}", req.get_binary_id());
    auto ec = co_await _transform_service->local().delete_wasm_binary(
      binary_uuid);
    throw_on_transform_error("delete_binary", ec);
    co_return proto::admin::delete_binary_response{};
}

} // namespace admin
