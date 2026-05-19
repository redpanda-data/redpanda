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

#include "redpanda/admin/services/datalake/datalake.h"

#include "cloud_storage/configuration.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "datalake/coordinator/catalog_factory.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/types.h"
#include "datalake/credential_manager.h"
#include "iceberg/catalog.h"
#include "iceberg/catalog_errors.h"

#include <seastar/coroutine/as_future.hh>

#include <yaml-cpp/yaml.h>

namespace {

proto::admin::data_file to_proto(const datalake::coordinator::data_file& df) {
    proto::admin::data_file pb_file;
    pb_file.set_remote_path(ss::sstring(df.remote_path));
    pb_file.set_row_count(df.row_count);
    pb_file.set_file_size_bytes(df.file_size_bytes);
    pb_file.set_table_schema_id(df.table_schema_id);
    pb_file.set_partition_spec_id(df.partition_spec_id);
    chunked_vector<iobuf> pb_partition_key;
    for (const auto& key : df.partition_key) {
        if (key.has_value()) {
            auto key_buf = bytes_to_iobuf(key.value());
            pb_partition_key.emplace_back(std::move(key_buf));
        } else {
            pb_partition_key.emplace_back(iobuf{});
        }
    }
    pb_file.set_partition_key(std::move(pb_partition_key));
    return pb_file;
}

proto::admin::translated_offset_range
to_proto(const datalake::coordinator::translated_offset_range& range) {
    proto::admin::translated_offset_range pb_range;
    pb_range.set_start_offset(range.start_offset());
    pb_range.set_last_offset(range.last_offset());
    chunked_vector<proto::admin::data_file> pb_data_files;
    for (const auto& file : range.files) {
        pb_data_files.emplace_back(to_proto(file));
    }
    pb_range.set_data_files(std::move(pb_data_files));
    chunked_vector<proto::admin::data_file> pb_dlq_files;
    for (const auto& dlq_file : range.dlq_files) {
        pb_dlq_files.emplace_back(to_proto(dlq_file));
    }
    pb_range.set_dlq_files(std::move(pb_dlq_files));
    pb_range.set_kafka_processed_bytes(range.kafka_bytes_processed);
    return pb_range;
}

proto::admin::pending_entry
to_proto(const datalake::coordinator::pending_entry& entry) {
    proto::admin::pending_entry pb_entry;
    pb_entry.set_data(to_proto(entry.data));
    pb_entry.set_added_pending_at(entry.added_pending_at());
    return pb_entry;
}

proto::admin::partition_state
to_proto(const datalake::coordinator::partition_state& state) {
    proto::admin::partition_state pb_state;
    chunked_vector<proto::admin::pending_entry> pb_entries;
    for (const auto& entry : state.pending_entries) {
        pb_entries.emplace_back(to_proto(entry));
    }
    pb_state.set_pending_entries(std::move(pb_entries));
    if (state.last_committed.has_value()) {
        pb_state.set_last_committed(state.last_committed.value()());
    }
    return pb_state;
}

proto::admin::lifecycle_state
to_proto(datalake::coordinator::topic_state::lifecycle_state_t state) {
    switch (state) {
    case datalake::coordinator::topic_state::lifecycle_state_t::live:
        return proto::admin::lifecycle_state::live;
    case datalake::coordinator::topic_state::lifecycle_state_t::closed:
        return proto::admin::lifecycle_state::closed;
    case datalake::coordinator::topic_state::lifecycle_state_t::purged:
        return proto::admin::lifecycle_state::purged;
    }
    return proto::admin::lifecycle_state::unspecified;
}

proto::admin::topic_state
to_proto(const datalake::coordinator::topic_state& state) {
    proto::admin::topic_state pb_state;
    pb_state.set_revision(state.revision());
    chunked_hash_map<int32_t, proto::admin::partition_state>
      pb_partition_states;
    for (const auto& [pid, pstate] : state.pid_to_pending_files) {
        pb_partition_states.emplace(pid(), to_proto(pstate));
    }
    pb_state.set_partition_states(std::move(pb_partition_states));
    pb_state.set_lifecycle_state(to_proto(state.lifecycle_state));
    pb_state.set_total_kafka_processed_bytes(state.total_kafka_bytes_processed);
    pb_state.set_last_committed_snapshot_id(
      state.last_committed_snapshot_id.value_or(iceberg::invalid_snapshot_id));
    return pb_state;
}

} // anonymous namespace

namespace admin {

datalake_service_impl::datalake_service_impl(
  admin::proxy::client proxy_client,
  ss::sharded<datalake::coordinator::frontend>* coordinator_fe,
  ss::sharded<cloud_io::remote>* cloud_io_remote)
  : _proxy_client(std::move(proxy_client))
  , _coordinator_fe(coordinator_fe)
  , _cloud_io_remote(cloud_io_remote) {}

ss::future<proto::admin::get_coordinator_state_response>
datalake_service_impl::get_coordinator_state(
  serde::pb::rpc::context, proto::admin::get_coordinator_state_request req) {
    if (!_coordinator_fe->local_is_initialized()) {
        throw serde::pb::rpc::unavailable_exception(
          "Datalake coordinator frontend not initialized");
    }

    // Group topics by coordinator partition.
    chunked_hash_map<model::partition_id, chunked_vector<model::topic>>
      topics_filter_by_partition;
    if (req.get_topics_filter().empty()) {
        auto partition_count_opt
          = _coordinator_fe->local().coordinator_partition_count();
        if (!partition_count_opt.has_value()) {
            throw serde::pb::rpc::unavailable_exception(
              fmt::format(
                "Datalake coordinator couldn't get coordinator partition "
                "count"));
        }
        // There is no topics filter, make a request for every partition.
        for (auto p = 0; p < partition_count_opt.value(); ++p) {
            topics_filter_by_partition.emplace(
              model::partition_id{p}, chunked_vector<model::topic>());
        }
    } else {
        for (const auto& topic_name : req.get_topics_filter()) {
            model::topic topic{topic_name};
            auto partition_opt = _coordinator_fe->local().coordinator_partition(
              topic);
            if (!partition_opt.has_value()) {
                throw serde::pb::rpc::unavailable_exception(
                  fmt::format(
                    "Datalake coordinator couldn't get coordinator partition "
                    "for {}",
                    topic));
            }
            topics_filter_by_partition[partition_opt.value()].emplace_back(
              std::move(topic));
        }
    }

    // Send out the RPCs.
    chunked_hash_map<model::topic, datalake::coordinator::topic_state>
      topic_states;
    for (auto& [partition_id, topics_filter] : topics_filter_by_partition) {
        datalake::coordinator::get_topic_state_request fe_req{
          partition_id, std::move(topics_filter)};
        auto fe_res = co_await _coordinator_fe->local().get_topic_state(
          std::move(fe_req));

        if (fe_res.errc != datalake::coordinator::errc::ok) {
            throw serde::pb::rpc::internal_exception(
              fmt::format(
                "Datalake coordinator error for partition {}: {}",
                partition_id,
                fe_res.errc));
        }

        for (auto& [topic, state] : fe_res.topic_states) {
            topic_states.insert({topic, std::move(state)});
        }
    }

    // Convert to protobuf response.
    proto::admin::get_coordinator_state_response response;
    chunked_hash_map<ss::sstring, proto::admin::topic_state> pb_topic_states;
    for (const auto& [topic, state] : topic_states) {
        pb_topic_states.emplace(topic(), to_proto(state));
    }
    proto::admin::coordinator_state state;
    state.set_topic_states(std::move(pb_topic_states));
    response.set_state(std::move(state));

    co_return response;
}

ss::future<proto::admin::coordinator_reset_topic_state_response>
datalake_service_impl::coordinator_reset_topic_state(
  serde::pb::rpc::context,
  proto::admin::coordinator_reset_topic_state_request req) {
    if (!_coordinator_fe->local_is_initialized()) {
        throw serde::pb::rpc::unavailable_exception(
          "Datalake coordinator frontend not initialized");
    }

    model::topic topic{req.get_topic_name()};
    auto partition_opt = _coordinator_fe->local().coordinator_partition(topic);
    if (!partition_opt.has_value()) {
        throw serde::pb::rpc::unavailable_exception(
          fmt::format(
            "Datalake coordinator couldn't get coordinator partition "
            "for {}",
            topic));
    }

    model::revision_id topic_revision{req.get_revision()};

    chunked_hash_map<
      model::partition_id,
      datalake::coordinator::partition_state_override>
      partition_overrides;
    for (const auto& [pid, po] : req.get_partition_overrides()) {
        datalake::coordinator::partition_state_override o;
        if (po.has_last_committed()) {
            o.last_committed = kafka::offset{po.get_last_committed()};
        }
        partition_overrides.emplace(model::partition_id{pid}, std::move(o));
    }

    auto fe_res = co_await _coordinator_fe->local().reset_topic_state(
      datalake::coordinator::reset_topic_state_request(
        partition_opt.value(),
        topic,
        topic_revision,
        req.get_reset_all_partitions(),
        std::move(partition_overrides)));
    if (fe_res.errc != datalake::coordinator::errc::ok) {
        throw serde::pb::rpc::internal_exception(
          fmt::format(
            "Datalake coordinator error for partition {}: {}",
            partition_opt.value(),
            fe_res.errc));
    }

    co_return proto::admin::coordinator_reset_topic_state_response{};
}

ss::future<proto::admin::describe_catalog_response>
datalake_service_impl::describe_catalog(
  serde::pb::rpc::context, proto::admin::describe_catalog_request) {
    if (!_coordinator_fe->local_is_initialized()) {
        throw serde::pb::rpc::unavailable_exception(
          "Datalake coordinator frontend not initialized");
    }

    auto res = co_await _coordinator_fe->local().describe_catalog();
    if (res.has_error()) {
        const auto& err = res.error();
        throw serde::pb::rpc::internal_exception(
          fmt::format(
            "Catalog describe failed ({}): {}", err.errc, err.message));
    }

    co_return proto::admin::describe_catalog_response{};
}

ss::future<proto::admin::test_catalog_response>
datalake_service_impl::test_catalog(
  serde::pb::rpc::context, proto::admin::test_catalog_request req) {
    const auto& overrides = req.get_property_overrides();

    if (overrides.empty()) {
        // No overrides: behave like describe_catalog but return structured
        // errors instead of throwing.
        if (!_coordinator_fe->local_is_initialized()) {
            throw serde::pb::rpc::unavailable_exception(
              "Datalake coordinator frontend not initialized");
        }

        auto res = co_await _coordinator_fe->local().describe_catalog();
        proto::admin::test_catalog_response response;
        if (res.has_error()) {
            const auto& err = res.error();
            response.set_catalog_describe_error_code(
              ss::sstring(iceberg::to_string_view(err.errc)));
            response.set_catalog_describe_error_message(
              ss::sstring(err.message));
        }
        co_return response;
    }

    // Override path: construct an ephemeral catalog from a snapshot of the
    // running config with the requested properties applied on top.
    // Heap-allocate the configuration via config::make_config so it stays
    // off the coroutine frame (it's ~130 KiB).
    auto overlay_ptr = config::make_config();
    auto& overlay = *overlay_ptr;
    config::shard_local_cfg().for_each(
      [&overlay](const config::base_property& p) {
          auto& tmp_p = overlay.get(p.name());
          tmp_p = p;
      });

    for (const auto& [name, value] : overrides) {
        config::base_property* prop = nullptr;
        try {
            prop = &overlay.get(name);
        } catch (...) {
            proto::admin::test_catalog_response response;
            response.set_catalog_describe_error_code("invalid_request");
            response.set_catalog_describe_error_message(
              fmt::format("unknown cluster property '{}'", name));
            co_return response;
        }

        try {
            prop->set_value(YAML::Load(value));
        } catch (const std::exception& e) {
            proto::admin::test_catalog_response response;
            response.set_catalog_describe_error_code("invalid_request");
            // Avoid echoing back the value if the property is marked secret.
            if (prop->is_secret()) {
                response.set_catalog_describe_error_message(
                  fmt::format("property '{}': invalid value", name));
            } else {
                response.set_catalog_describe_error_message(
                  fmt::format("property '{}': {}", name, e.what()));
            }
            co_return response;
        }
    }

    auto bucket_cfg = cloud_storage::configuration::get_bucket_config()();
    if (!bucket_cfg.has_value()) {
        proto::admin::test_catalog_response response;
        response.set_catalog_describe_error_code("invalid_request");
        response.set_catalog_describe_error_message(
          "cloud_storage_bucket is not configured");
        co_return response;
    }
    auto bucket = cloud_storage_clients::bucket_name{bucket_cfg.value()};

    // Build an ephemeral credential_manager from the overlay so the factory
    // resolves credentials against the proposed config, not the running
    // cluster's. Lives for the duration of this request only.
    datalake::credential_manager ephemeral_cred_mgr(overlay);
    co_await ephemeral_cred_mgr.start();

    // For auth modes that obtain credentials via the bg refresh op
    // (aws_sigv4 / gcp), validate that credentials are actually
    // reachable before we attempt the catalog round-trip. This surfaces
    // credential-source failures (unreachable IMDS, mis-configured
    // aws_credentials_source) as a distinct error rather than as a
    // generic catalog probe timeout. For other auth modes this branch
    // is skipped and we rely on the catalog probe as the validator.
    if (
      datalake::credential_manager::needs_background_credential_refresh(
        overlay)) {
        auto cred_result
          = co_await ephemeral_cred_mgr.ensure_initial_credentials_available();
        if (cred_result.has_error()) {
            proto::admin::test_catalog_response response;
            response.set_catalog_describe_error_code("credential_unavailable");
            response.set_catalog_describe_error_message(
              fmt::format(
                "could not obtain credentials from the configured source "
                "(check iceberg_rest_catalog_aws_credentials_source / "
                "iceberg_rest_catalog_credentials_host or the fallback "
                "cloud_storage_credentials_source): {}",
                cred_result.error().message()));
            co_await ephemeral_cred_mgr.stop();
            co_return response;
        }
    }

    auto factory = datalake::coordinator::get_catalog_factory(
      overlay,
      _cloud_io_remote->local(),
      bucket,
      ss::metrics::label_instance{"test_catalog", "request"},
      ephemeral_cred_mgr);

    ss::abort_source as;

    // Capture the probe result as a resolved future so we can both await
    // catalog->stop() unconditionally and translate any thrown exception
    // into a structured TestCatalog response.
    auto catalog = co_await factory->create_catalog(as);
    auto describe_fut = co_await ss::coroutine::as_future(
      catalog->describe_catalog());
    co_await catalog->stop();
    co_await ephemeral_cred_mgr.stop();

    proto::admin::test_catalog_response response;
    if (describe_fut.failed()) {
        try {
            std::rethrow_exception(describe_fut.get_exception());
        } catch (const std::exception& e) {
            response.set_catalog_describe_error_code("probe_exception");
            response.set_catalog_describe_error_message(e.what());
        } catch (...) {
            response.set_catalog_describe_error_code("probe_exception");
            response.set_catalog_describe_error_message("unknown exception");
        }
        co_return response;
    }
    auto probe_result = describe_fut.get();
    if (probe_result.has_error()) {
        const auto& err = probe_result.error();
        response.set_catalog_describe_error_code(
          ss::sstring(iceberg::to_string_view(err.errc)));
        response.set_catalog_describe_error_message(ss::sstring(err.message));
    }
    co_return response;
}

} // namespace admin
