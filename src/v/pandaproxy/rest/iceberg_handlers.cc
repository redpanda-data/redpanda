// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/iceberg_handlers.h"

#include "bytes/iostream.h"
#include "cluster/topic_table.h"
#include "config/rest_authn_endpoint.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/coordinator/state.h"
#include "datalake/table_id_provider.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/httpd.h"
#include "proto/redpanda/core/rest/iceberg.proto.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/request_auth.h"

namespace pandaproxy::rest {

namespace {

ss::future<proto::pandaproxy::get_translation_state_request>
parse_request(proxy::server::request_t& rq) {
    auto req_fmt = parse::content_type_header(
      *rq.req,
      {json::serialization_format::application_json,
       json::serialization_format::application_proto});

    auto data = co_await read_iobuf_exactly(
      *rq.req->content_stream, rq.context().max_memory);

    if (req_fmt == json::serialization_format::application_json) {
        co_return co_await proto::pandaproxy::get_translation_state_request::
          from_json(std::move(data));
    } else {
        co_return co_await proto::pandaproxy::get_translation_state_request::
          from_proto(std::move(data));
    }
}

ss::future<iobuf> serialize_response(
  proto::pandaproxy::get_translation_state_response resp,
  json::serialization_format fmt) {
    if (fmt == json::serialization_format::application_json) {
        co_return co_await resp.to_json();
    }
    co_return co_await resp.to_proto();
}

proto::pandaproxy::translation_status
get_translation_status(const cluster::topic_configuration& cfg) {
    if (cfg.properties.iceberg_mode == model::iceberg_mode::disabled) {
        return proto::pandaproxy::translation_status::disabled;
    }
    return proto::pandaproxy::translation_status::enabled;
}
} // namespace

ss::future<proxy::server::reply_t>
get_translation_state(proxy::server::request_t rq, proxy::server::reply_t rp) {
    auto req = co_await parse_request(rq);

    if (req.get_topics_filter().empty()) {
        vlog(
          plog.warn, "get_translation_state: topics_filter must not be empty");
        rp.rep->set_status(ss::http::reply::status_type::bad_request);
        co_return std::move(rp);
    }

    vlog(
      plog.trace,
      "get_translation_state for topics: {}",
      fmt::join(req.get_topics_filter(), ","));
    auto& frontend = rq.service().dl_frontend();

    if (!frontend.local_is_initialized()) {
        vlog(
          plog.warn,
          "datalake frontend is not initialized, Redpanda iceberg integration "
          "is disabled");
        rp.rep->set_status(ss::http::reply::status_type::bad_request);
        co_return std::move(rp);
    }

    chunked_vector<model::topic> topics_filter;
    topics_filter.reserve(req.get_topics_filter().size());
    for (const auto& topic_name : req.get_topics_filter()) {
        topics_filter.emplace_back(topic_name);
    }
    chunked_hash_map<model::topic, datalake::coordinator::topic_state>
      topic_states;
    try {
        auto fe_res = co_await frontend.local().get_topic_state(
          std::move(topics_filter));
        if (fe_res.errc != datalake::coordinator::errc::ok) {
            vlog(plog.warn, "error getting topics state - {}", fe_res.errc);
            rp.rep->set_status(
              ss::http::reply::status_type::internal_server_error);
            co_return std::move(rp);
        }
        topic_states = std::move(fe_res.topic_states);
    } catch (const std::exception& e) {
        vlog(
          plog.warn,
          "exception thrown while getting topic state from datalake "
          "coordinator - {}",
          e);
        rp.rep->set_status(ss::http::reply::status_type::internal_server_error);
        co_return std::move(rp);
    }

    // Filter out topics the user is not authorized to describe.
    if (rq.authn_method != config::rest_authn_method::none) {
        auto auth_result = rq.context().authenticator.authenticate(*rq.req);
        auth_result.pass();
        auto principal = security::acl_principal{
          security::principal_type::user, rq.user.name};
        auto host = security::acl_host{rq.req->get_client_address().addr()};
        auto& groups = auth_result.get_groups();
        std::erase_if(topic_states, [&](const auto& entry) {
            auto res = rq.service().authorizer().authorized(
              entry.first,
              security::acl_operation::describe,
              principal,
              host,
              security::superuser_required::no,
              groups);
            return !res.is_authorized();
        });
    }

    proto::pandaproxy::get_translation_state_response resp;
    chunked_hash_map<ss::sstring, proto::pandaproxy::topic_state>
      pb_topic_states;
    auto& topic_table = rq.service().topic_table();
    for (const auto& [topic, state] : topic_states) {
        proto::pandaproxy::topic_state pb_state;
        auto tp_md = topic_table.get_topic_metadata_ref(
          model::topic_namespace_view(model::kafka_namespace, topic));
        if (!tp_md) {
            vlog(
              plog.warn,
              "error getting topic translation state - topic {} not found in "
              "topic table",
              topic);
            continue;
        }
        auto current_topic_revision = tp_md->get().get_revision();
        if (current_topic_revision != state.revision) {
            vlog(
              plog.warn,
              "topic metadata revision mismatch for topic {} - topic table has "
              "revision {}, but translation state has revision {}",
              topic,
              current_topic_revision,
              state.revision);
            continue;
        }
        pb_state.set_translation_status(
          get_translation_status(tp_md->get().get_configuration()));

        if (state.last_committed_snapshot_id.has_value()) {
            pb_state.set_last_committed_snapshot_id(
              static_cast<uint64_t>(
                state.last_committed_snapshot_id.value()()));
        }

        auto table_id = datalake::table_id_provider::table_id(topic);
        pb_state.set_table_name(std::move(table_id.table));
        pb_state.set_namespace_name(std::move(table_id.ns));
        pb_state.set_dlq_table_name(
          datalake::table_id_provider::dlq_table_id(topic).table);

        chunked_hash_map<int32_t, proto::pandaproxy::partition_state>
          pb_partitions;
        for (const auto& [pid, pstate] : state.pid_to_pending_files) {
            proto::pandaproxy::partition_state pb_pstate;
            if (pstate.last_committed.has_value()) {
                pb_pstate.set_last_catalog_committed_offset(
                  pstate.last_committed.value()());
            }
            pb_partitions.emplace(pid(), std::move(pb_pstate));
        }
        pb_state.set_partition_states(std::move(pb_partitions));
        pb_topic_states.emplace(topic(), std::move(pb_state));
    }
    resp.set_topic_states(std::move(pb_topic_states));

    // Serialize and return.
    auto resp_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::application_json,
       json::serialization_format::application_proto});

    auto serialized = co_await serialize_response(std::move(resp), resp_fmt);
    rp.mime_type = resp_fmt;
    rp.rep->write_body(
      "bin",
      [serialized = std::move(serialized)](
        ss::output_stream<char>& writer) mutable {
          return write_iobuf_to_output_stream(serialized.share(), writer);
      });

    vlog(
      plog.trace,
      "get_translation_state: returning {} topic states",
      topic_states.size());
    co_return std::move(rp);
}

} // namespace pandaproxy::rest
