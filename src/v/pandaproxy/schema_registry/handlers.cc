// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "model/fundamental.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/reply.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/requests/get_schemas_ids_id.h"
#include "pandaproxy/schema_registry/requests/get_subject_versions_version.h"
#include "pandaproxy/schema_registry/requests/post_subject_versions.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/server.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/std-coroutine.hh>

namespace ppj = pandaproxy::json;

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

ss::future<server::reply_t>
get_schemas_types(server::request_t rq, server::reply_t rp) {
    auto res_fmt = parse::accept_header(
      *rq.req,
      {ppj::serialization_format::schema_registry_v1_json,
       ppj::serialization_format::schema_registry_json,
       ppj::serialization_format::none});
    rq.req.reset();

    static const std::vector<std::string_view> schemas_types{"AVRO"};
    auto json_rslt = ppj::rjson_serialize(schemas_types);
    rp.rep->write_body("json", json_rslt);
    rp.mime_type = res_fmt;
    return ss::make_ready_future<server::reply_t>(std::move(rp));
}

ss::future<server::reply_t>
get_schemas_ids_id(server::request_t rq, server::reply_t rp) {
    auto res_fmt = parse::accept_header(
      *rq.req,
      {ppj::serialization_format::schema_registry_v1_json,
       ppj::serialization_format::schema_registry_json,
       ppj::serialization_format::none});

    auto id = parse::request_param<schema_id>(*rq.req, "id");
    rq.req.reset();
    auto schema = rq.service().schema_store().get_schema(id);
    if (schema.has_error()) {
        rp.rep = pandaproxy::errored_body(
          make_error_code(error_code::schema_id_not_found), "Schema not found");
        co_return rp;
    }
    auto json_rslt = ppj::rjson_serialize(
      get_schemas_ids_id_response{.definition = schema.value().definition});
    rp.rep->write_body("json", json_rslt);
    rp.mime_type = res_fmt;
    co_return rp;
}

ss::future<server::reply_t>
get_subjects(server::request_t rq, server::reply_t rp) {
    parse::accept_header(
      *rq.req,
      {ppj::serialization_format::schema_registry_v1_json,
       ppj::serialization_format::schema_registry_json,
       ppj::serialization_format::none});

    rq.req.reset();

    auto subjects = rq.service().schema_store().get_subjects();
    auto json_rslt{json::rjson_serialize(subjects)};
    rp.rep->write_body("json", json_rslt);
    rp.mime_type = json::serialization_format::schema_registry_v1_json;

    co_return rp;
}

ss::future<server::reply_t>
get_subject_versions(server::request_t rq, server::reply_t rp) {
    parse::accept_header(
      *rq.req,
      {ppj::serialization_format::schema_registry_v1_json,
       ppj::serialization_format::schema_registry_json,
       ppj::serialization_format::none});

    auto sub = parse::request_param<subject>(*rq.req, "subject");
    rq.req.reset();

    auto versions = rq.service().schema_store().get_versions(sub);
    if (versions.has_error()) {
        rp.rep = pandaproxy::errored_body(
          versions.error(), versions.error().message());
        co_return rp;
    }

    auto json_rslt{json::rjson_serialize(versions.value())};
    rp.rep->write_body("json", json_rslt);
    rp.mime_type = json::serialization_format::schema_registry_v1_json;

    co_return rp;
}

ss::future<server::reply_t>
post_subject_versions(server::request_t rq, server::reply_t rp) {
    parse::content_type_header(
      *rq.req,
      {ppj::serialization_format::schema_registry_v1_json,
       ppj::serialization_format::schema_registry_json});
    parse::accept_header(
      *rq.req,
      {ppj::serialization_format::schema_registry_v1_json,
       ppj::serialization_format::schema_registry_json,
       ppj::serialization_format::none});

    auto req = post_subject_versions_request{
      .sub = parse::request_param<subject>(*rq.req, "subject"),
      .payload = ppj::rjson_parse(
        rq.req->content.data(), post_subject_versions_request_handler{})};

    rq.req.reset();

    auto ins_res = rq.service().schema_store().insert(
      req.sub, req.payload.schema, req.payload.type);

    if (ins_res.inserted) {
        storage::record_batch_builder rb{
          raft::data_batch_type, model::offset{0}};
        rb.add_raw_kv(
          make_schema_key(req.sub, ins_res.version),
          make_schema_value(
            req.sub, ins_res.version, ins_res.id, req.payload.schema));

        auto res = co_await rq.service().client().local().produce_record_batch(
          model::topic_partition{
            model::topic{"_schemas"}, model::partition_id{0}},
          std::move(rb).build());

        // TODO(Ben): Check the error reporting here
        if (res.error_code != kafka::error_code::none) {
            throw kafka::exception(res.error_code, *res.error_message);
        }
    }

    auto json_rslt{
      json::rjson_serialize(post_subject_versions_response{.id{ins_res.id}})};
    rp.rep->write_body("json", json_rslt);
    rp.mime_type = json::serialization_format::schema_registry_v1_json;

    co_return rp;
}

ss::future<ctx_server<service>::reply_t> get_subject_versions_version(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse::accept_header(
      *rq.req,
      {ppj::serialization_format::schema_registry_v1_json,
       ppj::serialization_format::schema_registry_json,
       ppj::serialization_format::none});

    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");

    auto version = invalid_schema_version;
    if (ver == "latest") {
        auto versions = rq.service().schema_store().get_versions(sub);
        if (versions.has_error()) {
            rp.rep = pandaproxy::errored_body(
              versions.error(), versions.error().message());
            co_return rp;
        }
        if (versions.value().empty()) {
            auto code = make_error_code(error_code::subject_version_not_found);
            rp.rep = pandaproxy::errored_body(code, code.message());
            co_return rp;
        }
        version = versions.value().back();
    } else {
        auto res = parse::from_chars<schema_version>{}(ver);
        if (res.has_error()) {
            rp.rep = pandaproxy::errored_body(
              res.error(), res.error().message());
            co_return rp;
        }
        version = res.value();
    }

    rq.req.reset();

    auto get_res = rq.service().schema_store().get_subject_schema(sub, version);
    if (get_res.has_error()) {
        rp.rep = pandaproxy::errored_body(
          get_res.error(), get_res.error().message());
        co_return rp;
    }

    auto json_rslt{json::rjson_serialize(post_subject_versions_version_response{
      .sub = sub,
      .version = version,
      .definition = std::move(get_res).value().definition})};
    rp.rep->write_body("json", json_rslt);
    rp.mime_type = json::serialization_format::schema_registry_v1_json;

    co_return rp;
}

} // namespace pandaproxy::schema_registry
