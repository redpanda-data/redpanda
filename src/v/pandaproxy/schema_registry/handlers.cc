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
post_subject_versions(server::request_t rq, server::reply_t rp) {
    // {"keytype":"SCHEMA","subject":"Kafka-key","version":1,"magic":1}
    // {"subject":"Kafka-key","version":1,"id":1,"schema":"\"string\"","deleted":false}
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

} // namespace pandaproxy::schema_registry
