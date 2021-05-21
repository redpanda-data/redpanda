// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/server.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

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

} // namespace pandaproxy::schema_registry
