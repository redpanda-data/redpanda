/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "json/json.h"
#include "kafka/errors.h"
#include "kafka/requests/fetch_request.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/requests/error_reply.h"
#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace pandaproxy::json {

template<>
class rjson_serialize_impl<kafka::fetch_response::partition> {
public:
    explicit rjson_serialize_impl(serialization_format fmt)
      : _fmt(fmt) {}

    void operator()(
      rapidjson::Writer<rapidjson::StringBuffer>& w,
      kafka::fetch_response::partition&& v) {
        vassert(
          v.responses.size() == 1, "expected a single partition_response");

        auto r = std::move(v.responses[0]);
        if (r.has_error()) {
            error_body e{
              .error_code = ss::httpd::reply::status_type::not_found,
              .message{ss::sstring{kafka::error_code_to_str(r.error)}}};
            rjson_serialize(w, e);
            return;
        }

        w.StartArray();
        if (r.record_set && !r.record_set->empty()) {
            kafka::kafka_batch_adapter adapter;
            adapter.adapt(std::move(*r.record_set));
            adapter.batch->for_each_record(
              [this, &w, &v, &r, &adapter](model::record record) {
                  w.StartObject();
                  w.Key("topic");
                  ::json::rjson_serialize(w, v.name);
                  w.Key("key");
                  rjson_serialize_fmt(_fmt)(w, record.release_key());
                  w.Key("value");
                  rjson_serialize_fmt(_fmt)(w, record.release_value());
                  w.Key("partition");
                  ::json::rjson_serialize(w, r.id);
                  w.Key("offset");
                  ::json::rjson_serialize(
                    w, adapter.batch->base_offset()() + record.offset_delta());
                  w.EndObject();
              });
        }
        w.EndArray();
    }

private:
    serialization_format _fmt;
};

} // namespace pandaproxy::json
