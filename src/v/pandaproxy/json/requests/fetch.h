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
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::json {

template<>
class rjson_serialize_impl<model::record> {
public:
    explicit rjson_serialize_impl(
      serialization_format fmt,
      model::topic_partition_view tpv,
      model::offset base_offset)
      : _fmt(fmt)
      , _tpv(tpv)
      , _base_offset(base_offset) {}

    void
    operator()(::json::Writer<::json::StringBuffer>& w, model::record record) {
        w.StartObject();
        w.Key("topic");
        ::json::rjson_serialize(w, _tpv.topic);
        w.Key("key");
        rjson_serialize_fmt(_fmt)(w, record.release_key());
        w.Key("value");
        rjson_serialize_fmt(_fmt)(w, record.release_value());
        w.Key("partition");
        ::json::rjson_serialize(w, _tpv.partition);
        w.Key("offset");
        ::json::rjson_serialize(w, _base_offset() + record.offset_delta());
        w.EndObject();
    }

private:
    serialization_format _fmt;
    model::topic_partition_view _tpv;
    model::offset _base_offset;
};

template<>
class rjson_serialize_impl<kafka::fetch_response> {
public:
    explicit rjson_serialize_impl(serialization_format fmt)
      : _fmt(fmt) {}

    void operator()(
      ::json::Writer<::json::StringBuffer>& w, kafka::fetch_response&& res) {
        // Eager check for errors
        for (auto& v : res) {
            if (v.partition_response->error_code != kafka::error_code::none) {
                throw serialize_error(v.partition_response->error_code);
            }
        }

        w.StartArray();
        for (auto& v : res) {
            auto r = std::move(*v.partition_response);
            model::topic_partition_view tpv(
              v.partition->name, r.partition_index);
            while (r.records && !r.records->empty()) {
                auto adapter = r.records->consume_batch();
                if (
                  !adapter.batch
                  || adapter.batch->header().attrs.is_control()) {
                    continue;
                }

                auto rjs = rjson_serialize_impl<model::record>(
                  _fmt, tpv, adapter.batch->base_offset());

                adapter.batch->for_each_record(
                  [&rjs, &w](model::record record) {
                      rjs(w, std::move(record));
                  });
            }
        }
        w.EndArray();
    }

private:
    serialization_format _fmt;
};

} // namespace pandaproxy::json
