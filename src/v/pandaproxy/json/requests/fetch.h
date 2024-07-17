/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "json/writer.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "storage/parser_utils.h"

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

    template<typename Buffer>
    bool operator()(::json::iobuf_writer<Buffer>& w, model::record record) {
        auto offset = _base_offset() + record.offset_delta();

        w.StartObject();
        w.Key("topic");
        ::json::rjson_serialize(w, _tpv.topic);
        w.Key("key");
        if (!rjson_serialize_fmt(_fmt)(w, record.release_key())) {
            throw serialize_error(
              make_error_code(error_code::unable_to_serialize),
              fmt::format(
                "Unable to serialize key of record at offset {} in "
                "topic:partition {}:{}",
                offset,
                _tpv.topic(),
                _tpv.partition()));
        }
        w.Key("value");
        if (!rjson_serialize_fmt(_fmt)(w, record.release_value())) {
            throw serialize_error(
              make_error_code(error_code::unable_to_serialize),
              fmt::format(
                "Unable to serialize value of record at offset {} in "
                "topic:partition {}:{}",
                offset,
                _tpv.topic(),
                _tpv.partition()));
        }
        w.Key("partition");
        ::json::rjson_serialize(w, _tpv.partition);
        w.Key("offset");
        ::json::rjson_serialize(w, offset);
        w.EndObject();

        return true;
    }

    model::topic_partition_view tpv() const noexcept { return _tpv; }

    model::offset base_offset() const noexcept { return _base_offset; }

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

    template<typename Buffer>
    bool
    operator()(::json::iobuf_writer<Buffer>& w, kafka::fetch_response&& res) {
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

                auto batch = std::move(*adapter.batch);

                if (batch.compressed()) {
                    batch = storage::internal::maybe_decompress_batch_sync(
                      batch);
                }

                batch.for_each_record([&rjs, &w](model::record record) {
                    auto offset = record.offset_delta() + rjs.base_offset()();
                    if (!rjs(w, std::move(record))) {
                        throw serialize_error(
                          make_error_code(error_code::unable_to_serialize),
                          fmt::format(
                            "Unable to serialize record at offset {} in "
                            "topic:partition {}:{}",
                            offset,
                            rjs.tpv().topic(),
                            rjs.tpv().partition()));
                    }
                });
            }
        }
        w.EndArray();

        return true;
    }

private:
    serialization_format _fmt;
};

} // namespace pandaproxy::json
