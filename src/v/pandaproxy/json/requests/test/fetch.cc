// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/json/requests/fetch.h"

#include "json/stringbuffer.h"
#include "json/writer.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/wire.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "pandaproxy/json/requests/fetch.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "seastarx.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/interface.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <type_traits>

namespace ppj = pandaproxy::json;

std::optional<kafka::batch_reader>
make_record_set(model::offset offset, size_t count) {
    if (!count) {
        return std::nullopt;
    }
    iobuf record_set;
    auto writer{kafka::protocol::encoder(record_set)};
    kafka::protocol::writer_serialize_batch(writer, make_batch(offset, count));
    return kafka::batch_reader{std::move(record_set)};
}

auto make_fetch_response(
  std::vector<model::topic_partition> tps, model::offset offset, size_t count) {
    std::vector<kafka::fetch_response::partition> parts;
    for (const auto& tp : tps) {
        kafka::fetch_response::partition res{tp.topic};
        res.partitions.push_back(kafka::fetch_response::partition_response{
          .partition_index{tp.partition},
          .error_code = kafka::error_code::none,
          .high_watermark{model::offset{0}},
          .last_stable_offset{model::offset{1}},
          .log_start_offset{model::offset{0}},
          .aborted{},
          .records{make_record_set(offset, count)}});
        parts.push_back(std::move(res));
    }
    return kafka::fetch_response{
      .data = {
        .error_code = kafka::error_code::none,
        .topics = std::move(parts),
      }};
}

SEASTAR_THREAD_TEST_CASE(test_produce_fetch_empty) {
    model::topic_partition tp{model::topic{"topic"}, model::partition_id{1}};
    auto res = make_fetch_response({tp}, model::offset{0}, 0);
    auto fmt = ppj::serialization_format::binary_v2;

    ::json::StringBuffer str_buf;
    ::json::Writer<::json::StringBuffer> w(str_buf);
    ppj::rjson_serialize_fmt(fmt)(w, std::move(res));

    auto expected = R"([])";

    BOOST_REQUIRE_EQUAL(str_buf.GetString(), expected);
}

SEASTAR_THREAD_TEST_CASE(test_produce_fetch_one) {
    std::vector<model::topic_partition> tps = {
      {model::topic{"topic1"}, model::partition_id{1}},
      {model::topic{"topic2"}, model::partition_id{2}},
    };
    model::topic_partition tp{model::topic{"topic"}, model::partition_id{1}};
    auto res = make_fetch_response(tps, model::offset{42}, 1);
    auto fmt = ppj::serialization_format::binary_v2;

    ::json::StringBuffer str_buf;
    ::json::Writer<::json::StringBuffer> w(str_buf);
    ppj::rjson_serialize_fmt(fmt)(w, std::move(res));

    auto expected
      = R"([{"topic":"topic1","key":"KgAAAAAAAAA=","value":null,"partition":1,"offset":42},{"topic":"topic2","key":"KgAAAAAAAAA=","value":null,"partition":2,"offset":42}])";

    BOOST_REQUIRE_EQUAL(str_buf.GetString(), expected);
}
