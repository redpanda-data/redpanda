// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "random/generators.h"
#include "utils/to_string.h"

#include <seastar/testing/thread_test_case.hh>
using namespace kafka; // NOLINT

#define roundtrip_test(value, type_cast, read_method)                          \
    {                                                                          \
        BOOST_TEST_CHECKPOINT("write<" #type_cast "> :: read<" #read_method    \
                              ">");                                            \
        auto val = value;                                                      \
        auto out = iobuf();                                                    \
        kafka::response_writer w(out);                                         \
        w.write((type_cast)val);                                               \
        kafka::request_reader r(std::move(out));                               \
        BOOST_REQUIRE_EQUAL(val, (r.*read_method)());                          \
    }

SEASTAR_THREAD_TEST_CASE(write_and_read_value_test) {
    roundtrip_test(static_cast<int8_t>(64), int8_t, &request_reader::read_int8);
    roundtrip_test(
      static_cast<int16_t>(32000), int16_t, &request_reader::read_int16);
    roundtrip_test(
      static_cast<int32_t>(64000000), int32_t, &request_reader::read_int32);
    roundtrip_test(
      static_cast<int64_t>(45564000000), int64_t, &request_reader::read_int64);
    roundtrip_test(true, bool, &request_reader::read_bool);
    roundtrip_test(false, bool, &request_reader::read_bool);
    roundtrip_test(
      ss::sstring{"test_string"}, ss::sstring, &request_reader::read_string);
    roundtrip_test(
      ss::sstring("test_string"),
      std::optional<ss::sstring>,
      &request_reader::read_nullable_string);
    roundtrip_test(
      static_cast<std::optional<ss::sstring>>(std::nullopt),
      std::optional<ss::sstring>,
      &request_reader::read_nullable_string);
    roundtrip_test(
      model::topic{"test_topic"}, ss::sstring, &request_reader::read_string);
}
