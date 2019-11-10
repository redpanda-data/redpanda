#include "random/generators.h"
#include "redpanda/kafka/requests/request_reader.h"
#include "redpanda/kafka/requests/response_writer.h"
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
    roundtrip_test(
      static_cast<uint32_t>(64000000), uint32_t, &request_reader::read_uint32);
    roundtrip_test(true, bool, &request_reader::read_bool);
    roundtrip_test(false, bool, &request_reader::read_bool);
    roundtrip_test(
      sstring{"test_string"}, sstring, &request_reader::read_string);
    roundtrip_test(
      sstring{"test_string"},
      std::string_view,
      &request_reader::read_string_view);
    roundtrip_test(
      sstring("test_string"),
      std::optional<sstring>,
      &request_reader::read_nullable_string);
    roundtrip_test(
      static_cast<std::optional<sstring>>(std::nullopt),
      std::optional<sstring>,
      &request_reader::read_nullable_string);
    roundtrip_test(
      sstring("test_string"),
      std::optional<std::string_view>,
      &request_reader::read_nullable_string_view);
    roundtrip_test(
      static_cast<std::optional<std::string_view>>(std::nullopt),
      std::optional<std::string_view>,
      &request_reader::read_nullable_string_view);
    roundtrip_test(
      random_generators::get_bytes(),
      bytes_view,
      &request_reader::read_bytes_view);
    roundtrip_test(
      random_generators::get_bytes(), bytes_view, &request_reader::read_bytes);
    roundtrip_test(
      random_generators::get_bytes(),
      bytes_opt,
      &request_reader::read_nullable_bytes);
    roundtrip_test(
      model::topic{"test_topic"}, sstring, &request_reader::read_string);
}
