#include "random/generators.h"
#include "redpanda/kafka/requests/request_reader.h"
#include "redpanda/kafka/requests/response_writer.h"
#include "redpanda/kafka/requests/tests/utils.h"
#include "utils/to_string.h"

#include <seastar/testing/thread_test_case.hh>
using namespace kafka; // NOLINT

template<typename TestType, typename ReadFunc>
CONCEPT(requires requires(ReadFunc f, request_reader& reader, TestType t){
  {(reader.*f)() == t}->bool})
void roundtrip_test(TestType val, ReadFunc&& f) {
    bytes_ostream out;
    kafka::response_writer w(out);
    w.write(val);
    auto fb = copy_to_fragbuf(out);
    kafka::request_reader r(fb.get_istream());
    BOOST_REQUIRE_EQUAL(val, (r.*f)());
}

SEASTAR_THREAD_TEST_CASE(write_and_read_value_test) {
    roundtrip_test(static_cast<int8_t>(64), &request_reader::read_int8);
    roundtrip_test(static_cast<int16_t>(32000), &request_reader::read_int16);
    roundtrip_test(static_cast<int32_t>(64000000), &request_reader::read_int32);
    roundtrip_test(
      static_cast<int64_t>(45564000000), &request_reader::read_int64);
    roundtrip_test(
      static_cast<uint32_t>(64000000), &request_reader::read_uint32);
    roundtrip_test(true, &request_reader::read_bool);
    roundtrip_test(false, &request_reader::read_bool);
    roundtrip_test(sstring{"test_string"}, &request_reader::read_string);
    roundtrip_test(
      std::string_view{"test_string"}, &request_reader::read_string_view);
    roundtrip_test(
      std::make_optional<sstring>("test_string"),
      &request_reader::read_nullable_string);
    roundtrip_test(
      static_cast<std::optional<sstring>>(std::nullopt),
      &request_reader::read_nullable_string);
    roundtrip_test(
      std::make_optional<std::string_view>("test_string"),
      &request_reader::read_nullable_string_view);
    roundtrip_test(
      static_cast<std::optional<std::string_view>>(std::nullopt),
      &request_reader::read_nullable_string_view);
    roundtrip_test(
      bytes_view(random_generators::get_bytes()),
      &request_reader::read_bytes_view);
    roundtrip_test(
      bytes_view(random_generators::get_bytes()), &request_reader::read_bytes);
    roundtrip_test(
      std::make_optional<bytes>(random_generators::get_bytes()),
      &request_reader::read_nullable_bytes);
    roundtrip_test(model::topic{"test_topic"}, &request_reader::read_string);
}
