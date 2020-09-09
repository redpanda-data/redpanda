#include "pandaproxy/json/iobuf.h"

#include "bytes/iobuf_parser.h"

#include <seastar/testing/thread_test_case.hh>

namespace pp = pandaproxy;
namespace ppj = pp::json;

SEASTAR_THREAD_TEST_CASE(test_iobuf_parse_binary) {
    auto [res, buf] = ppj::rjson_parse_impl<iobuf>{
      pp::serialization_format::binary_v2}("cGFuZGFwcm94eQ==");
    BOOST_REQUIRE(res);
    BOOST_REQUIRE(!!buf);
    iobuf_parser p(std::move(*buf));
    BOOST_TEST(p.read_string(p.bytes_left()) == "pandaproxy");
}

SEASTAR_THREAD_TEST_CASE(test_iobuf_parse_binary_error) {
    auto [res, buf] = ppj::rjson_parse_impl<iobuf>{
      pp::serialization_format::binary_v2}("cGFuZGFwcm94eQ=?");
    BOOST_REQUIRE(!res);
    BOOST_REQUIRE(!buf);
}
