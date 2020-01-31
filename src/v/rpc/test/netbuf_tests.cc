#include "rpc/netbuf.h"
#include "rpc/parse_utils.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// utils
#include "rpc/test/test_types.h"

#include <fmt/ostream.h>

namespace rpc {
/// \brief expects the inputstream to be prefixed by an rpc::header
template<typename T>
ss::future<T> parse_framed(ss::input_stream<char>& in) {
    return parse_header(in).then([&in](std::optional<header> o) {
        auto h = std::move(o.value());
        if (h.bitflags == 0) {
            return rpc::parse_type_wihout_compression<T>(in, h);
        }
        throw std::runtime_error(
          fmt::format("no compression supported. header: {}", h));
    });
}
} // namespace rpc

SEASTAR_THREAD_TEST_CASE(netbuf_pod) {
    auto n = rpc::netbuf();
    // type to serialize out
    pod src;
    src.x = 88;
    src.y = 88;
    src.z = 88;
    n.set_correlation_id(42);
    n.set_service_method_id(66);
    reflection::async_adl<pod>{}.to(n.buffer(), std::move(src)).get();
    // forces the computation of the header
    auto bufs = std::move(n).as_scattered().release().release();
    auto in = make_iobuf_input_stream(iobuf(std::move(bufs)));
    const pod dst = rpc::parse_framed<pod>(in).get0();
    BOOST_REQUIRE_EQUAL(src.x, dst.x);
    BOOST_REQUIRE_EQUAL(src.y, dst.y);
    BOOST_REQUIRE_EQUAL(src.z, dst.z);
}
