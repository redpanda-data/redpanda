#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/sleep.hh>
using namespace std::chrono_literals; // NOLINT

struct test_str_ctx final : public rpc::streaming_context {
    virtual ~test_str_ctx() noexcept = default;
    virtual ss::future<ss::semaphore_units<>> reserve_memory(size_t) final {
        return get_units(s, 1);
    }
    virtual const rpc::header& get_header() const final { return hdr; }

    virtual void signal_body_parse() final {}

    rpc::header hdr;
    ss::semaphore s{1};
};

struct test_fixture {
    bool triggered = false;

    rpc::internal::response_handler
    create_handler(rpc::clock_type::time_point timeout) {
        rpc::internal::response_handler r;
        r.with_timeout(timeout, [this]() mutable { triggered = true; });
        return r;
    }
};

FIXTURE_TEST(fail_with_timeout, test_fixture) {
    auto rh = create_handler(rpc::clock_type::now() + 100ms);

    // wait for timeout
    ss::sleep(1s).get();

    BOOST_REQUIRE_THROW(rh.get_future().get0(), rpc::request_timeout_exception);
    BOOST_REQUIRE_EQUAL(triggered, true);
};

FIXTURE_TEST(fail_other_error_with_timeout, test_fixture) {
    auto rh = create_handler(rpc::clock_type::now() + 100s);

    rh.set_exception(std::runtime_error("test"));

    BOOST_REQUIRE_THROW(rh.get_future().get0(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(triggered, false);
};

FIXTURE_TEST(success_case_with_timout, test_fixture) {
    auto rh = create_handler(rpc::clock_type::now() + 100s);
    rh.set_value(std::make_unique<test_str_ctx>());

    auto v = rh.get_future().get0();

    BOOST_REQUIRE_EQUAL(triggered, false);
};
