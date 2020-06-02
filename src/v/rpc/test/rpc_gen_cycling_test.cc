#include "model/timeout_clock.h"
#include "random/generators.h"
#include "rpc/exceptions.h"
#include "rpc/test/rpc_gen_types.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "rpc/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <exception>

using namespace std::chrono_literals; // NOLINT

FIXTURE_TEST(rpcgen_integration, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    info("started");
    auto cli = rpc::client<cycling::team_movistar_client_protocol>(
      client_config());
    info("client connecting");
    cli.connect().get();
    auto dcli = ss::defer([&cli] { cli.stop().get(); });
    info("client calling method");
    auto ret = cli
                 .ibis_hakka(
                   cycling::san_francisco{66},
                   rpc::client_opts(rpc::no_timeout))
                 .get0();
    info("service stopping");

    BOOST_REQUIRE_EQUAL(ret.value().data.x, 66);
}

FIXTURE_TEST(rpcgen_tls_integration, rpc_integration_fixture) {
    auto creds_builder = config::tls_config(
                           true,
                           config::key_cert{"redpanda.key", "redpanda.crt"},
                           "root_certificate_authority.chain_cert",
                           false)
                           .get_credentials_builder()
                           .get0();
    configure_server(creds_builder);
    register_services();
    start_server();
    info("started");
    auto cli = rpc::client<cycling::team_movistar_client_protocol>(
      client_config(creds_builder));
    info("client connecting");
    cli.connect().get();
    auto dcli = ss::defer([&cli] { cli.stop().get(); });
    info("client calling method");
    auto ret = cli
                 .ibis_hakka(
                   cycling::san_francisco{66},
                   rpc::client_opts(rpc::no_timeout))
                 .get0();
    BOOST_REQUIRE_EQUAL(ret.value().data.x, 66);
}

FIXTURE_TEST(client_muxing, rpc_integration_fixture) {
    configure_server();
    // Two services @ single server
    register_services();
    start_server();

    rpc::
      client<cycling::team_movistar_client_protocol, echo::echo_client_protocol>
        client(client_config());
    client.connect().get();
    info("Calling movistar method");
    auto ret = client
                 .ibis_hakka(
                   cycling::san_francisco{66},
                   rpc::client_opts(rpc::no_timeout))
                 .get0();
    info("Calling echo method");
    auto echo_resp = client
                       .suffix_echo(
                         echo::echo_req{.str = "testing..."},
                         rpc::client_opts(rpc::no_timeout))
                       .get0();
    client.stop().get();

    BOOST_REQUIRE_EQUAL(echo_resp.value().data.str, "testing..._suffix");
}

FIXTURE_TEST(timeout_test, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::client<echo::echo_client_protocol> client(client_config());
    client.connect().get();
    info("Calling echo method");
    auto echo_resp = client.sleep_1s(
      echo::echo_req{.str = "testing..."},
      rpc::client_opts(rpc::clock_type::now() + 100ms));
    BOOST_REQUIRE_EQUAL(
      echo_resp.get0().error(), rpc::errc::client_request_timeout);
    client.stop().get();
}

FIXTURE_TEST(ordering_test, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::client<echo::echo_client_protocol> client(client_config());
    client.connect().get();
    std::vector<ss::future<>> futures;
    futures.reserve(10);
    ss::semaphore sem{1};
    for (uint64_t i = 0; i < 10; ++i) {
        ss::semaphore_units<> u = ss::get_units(sem, 1).get0();
        std::vector<ss::semaphore_units<>> units;
        units.push_back(std::move(u));
        auto units_ptr = ss::make_lw_shared(std::move(units));
        futures.push_back(
          client
            .counter(
              echo::cnt_req{i}, rpc::client_opts(rpc::no_timeout, units_ptr))
            .then(&rpc::get_ctx_data<echo::cnt_resp>)
            .then([i](result<echo::cnt_resp> r) {
                BOOST_REQUIRE_EQUAL(r.value().current, i);
                BOOST_REQUIRE_EQUAL(r.value().expected, i);
            }));
    }
    ss::when_all_succeed(futures.begin(), futures.end()).get0();
    client.stop().get();
}

FIXTURE_TEST(rpc_mixed_compression, rpc_integration_fixture) {
    const auto data = random_generators::gen_alphanum_string(1024);
    configure_server();
    // Two services @ single server
    register_services();
    start_server();

    using client_t = rpc::client<echo::echo_client_protocol>;
    client_t client(client_config());
    client.connect().get();
    BOOST_TEST_MESSAGE("Calling echo method no compression");
    auto echo_resp = client
                       .echo(
                         echo::echo_req{.str = data},
                         rpc::client_opts(rpc::no_timeout))
                       .get0();
    BOOST_REQUIRE_EQUAL(echo_resp.value().data.str, data);
    BOOST_TEST_MESSAGE("Calling echo method *WITH* compression");
    echo_resp = client
                  .echo(
                    echo::echo_req{.str = data},
                    rpc::client_opts(
                      rpc::no_timeout,
                      {},
                      rpc::compression_type::zstd,
                      0 /*min bytes compress*/))
                  .get0();
    BOOST_REQUIRE_EQUAL(echo_resp.value().data.str, data);

    // close resources
    client.stop().get();
}

FIXTURE_TEST(server_exception_test, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::client<echo::echo_client_protocol> client(client_config());
    client.connect().get();
    auto ret = client
                 .throw_exception(
                   echo::failure_type::exceptional_future,
                   rpc::client_opts(model::no_timeout))
                 .get0();

    BOOST_REQUIRE(ret.has_error());
    BOOST_REQUIRE_EQUAL(ret.error(), rpc::errc::service_error);
    client.stop().get();
}

FIXTURE_TEST(missing_method_test, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::transport t(client_config());
    t.connect().get();
    auto ret = t.send_typed<echo::failure_type, echo::throw_resp>(
                  echo::failure_type::exceptional_future,
                  1234,
                  rpc::client_opts(model::no_timeout))
                 .get0();
    BOOST_REQUIRE(ret.has_error());
    BOOST_REQUIRE_EQUAL(ret.error(), rpc::errc::method_not_found);
    t.stop().get();
}