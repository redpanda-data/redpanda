// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/timeout_clock.h"
#include "random/generators.h"
#include "rpc/exceptions.h"
#include "rpc/test/rpc_gen_types.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "rpc/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <exception>
#include <filesystem>

using namespace std::chrono_literals; // NOLINT

FIXTURE_TEST(rpcgen_integration, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    info("started");
    auto cli = rpc::client<cycling::team_movistar_client_protocol>(
      client_config());
    info("client connecting");
    cli.connect(model::no_timeout).get();
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
                           false,
                           std::nullopt)
                           .get_credentials_builder()
                           .get0();
    configure_server(creds_builder);
    register_services();
    start_server();
    info("started");
    auto cli = rpc::client<cycling::team_movistar_client_protocol>(
      client_config(creds_builder));
    info("client connecting");
    cli.connect(model::no_timeout).get();
    auto dcli = ss::defer([&cli] { cli.stop().get(); });
    info("client calling method");
    auto ret = cli
                 .ibis_hakka(
                   cycling::san_francisco{66},
                   rpc::client_opts(rpc::no_timeout))
                 .get0();
    BOOST_REQUIRE_EQUAL(ret.value().data.x, 66);
}

class temporary_dir {
public:
    temporary_dir()
      : _tmp_path("/tmp/rpc-test-XXXX") {
        _dir = ss::make_tmp_dir(_tmp_path).get0();
    }

    temporary_dir(const temporary_dir&) = delete;
    temporary_dir& operator=(const temporary_dir&) = delete;
    temporary_dir(temporary_dir&&) = delete;
    temporary_dir& operator=(temporary_dir&&) = delete;

    ~temporary_dir() { _dir.remove().get0(); }
    /// Prefix file name with tmp path
    std::filesystem::path prefix(const char* name) const {
        auto res = _dir.get_path();
        res.append(name);
        return res;
    }
    /// Copy file from current work dir to tmp-dir
    std::filesystem::path copy_file(const char* src, const char* dst) {
        auto res = prefix(dst);
        if (std::filesystem::exists(res)) {
            ss::remove_file(res.native()).get0();
        }
        BOOST_REQUIRE(std::filesystem::copy_file(src, res));
        return res;
    }

private:
    std::filesystem::path _tmp_path;
    ss::tmp_dir _dir;
};

struct certificate_reload_ctx {
    std::unordered_set<ss::sstring> updated;
    ss::condition_variable cvar;
};

FIXTURE_TEST(rpcgen_reload_credentials_integration, rpc_integration_fixture) {
    // Server starts with bad credentials, files are updated on disk and then
    // client connects. Expected behavior is that client can connect without
    // issues. Condition variable is used to wait for credentials to reload.
    auto context = ss::make_lw_shared<certificate_reload_ctx>();
    temporary_dir tmp;
    // client credentials
    auto client_key = tmp.copy_file("redpanda.key", "client.key");
    auto client_crt = tmp.copy_file("redpanda.crt", "client.crt");
    auto client_ca = tmp.copy_file(
      "root_certificate_authority.chain_cert", "ca_client.pem");
    auto client_creds_builder = config::tls_config(
                                  true,
                                  config::key_cert{
                                    client_key.native(), client_crt.native()},
                                  client_ca.native(),
                                  true,
                                  std::nullopt)
                                  .get_credentials_builder()
                                  .get0();
    // server credentials
    auto server_key = tmp.copy_file("redpanda.other.key", "server.key");
    auto server_crt = tmp.copy_file("redpanda.other.crt", "server.crt");
    auto server_ca = tmp.copy_file(
      "root_certificate_authority.other.chain_cert", "ca_server.pem");
    auto server_creds_builder = config::tls_config(
                                  true,
                                  config::key_cert{
                                    server_key.native(), server_crt.native()},
                                  server_ca.native(),
                                  true,
                                  std::nullopt)
                                  .get_credentials_builder()
                                  .get0();

    configure_server(
      server_creds_builder,
      [context](
        const std::unordered_set<ss::sstring>& delta,
        const std::exception_ptr& err) {
          info("server credentials reload event");
          if (err) {
              try {
                  std::rethrow_exception(err);
              } catch (...) {
                  // The expection is expected to be triggered when the
                  // temporary files are deleted at the end of the test
                  info(
                    "tls reloadable credentials callback exception: {}",
                    std::current_exception());
              }
          } else {
              for (const auto& name : delta) {
                  info("server credentials reload {}", name);
                  context->updated.insert(name);
              }
              context->cvar.signal();
          }
      });

    register_services();
    start_server();
    info("server started");

    // fix client credentials and reconnect
    info("replacing files");
    tmp.copy_file("redpanda.key", "server.key");
    tmp.copy_file("redpanda.crt", "server.crt");
    tmp.copy_file("root_certificate_authority.chain_cert", "ca_server.pem");

    context->cvar.wait([context] { return context->updated.size() == 3; })
      .get();

    info("client connection attempt");
    auto cli = rpc::client<cycling::team_movistar_client_protocol>(
      client_config(client_creds_builder));
    cli.connect(model::no_timeout).get();
    auto okret = cli
                   .ibis_hakka(
                     cycling::san_francisco{66},
                     rpc::client_opts(rpc::no_timeout))
                   .get0();
    BOOST_REQUIRE_EQUAL(okret.value().data.x, 66);
    cli.stop().get0();
}

FIXTURE_TEST(client_muxing, rpc_integration_fixture) {
    configure_server();
    // Two services @ single server
    register_services();
    start_server();

    rpc::
      client<cycling::team_movistar_client_protocol, echo::echo_client_protocol>
        client(client_config());
    client.connect(model::no_timeout).get();
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
    client.connect(model::no_timeout).get();
    info("Calling echo method");
    auto echo_resp = client.sleep_1s(
      echo::echo_req{.str = "testing..."},
      rpc::client_opts(rpc::clock_type::now() + 100ms));
    BOOST_REQUIRE_EQUAL(
      echo_resp.get0().error(), rpc::errc::client_request_timeout);
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
    client.connect(model::no_timeout).get();
    BOOST_TEST_MESSAGE("Calling echo method no compression");
    auto echo_resp
      = client
          .echo(echo::echo_req{.str = data}, rpc::client_opts(rpc::no_timeout))
          .get0();
    BOOST_REQUIRE_EQUAL(echo_resp.value().data.str, data);
    BOOST_TEST_MESSAGE("Calling echo method *WITH* compression");
    echo_resp = client
                  .echo(
                    echo::echo_req{.str = data},
                    rpc::client_opts(
                      rpc::no_timeout,
                      rpc::compression_type::zstd,
                      0 /*min bytes compress*/))
                  .get0();
    BOOST_REQUIRE_EQUAL(echo_resp.value().data.str, data);

    // close resources
    client.stop().get();
}

FIXTURE_TEST(ordering_test, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::client<echo::echo_client_protocol> client(client_config());
    client.connect(model::no_timeout).get();
    std::vector<ss::future<>> futures;
    futures.reserve(10);
    for (uint64_t i = 0; i < 10; ++i) {
        futures.push_back(
          client.counter(echo::cnt_req{i}, rpc::client_opts(rpc::no_timeout))
            .then(&rpc::get_ctx_data<echo::cnt_resp>)
            .then([i](result<echo::cnt_resp> r) {
                BOOST_REQUIRE_EQUAL(r.value().current, i);
                BOOST_REQUIRE_EQUAL(r.value().expected, i);
            }));
    }
    ss::when_all_succeed(futures.begin(), futures.end()).get0();
    client.stop().get();
}

FIXTURE_TEST(server_exception_test, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::client<echo::echo_client_protocol> client(client_config());
    client.connect(model::no_timeout).get();
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
    t.connect(model::no_timeout).get();
    auto ret = t.send_typed<echo::failure_type, echo::throw_resp>(
                  echo::failure_type::exceptional_future,
                  1234,
                  rpc::client_opts(model::no_timeout))
                 .get0();
    BOOST_REQUIRE(ret.has_error());
    BOOST_REQUIRE_EQUAL(ret.error(), rpc::errc::method_not_found);
    t.stop().get();
}

FIXTURE_TEST(corrupted_header_at_client_test, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto client = echo::echo_client_protocol(t);
    auto stop_action = ss::defer([&t] { t.stop().get(); });
    BOOST_TEST_MESSAGE("Request with valid payload");
    auto echo_resp = client
                       .echo(
                         echo::echo_req{.str = "testing..."},
                         rpc::client_opts(rpc::no_timeout))
                       .get0();
    BOOST_REQUIRE_EQUAL(echo_resp.value().data.str, "testing...");

    // Send request but do not consume response
    BOOST_TEST_MESSAGE("Send request without consuming payload");
    rpc::netbuf nb;
    nb.set_compression(rpc::compression_type::none);
    nb.set_correlation_id(10);
    nb.set_service_method_id(960598415);
    reflection::adl<echo::echo_req>{}.to(
      nb.buffer(), echo::echo_req{.str = "testing..."});
    // will fail all the futures as server close the connection
    auto ret = t.send(
                  std::move(nb), rpc::client_opts(rpc::clock_type::now() + 1s))
                 .get0();
    ret.value()->signal_body_parse();

    // reconnect
    BOOST_TEST_MESSAGE("Another request with valid payload");
    t.connect(model::no_timeout).get0();
    for (int i = 0; i < 10; ++i) {
        auto echo_resp_new = client
                               .echo(
                                 echo::echo_req{.str = "testing..."},
                                 rpc::client_opts(rpc::clock_type::now() + 1s))
                               .get0();

        BOOST_REQUIRE_EQUAL(echo_resp_new.value().data.str, "testing...");
    }
}

FIXTURE_TEST(corrupted_data_at_server, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto client = echo::echo_client_protocol(t);
    auto stop_action = ss::defer([&t] { t.stop().get(); });
    BOOST_TEST_MESSAGE("Request with valid payload");
    auto echo_resp = client
                       .echo(
                         echo::echo_req{.str = "testing..."},
                         rpc::client_opts(rpc::no_timeout))
                       .get0();
    BOOST_REQUIRE_EQUAL(echo_resp.value().data.str, "testing...");

    rpc::netbuf nb;
    nb.set_compression(rpc::compression_type::none);
    nb.set_correlation_id(10);
    nb.set_service_method_id(960598415);
    auto bytes = random_generators::get_bytes();
    nb.buffer().append(bytes.c_str(), bytes.size());

    BOOST_TEST_MESSAGE("Request with invalid payload");
    // will fail all the futures as server close the connection
    auto ret = t.send(
                  std::move(nb), rpc::client_opts(rpc::clock_type::now() + 2s))
                 .get0();
    // reconnect
    BOOST_TEST_MESSAGE("Another request with valid payload");
    t.connect(model::no_timeout).get0();
    for (int i = 0; i < 10; ++i) {
        auto echo_resp_new = client
                               .echo(
                                 echo::echo_req{.str = "testing..."},
                                 rpc::client_opts(rpc::clock_type::now() + 2s))
                               .get0();

        BOOST_REQUIRE_EQUAL(echo_resp_new.value().data.str, "testing...");
    }
}
