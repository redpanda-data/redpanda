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
#include "rpc/parse_utils.h"
#include "rpc/test/cycling_service.h"
#include "rpc/test/echo_service.h"
#include "rpc/test/rpc_gen_types.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "rpc/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <exception>
#include <filesystem>

using namespace std::chrono_literals; // NOLINT

// Test services
template<typename Codec>
struct movistar final : cycling::team_movistar_service_base<Codec> {
    movistar(ss::scheduling_group& sc, ss::smp_service_group& ssg)
      : cycling::team_movistar_service_base<Codec>(sc, ssg) {}
    ss::future<cycling::mount_tamalpais>
    ibis_hakka(cycling::san_francisco&&, rpc::streaming_context&) final {
        return ss::make_ready_future<cycling::mount_tamalpais>(
          cycling::mount_tamalpais{66});
    }
    ss::future<cycling::nairo_quintana>
    canyon(cycling::ultimate_cf_slx&&, rpc::streaming_context&) final {
        return ss::make_ready_future<cycling::nairo_quintana>(
          cycling::nairo_quintana{32});
    }
};

template<typename Codec>
struct echo_impl final : echo::echo_service_base<Codec> {
    echo_impl(ss::scheduling_group& sc, ss::smp_service_group& ssg)
      : echo::echo_service_base<Codec>(sc, ssg) {}
    ss::future<echo::echo_resp>
    echo(echo::echo_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp>(
          echo::echo_resp{.str = req.str});
    }
    ss::future<echo::echo_resp>
    prefix_echo(echo::echo_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp>(
          echo::echo_resp{.str = ssx::sformat("prefix_{}", req.str)});
    }
    ss::future<echo::echo_resp>
    suffix_echo(echo::echo_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp>(
          echo::echo_resp{.str = ssx::sformat("{}_suffix", req.str)});
    }

    ss::future<echo::sleep_resp>
    sleep_for(echo::sleep_req&& req, rpc::streaming_context&) final {
        return ss::sleep(std::chrono::seconds(req.secs)).then([]() {
            return echo::sleep_resp{.str = "Zzz..."};
        });
    }

    ss::future<echo::cnt_resp>
    counter(echo::cnt_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::cnt_resp>(
          echo::cnt_resp{.expected = req.expected, .current = cnt++});
    }

    ss::future<echo::throw_resp>
    throw_exception(echo::throw_req&& req, rpc::streaming_context&) final {
        switch (req.type) {
        case echo::failure_type::exceptional_future:
            return ss::make_exception_future<echo::throw_resp>(
              std::runtime_error("gentle crash"));
        case echo::failure_type::throw_exception:
            throw std::runtime_error("bad crash");
        default:
            return ss::make_ready_future<echo::throw_resp>();
        }
    }

    ss::future<echo::echo_resp_adl_only> echo_adl_only(
      echo::echo_req_adl_only&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp_adl_only>(
          echo::echo_resp_adl_only{.str = req.str});
    }

    ss::future<echo::echo_resp_adl_serde> echo_adl_serde(
      echo::echo_req_adl_serde&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp_adl_serde>(
          echo::echo_resp_adl_serde{.str = req.str});
    }

    ss::future<echo::echo_resp_serde_only> echo_serde_only(
      echo::echo_req_serde_only&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp_serde_only>(
          echo::echo_resp_serde_only{.str = req.str});
    }

    uint64_t cnt = 0;
};

class rpc_integration_fixture : public rpc_simple_integration_fixture {
public:
    rpc_integration_fixture()
      : rpc_simple_integration_fixture(redpanda_rpc_port) {}

    void register_services() {
        register_service<movistar<rpc::default_message_codec>>();
        register_service<echo_impl<rpc::default_message_codec>>();
    }

    void register_services_v0() {
        register_service<movistar<rpc::v0_message_codec>>();
        register_service<echo_impl<rpc::v0_message_codec>>();
    }

    static constexpr uint16_t redpanda_rpc_port = 32147;
};

FIXTURE_TEST(echo_round_trip, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    auto client = rpc::client<echo::echo_client_protocol>(client_config());
    client.connect(model::no_timeout).get();
    auto cleanup = ss::defer([&client] { client.stop().get(); });

    const auto payload = random_generators::gen_alphanum_string(100);
    auto f = client.echo(
      echo::echo_req{.str = payload}, rpc::client_opts(rpc::no_timeout));
    auto ret = f.get();
    BOOST_REQUIRE(ret.has_value());
    BOOST_REQUIRE_EQUAL(ret.value().data.str, payload);
}

FIXTURE_TEST(echo_round_trip_tls, rpc_integration_fixture) {
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

    auto client = rpc::client<echo::echo_client_protocol>(
      client_config(creds_builder));
    client.connect(model::no_timeout).get();
    auto cleanup = ss::defer([&client] { client.stop().get(); });

    const auto payload = random_generators::gen_alphanum_string(100);
    auto f = client.echo(
      echo::echo_req{.str = payload}, rpc::client_opts(rpc::no_timeout));
    auto ret = f.get();
    BOOST_REQUIRE(ret.has_value());
    BOOST_REQUIRE_EQUAL(ret.value().data.str, payload);
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
                                  true)
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
                                  true)
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
    info("Calling sleep for.. 1s");
    auto echo_resp = client.sleep_for(
      echo::sleep_req{.secs = 1},
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
                   {.type = echo::failure_type::exceptional_future},
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
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    const auto check_missing = [&] {
        auto f = t.send_typed<echo::echo_req, echo::echo_resp>(
          echo::echo_req{.str = "testing..."},
          1234,
          rpc::client_opts(rpc::no_timeout));
        return f.then([&](auto ret) {
            BOOST_REQUIRE(ret.has_error());
            BOOST_REQUIRE_EQUAL(ret.error(), rpc::errc::method_not_found);
        });
    };

    const auto check_success = [&] {
        auto f = client.echo(
          echo::echo_req{.str = "testing..."},
          rpc::client_opts(rpc::no_timeout));
        return f.then([&](auto ret) {
            BOOST_REQUIRE(ret.has_value());
            BOOST_REQUIRE_EQUAL(ret.value().data.str, "testing...");
        });
    };

    /*
     * randomizing the messages here is intentional: we want to ensure that
     * missing method error can be handled in any situation and that the
     * connection remains in a healthy state.
     */
    std::vector<std::function<ss::future<>()>> request_factory;
    for (int i = 0; i < 200; i++) {
        request_factory.emplace_back(check_missing);
        request_factory.emplace_back(check_success);
    }
    std::shuffle(
      request_factory.begin(),
      request_factory.end(),
      random_generators::internal::gen);

    // dispatch the requests
    std::vector<ss::future<>> requests;
    for (const auto& factory : request_factory) {
        requests.emplace_back(factory());
    }

    ss::when_all_succeed(requests.begin(), requests.end()).get();
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
    nb.set_service_method_id(echo::echo_service::echo_method_id);
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
    nb.set_service_method_id(echo::echo_service::echo_method_id);
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

/*
 * the not_supported_version test uses the echo_adl_serde variant rather than
 * the original version whose types cause it to be treated as adl-only. Because
 * adl-only messages are sent at v0 and the test specifically requires sending
 * messages at an arbitrarily higher value to trigger the error, a type was
 * needed that supports a dynamic version range. When encoding adl/serde
 * supported types the version is passed through.
 */
FIXTURE_TEST(version_not_supported, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    const auto check_unsupported = [&] {
        auto f = t.send_typed_versioned<
          echo::echo_req_adl_serde,
          echo::echo_resp_adl_serde>(
          echo::echo_req_adl_serde{.str = "testing..."},
          echo::echo_service::echo_adl_serde_method_id,
          rpc::client_opts(rpc::no_timeout),
          rpc::transport_version::unsupported);
        return f.then([&](auto ret) {
            BOOST_REQUIRE(ret.has_value()); // low-level rpc succeeded

            BOOST_REQUIRE(ret.value().ctx.has_error()); // payload failed
            BOOST_REQUIRE_EQUAL(
              ret.value().ctx.error(), rpc::errc::version_not_supported);

            // returned version is server's max supported
            BOOST_REQUIRE_EQUAL(
              static_cast<uint8_t>(ret.value().version),
              static_cast<uint8_t>(rpc::transport_version::max_supported));
        });
    };

    const auto check_supported = [&] {
        auto f = client.echo_adl_serde(
          echo::echo_req_adl_serde{.str = "testing..."},
          rpc::client_opts(rpc::no_timeout));
        return f.then([&](auto ret) {
            BOOST_REQUIRE(ret.has_value());
            // could be either one. depends on timing of transport upgrade
            BOOST_REQUIRE(
              ret.value().data.str
                == "testing..._to_aas_from_aas_to_aas_from_aas"
              || ret.value().data.str
                   == "testing..._to_sas_from_sas_to_sas_from_sas");
        });
    };

    /*
     * randomizing the messages here is intentional: we want to ensure that
     * unsupported version error can be handled in any situation and that the
     * connection remains in a healthy state.
     */
    std::vector<std::function<ss::future<>()>> request_factory;
    for (int i = 0; i < 200; i++) {
        request_factory.emplace_back(check_unsupported);
        request_factory.emplace_back(check_supported);
    }
    std::shuffle(
      request_factory.begin(),
      request_factory.end(),
      random_generators::internal::gen);

    // dispatch the requests
    std::vector<ss::future<>> requests;
    for (const auto& factory : request_factory) {
        requests.emplace_back(factory());
    }

    ss::when_all_succeed(requests.begin(), requests.end()).get();
}

class erroneous_protocol_exception : public std::exception {};

class erroneous_protocol final : public net::server::protocol {
public:
    template<std::derived_from<rpc::service> T, typename... Args>
    void register_service(Args&&... args) {
        _services.push_back(std::make_unique<T>(std::forward<Args>(args)...));
    }

    const char* name() const final { return "redpanda erraneous proto"; };

    ss::future<> apply(net::server::resources rs) final {
        return ss::do_until(
          [rs] { return rs.conn->input().eof() || rs.abort_requested(); },
          [this, rs]() mutable {
              return ss::make_exception_future<>(
                erroneous_protocol_exception());
          });
    }

private:
    std::vector<std::unique_ptr<rpc::service>> _services;
};

class erroneous_service_fixture
  : public rpc_fixture_swappable_proto<erroneous_protocol> {
public:
    erroneous_service_fixture()
      : rpc_fixture_swappable_proto(redpanda_rpc_port) {}

    void register_services() {
        register_service<movistar<rpc::default_message_codec>>();
        register_service<echo_impl<rpc::default_message_codec>>();
    }

    static constexpr uint16_t redpanda_rpc_port = 32147;
};

FIXTURE_TEST(unhandled_throw_in_proto_apply, erroneous_service_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto client = echo::echo_client_protocol(t);

    /// Server should not crash at this line
    client
      .echo(
        echo::echo_req{.str = "testing..."}, rpc::client_opts(rpc::no_timeout))
      .get();
    t.stop().get();
}

/*
 * new client, new server
 * client has initial transport version v1
 * sends adl+serde message at (adl,v1)
 * client has transport upgraded to v2
 * client transport remains at v2
 */
FIXTURE_TEST(nc_ns_adl_serde_client_upgraded, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);

    // first messages are sent with adl
    {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_adl_serde(
          echo::echo_req_adl_serde{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_aas_from_aas_to_aas_from_aas");
    }

    // subsequent messages use serde
    for (int i = 0; i < 10; i++) {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_adl_serde(
          echo::echo_req_adl_serde{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_sas_from_sas_to_sas_from_sas");

        // upgraded and remains at v2
        BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v2);
    }
}

/*
 * new client, new server
 * client has initial transport version v1
 * sends serde-only message at (serde,v2)
 * client has transport upgraded to v2
 * client transport remains at v2
 */
FIXTURE_TEST(nc_ns_serde_only_client_upgraded, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);

    for (int i = 0; i < 10; i++) {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_serde_only(
          echo::echo_req_serde_only{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_sso_from_sso_to_sso_from_sso");

        // upgraded and remains at v2
        BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v2);
    }
}

/*
 * new client, new server
 * client sends adl-only message (adl,v1)
 * client remains pinned at v1
 *
 * client will not be upgraded. adl-only messages are always set at v0 and the
 * server will always respond with v0 messages. upgrade doesn't happen because
 * client only upgrades in response to a v1 or v2 message.
 *
 * this case is for the interim development period where we are allowing types
 * with only adl support until all types have serde support added.
 */
FIXTURE_TEST(nc_ns_adl_only_no_client_upgrade, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);

    for (int i = 0; i < 10; i++) {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_adl_only(
          echo::echo_req_adl_only{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_aao_from_aao_to_aao_from_aao");

        // no upgrade
        BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);
    }
}

/*
 * new client, old server
 * client has initial transport version v1
 * [sends adl+serde message at (adl,v1)] * N
 * client transport version is not upgraded
 */
FIXTURE_TEST(nc_os_adl_serde_no_client_upgrade, rpc_integration_fixture) {
    configure_server();
    register_services_v0();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    // client initially at v1
    BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);

    for (int i = 0; i < 10; i++) {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_adl_serde(
          echo::echo_req_adl_serde{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_aas_from_aas_to_aas_from_aas");

        // client stays at v1 without upgrade to v2
        BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);
    }
}

/*
 * new client, old server
 * client has initial transport version v1
 * [sends adl-only message at (adl,v1)] * N
 * client transport verison is not upgraded
 */
FIXTURE_TEST(nc_os_adl_only_no_client_upgrade, rpc_integration_fixture) {
    configure_server();
    register_services_v0();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    // client initially at v1
    BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);

    for (int i = 0; i < 10; i++) {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_adl_only(
          echo::echo_req_adl_only{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_aao_from_aao_to_aao_from_aao");

        // client stays at v1 without upgrade to v2
        BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v1);
    }
}

/*
 * old client, new server
 * sends an adl encoded message which the server understands but also has serde
 * support for. communication should continue to use adl.
 */
FIXTURE_TEST(oc_ns_adl_serde_no_upgrade, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    t.set_version(rpc::transport_version::v0); // connect resets version=v1
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v0);

    for (int i = 0; i < 10; i++) {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_adl_serde(
          echo::echo_req_adl_serde{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_aas_from_aas_to_aas_from_aas");
        BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v0);
    }
}

/*
 * old client, new server
 * adl-only. verifies behavior for intermediate state when we support adl-only
 * messages.
 */
FIXTURE_TEST(oc_ns_adl_only_no_upgrade, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    t.set_version(rpc::transport_version::v0); // connect resets version=v1
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v0);

    for (int i = 0; i < 10; i++) {
        const auto payload = random_generators::gen_alphanum_string(100);
        auto f = client.echo_adl_only(
          echo::echo_req_adl_only{.str = payload},
          rpc::client_opts(rpc::no_timeout));
        auto ret = f.get();
        BOOST_REQUIRE(ret.has_value());
        BOOST_REQUIRE_EQUAL(
          ret.value().data.str, payload + "_to_aao_from_aao_to_aao_from_aao");
        BOOST_REQUIRE_EQUAL(t.version(), rpc::transport_version::v0);
    }
}
