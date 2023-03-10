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
#include "rpc/backoff_policy.h"
#include "rpc/connection_cache.h"
#include "rpc/exceptions.h"
#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/test/cycling_service.h"
#include "rpc/test/echo_service.h"
#include "rpc/test/echo_v2_service.h"
#include "rpc/test/rpc_gen_types.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "rpc/types.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/metrics_api.hh>
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

    ss::future<echo::echo_resp>
    echo(echo::echo_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp>(
          echo::echo_resp{.str = req.str});
    }

    uint64_t cnt = 0;
};

template<typename Codec>
struct echo_v2_impl final : echo_v2::echo_service_base<Codec> {
    echo_v2_impl(ss::scheduling_group& sc, ss::smp_service_group& ssg)
      : echo_v2::echo_service_base<Codec>(sc, ssg) {}

    ss::future<echo_v2::echo_resp>
    echo(echo_v2::echo_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo_v2::echo_resp>(
          echo_v2::echo_resp{.str = req.str, .str_two = req.str_two});
    }
};

class rpc_integration_fixture : public rpc_simple_integration_fixture {
public:
    rpc_integration_fixture()
      : rpc_simple_integration_fixture(redpanda_rpc_port) {}

    void register_services() {
        register_service<movistar<rpc::default_message_codec>>();
        register_service<echo_impl<rpc::default_message_codec>>();
        register_service<echo_v2_impl<rpc::default_message_codec>>();
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

FIXTURE_TEST(echo_from_cache, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();
    rpc::connection_cache cache;

    // Check that we can create connections from a cache, and moreover that we
    // can run several clients targeted at the same server, if we provide
    // multiple node IDs to the cache.
    constexpr const size_t num_nodes_ids = 10;
    const auto ccfg = client_config();
    for (int i = 0; i < num_nodes_ids; ++i) {
        const auto payload = random_generators::gen_alphanum_string(100);
        const auto node_id = model::node_id(i);
        cache
          .emplace(
            node_id,
            ccfg,
            rpc::make_exponential_backoff_policy<rpc::clock_type>(
              std::chrono::milliseconds(1), std::chrono::milliseconds(1)))
          .get();
        auto reconnect_transport = cache.get(node_id);
        auto transport_res = reconnect_transport
                               ->get_connected(rpc::clock_type::now() + 5s)
                               .get();
        BOOST_REQUIRE(transport_res.has_value());
        auto transport = transport_res.value();
        echo::echo_client_protocol client(*transport);
        auto cleanup = ss::defer([&transport] { transport->stop().get(); });
        auto f = client.echo(
          echo::echo_req{.str = payload},
          rpc::client_opts(rpc::clock_type::now() + 100ms));
        auto ret = f.get();
        BOOST_CHECK(ret.has_value());
        BOOST_CHECK_EQUAL(ret.value().data.str, payload);
    }
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
    BOOST_REQUIRE(ret.has_value());
    info("Calling echo method");
    auto echo_resp = client
                       .suffix_echo(
                         echo::echo_req{.str = "testing..."},
                         rpc::client_opts(rpc::no_timeout))
                       .get0();
    client.stop().get();
    BOOST_REQUIRE(echo_resp.has_value());

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

FIXTURE_TEST(timeout_test_cleanup_resources, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::client<echo::echo_client_protocol> client(client_config());
    client.connect(model::no_timeout).get();
    using units_t = std::vector<ssx::semaphore_units>;
    ::mutex lock;
    units_t units;
    units.push_back(std::move(lock.get_units().get()));

    auto opts = rpc::client_opts(rpc::clock_type::now() + 1s);
    opts.resource_units = ss::make_foreign(
      ss::make_lw_shared(std::move(units)));

    // Resources should now be owned by the client.
    BOOST_REQUIRE(lock.try_get_units().has_value() == false);
    auto echo_resp = client.sleep_for(
      echo::sleep_req{.secs = 10}, std::move(opts));
    BOOST_REQUIRE_EQUAL(
      echo_resp.get0().error(), rpc::errc::client_request_timeout);
    // Verify that the resources are released correctly after timeout.
    BOOST_REQUIRE(lock.try_get_units().has_value());
    client.stop().get();
}

FIXTURE_TEST(test_cleanup_on_timeout_before_sending, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::client<echo::echo_client_protocol> client(client_config());
    client.connect(model::no_timeout).get();
    auto stop_client = ss::defer([&] { client.stop().get(); });

    size_t num_reqs = 10;

    // Dispatch several requests with zero timeout. Presumably, some of them
    // will timeout before even sending a message to the server. Even in this
    // case resource units must be returned before the request finishes.
    std::vector<ss::future<>> requests;
    for (size_t i = 0; i < num_reqs; ++i) {
        auto lock = ss::make_lw_shared<::mutex>();

        auto units = lock->try_get_units();
        BOOST_REQUIRE(units);
        std::vector<ssx::semaphore_units> units_vec;
        units_vec.push_back(std::move(*units));
        auto opts = rpc::client_opts(rpc::clock_type::now());
        opts.resource_units = ss::make_foreign(
          ss::make_lw_shared(std::move(units_vec)));

        auto f = client.sleep_for(echo::sleep_req{.secs = 1}, std::move(opts))
                   .then([lock, i](auto result) {
                       info("finished request {}", i);
                       BOOST_REQUIRE_EQUAL(
                         result.error(), rpc::errc::client_request_timeout);
                       BOOST_REQUIRE(lock->ready());
                   });
        requests.push_back(std::move(f));
    }

    ss::when_all_succeed(requests.begin(), requests.end()).get();
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
                      rpc::timeout_spec::none,
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
          client
            .counter(
              echo::cnt_req{.expected = i}, rpc::client_opts(rpc::no_timeout))
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

    const auto check_missing = [&](rpc::errc expected_errc) {
        auto f = t.send_typed<echo::echo_req, echo::echo_resp>(
          echo::echo_req{.str = "testing..."},
          {"missing_method_test::missing", 1234},
          rpc::client_opts(rpc::no_timeout));
        return f.then([&, expected_errc](auto ret) {
            BOOST_REQUIRE(ret.has_error());
            BOOST_REQUIRE_EQUAL(ret.error(), expected_errc);
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
    const auto verify_bad_method_errors = [&](rpc::errc expected_errc) {
        std::vector<std::function<ss::future<>()>> request_factory;
        for (int i = 0; i < 200; i++) {
            request_factory.emplace_back(
              [&, expected_errc] { return check_missing(expected_errc); });
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
    };
    // If the server is configured to allow use of service_unavailable, while
    // the server hasn't added all services, we should see a retriable error
    // instead of method_not_found.
    server().set_use_service_unavailable();
    verify_bad_method_errors(rpc::errc::exponential_backoff);

    server().set_all_services_added();
    verify_bad_method_errors(rpc::errc::method_not_found);
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
    nb.set_service_method(echo::echo_service::echo_method);
    serde::write(nb.buffer(), echo::echo_req{.str = "testing..."});
    std::exception_ptr err;
    try {
        // will fail all the futures as server close the connection
        auto ret
          = t.send(std::move(nb), rpc::client_opts(rpc::clock_type::now() + 1s))
              .get0();
        ret.value()->signal_body_parse();
    } catch (...) {
        err = std::current_exception();
    }

    BOOST_REQUIRE(err);

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
    nb.set_service_method(echo::echo_service::echo_method);
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

FIXTURE_TEST(version_not_supported, rpc_integration_fixture) {
    configure_server();
    register_services();
    start_server();

    rpc::transport t(client_config());
    t.connect(model::no_timeout).get();
    auto stop = ss::defer([&t] { t.stop().get(); });
    auto client = echo::echo_client_protocol(t);

    const auto check_unsupported = [&] {
        auto f = t.send_typed_versioned<echo::echo_req, echo::echo_resp>(
          echo::echo_req{.str = "testing..."},
          echo::echo_service::echo_method,
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
        auto f = client.echo(
          echo::echo_req{.str = "testing..."},
          rpc::client_opts(rpc::no_timeout));
        return f.then([&](auto ret) {
            BOOST_REQUIRE(ret.has_value());
            BOOST_REQUIRE(ret.value().data.str == "testing...");
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

class erroneous_protocol_server final : public net::server {
public:
    using net::server::server;

    template<std::derived_from<rpc::service> T, typename... Args>
    void register_service(Args&&... args) {
        _services.push_back(std::make_unique<T>(std::forward<Args>(args)...));
    }

    std::string_view name() const final { return "redpanda erraneous proto"; };

    ss::future<> apply(ss::lw_shared_ptr<net::connection> conn) final {
        return ss::do_until(
          [this, conn] { return conn->input().eof() || abort_requested(); },
          [] {
              return ss::make_exception_future<>(
                erroneous_protocol_exception());
          });
    }

private:
    std::vector<std::unique_ptr<rpc::service>> _services;
};

class erroneous_service_fixture
  : public rpc_fixture_swappable_proto<erroneous_protocol_server> {
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

class rpc_sharded_fixture : public rpc_sharded_integration_fixture {
public:
    static constexpr uint16_t redpanda_rpc_port = 32147;
    rpc_sharded_fixture()
      : rpc_sharded_integration_fixture(redpanda_rpc_port) {}
};

FIXTURE_TEST(rpc_add_service, rpc_sharded_fixture) {
    configure_server();
    start_server();
    auto client = rpc::client<echo::echo_client_protocol>(client_config());
    client.connect(model::no_timeout).get();
    auto cleanup = ss::defer([&client] { client.stop().get(); });

    const auto payload = random_generators::gen_alphanum_string(100);
    const auto echo_result = [&] {
        auto f = client.echo(
          echo::echo_req{.str = payload}, rpc::client_opts(rpc::no_timeout));
        return f.get();
    };
    BOOST_REQUIRE(!echo_result().has_value());

    server()
      .invoke_on_all([this](rpc::rpc_server& s) {
          std::vector<std::unique_ptr<rpc::service>> service;
          service.emplace_back(
            std::make_unique<echo_impl<rpc::default_message_codec>>(_sg, _ssg));
          s.add_services(std::move(service));
      })
      .get();
    const auto res = echo_result();
    BOOST_REQUIRE(res.has_value());
    BOOST_CHECK_EQUAL(payload, res.value().data.str);
}

// Runs the given RPC-sending function, validating the successes and
// failures, depending on whether the service has been registered.
template<typename workload_t>
static ss::future<> send_req_until_close(
  ss::gate& rpc_g,
  workload_t workload_fn,
  ss::sstring service_name,
  int& num_successes,
  int& num_failures,
  bool& service_start_support,
  bool& service_full_support) {
    while (!rpc_g.is_closed()) {
        // Cache the full-support flag before sending anything out so we
        // know up front whether to expect success.
        bool full_support = service_full_support;
        auto res = co_await workload_fn();
        if (full_support) {
            BOOST_CHECK_MESSAGE(
              res.has_value(),
              ssx::sformat(
                "{} service was fully started; requests should not fail",
                service_name));
            ++num_successes;
        } else if (res.has_value()) {
            BOOST_CHECK_MESSAGE(
              service_start_support,
              ssx::sformat(
                "{} request succeeded; service must have been started",
                service_name));
            ++num_successes;
        } else {
            ++num_failures;
        }
    }
};

// Test that exercises adding new services to the RPC server while there are
// requests being processed.
FIXTURE_TEST(rpc_mt_add_service, rpc_sharded_fixture) {
    configure_server();
    start_server();
    // Disable client metrics, since Seastar doesn't like when two clients
    // register the same metrics.
    auto ccfg = client_config();
    ccfg.disable_metrics = net::metrics_disabled::yes;
    auto echo_client = rpc::client<echo::echo_client_protocol>(ccfg);
    auto movistar_client = rpc::client<cycling::team_movistar_client_protocol>(
      ccfg);
    echo_client.connect(model::no_timeout).get();
    movistar_client.connect(model::no_timeout).get();
    ss::gate rpc_g;

    // Starts the service while the workload is running, ensuring we see the
    // expected success and errors at different points in service registration.
    const auto start_service_and_wait =
      [&]<typename workload_t, typename start_srv_t>(
        workload_t workload_fn,
        start_srv_t start_service_fn,
        ss::sstring service_name,
        std::optional<ss::future<>>& workload_fut,
        int& num_successes,
        int& num_failures,
        bool& service_start_support,
        bool& service_full_support) {
          // Start our workload before we start the service.
          workload_fut = send_req_until_close(
            rpc_g,
            std::move(workload_fn),
            service_name,
            num_successes,
            num_failures,
            service_start_support,
            service_full_support);
          rpc::rpclog.info("Started {}-ing", service_name);

          // Verify that we're unable to proceed.
          constexpr const auto num_reqs_to_wait = 10;
          while (num_failures < num_reqs_to_wait) {
              ss::sleep(1s).get();
          }
          BOOST_CHECK_EQUAL(0, num_successes);

          rpc::rpclog.info("Starting {} service support", service_name);
          service_start_support = true;
          start_service_fn();
          service_full_support = true;
          rpc::rpclog.info("Started {} service support", service_name);

          // Verify that we're able to proceed.
          bool successes_after_start = num_successes;
          while (num_successes < successes_after_start + num_reqs_to_wait) {
              ss::sleep(1s).get();
          }
      };

    // NOTE: these live for the duration of the test so our async workload can
    // continue through to completion.
    int echo_num_success = 0, echo_num_failures = 0, movistar_num_success = 0,
        movistar_num_failures = 0;
    bool echo_start_support = false, echo_full_support = false,
         movistar_start_support = false, movistar_full_support = false;
    std::optional<ss::future<>> echo_loop_fut;
    std::optional<ss::future<>> movistar_loop_fut;
    auto cleanup = ss::defer([&echo_client,
                              &movistar_client,
                              &rpc_g,
                              &echo_loop_fut,
                              &movistar_loop_fut] {
        // Stop all workloads.
        if (!rpc_g.is_closed()) {
            rpc_g.close().get();
        }
        // Wait for the workloads to complete.
        if (echo_loop_fut.has_value()) {
            echo_loop_fut->get();
        }
        if (movistar_loop_fut.has_value()) {
            movistar_loop_fut->get();
        }
        // Stop the clients.
        echo_client.stop().get();
        movistar_client.stop().get();
    });

    // Run through the echo service, ensuring we see the expected responses.
    start_service_and_wait(
      [&] {
          const auto payload = random_generators::gen_alphanum_string(100);
          return echo_client.echo(
            echo::echo_req{.str = payload}, rpc::client_opts(rpc::no_timeout));
      },
      [this] {
          server()
            .invoke_on_all([this](rpc::rpc_server& s) {
                std::vector<std::unique_ptr<rpc::service>> service;
                service.emplace_back(
                  std::make_unique<echo_impl<rpc::default_message_codec>>(
                    _sg, _ssg));
                s.add_services(std::move(service));
            })
            .get();
      },
      "echo",
      echo_loop_fut,
      echo_num_success,
      echo_num_failures,
      echo_start_support,
      echo_full_support);

    // Now run through the movistar service, ensuring we see the expected
    // responses.
    // NOTE: we are explicitly not stopping the echo client workload. Requests
    // should continue to succeed while we add a new service.
    start_service_and_wait(
      [&movistar_client] {
          return movistar_client.ibis_hakka(
            cycling::san_francisco{}, rpc::client_opts(rpc::no_timeout));
      },
      [this] {
          server()
            .invoke_on_all([this](rpc::rpc_server& s) {
                std::vector<std::unique_ptr<rpc::service>> service;
                service.emplace_back(
                  std::make_unique<movistar<rpc::default_message_codec>>(
                    _sg, _ssg));
                s.add_services(std::move(service));
            })
            .get();
      },
      "movistar",
      movistar_loop_fut,
      movistar_num_success,
      movistar_num_failures,
      movistar_start_support,
      movistar_full_support);
}
