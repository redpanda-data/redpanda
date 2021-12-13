/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/tls_config.h"
#include "rpc/dns.h"
#include "rpc/server.h"
#include "rpc/service.h"
#include "rpc/simple_protocol.h"
#include "rpc/test/cycling_service.h"
#include "rpc/test/echo_service.h"
#include "rpc/test/rpc_gen_types.h"
#include "rpc/transport.h"
#include "rpc/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

// Test services
struct movistar final : cycling::team_movistar_service {
    movistar(ss::scheduling_group& sc, ss::smp_service_group& ssg)
      : cycling::team_movistar_service(sc, ssg) {}
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

struct echo_impl final : echo::echo_service {
    echo_impl(ss::scheduling_group& sc, ss::smp_service_group& ssg)
      : echo::echo_service(sc, ssg) {}
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

    ss::future<echo::echo_resp>
    sleep_1s(echo::echo_req&&, rpc::streaming_context&) final {
        using namespace std::chrono_literals;
        return ss::sleep(1s).then(
          []() { return echo::echo_resp{.str = "Zzz..."}; });
    }

    ss::future<echo::cnt_resp>
    counter(echo::cnt_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::cnt_resp>(
          echo::cnt_resp{.expected = req.expected, .current = cnt++});
    }

    ss::future<echo::throw_resp>
    throw_exception(echo::throw_req&& req, rpc::streaming_context&) final {
        switch (req) {
        case echo::failure_type::exceptional_future:
            return ss::make_exception_future<echo::throw_resp>(
              std::runtime_error("gentle crash"));
        case echo::failure_type::throw_exception:
            throw std::runtime_error("bad crash");
        default:
            return ss::make_ready_future<echo::throw_resp>();
        }
    }

    uint64_t cnt = 0;
};

class rpc_base_integration_fixture {
public:
    explicit rpc_base_integration_fixture(uint16_t port)
      : _listen_address("127.0.0.1", port)
      , _ssg(ss::create_smp_service_group({5000}).get0())
      , _sg(ss::default_scheduling_group()) {}

    virtual ~rpc_base_integration_fixture() {
        destroy_smp_service_group(_ssg).get0();
    }

    virtual void start_server() = 0;
    virtual void stop_server() = 0;

    virtual void configure_server(
      std::optional<ss::tls::credentials_builder> credentials = std::nullopt,
      ss::tls::reload_callback&& cb = {})
      = 0;

    rpc::transport_configuration client_config(
      std::optional<ss::tls::credentials_builder> credentials
      = std::nullopt) const {
        return rpc::transport_configuration{
          .server_addr = _listen_address,
          .credentials
          = credentials
              ? credentials->build_reloadable_certificate_credentials().get0()
              : nullptr};
    }

protected:
    unresolved_address _listen_address;
    ss::smp_service_group _ssg;
    ss::scheduling_group _sg;

private:
    virtual void check_server() = 0;
};

class rpc_simple_integration_fixture : public rpc_base_integration_fixture {
public:
    explicit rpc_simple_integration_fixture(uint16_t port)
      : rpc_base_integration_fixture(port) {}

    ~rpc_simple_integration_fixture() override { stop_server(); }

    void start_server() override {
        check_server();
        _server->set_protocol(std::move(_proto));
        _server->start();
    }

    void stop_server() override {
        if (_server) {
            _server->stop().get();
        }
    }

    void configure_server(
      std::optional<ss::tls::credentials_builder> credentials = std::nullopt,
      ss::tls::reload_callback&& cb = {}) override {
        rpc::server_configuration scfg("unit_test_rpc");
        scfg.disable_metrics = rpc::metrics_disabled::yes;
        auto resolved = rpc::resolve_dns(_listen_address).get();
        scfg.addrs.emplace_back(
          resolved,
          credentials
            ? credentials->build_reloadable_server_credentials(std::move(cb))
                .get0()
            : nullptr);
        scfg.max_service_memory_per_core = static_cast<int64_t>(
          ss::memory::stats().total_memory() / 10);
        _server = std::make_unique<rpc::server>(std::move(scfg));
        _proto = std::make_unique<rpc::simple_protocol>();
    }

    template<typename Service, typename... Args>
    void register_service(Args&&... args) {
        check_server();
        _proto->register_service<Service>(
          _sg, _ssg, std::forward<Args>(args)...);
    }

private:
    void check_server() override {
        if (!_server || !_proto) {
            throw std::runtime_error("Configure server first!!!");
        }
    }

    std::unique_ptr<rpc::simple_protocol> _proto;
    std::unique_ptr<rpc::server> _server;
};

class rpc_sharded_integration_fixture : public rpc_base_integration_fixture {
public:
    explicit rpc_sharded_integration_fixture(uint16_t port)
      : rpc_base_integration_fixture(port) {}

    void start_server() override {
        check_server();
        _server.invoke_on_all(&rpc::server::start).get();
    }

    void stop_server() override { _server.stop().get(); }

    void configure_server(
      std::optional<ss::tls::credentials_builder> credentials = std::nullopt,
      ss::tls::reload_callback&& cb = {}) override {
        rpc::server_configuration scfg("unit_test_rpc_sharded");
        scfg.disable_metrics = rpc::metrics_disabled::yes;
        auto resolved = rpc::resolve_dns(_listen_address).get();
        scfg.addrs.emplace_back(
          resolved,
          credentials
            ? credentials->build_reloadable_server_credentials(std::move(cb))
                .get0()
            : nullptr);
        scfg.max_service_memory_per_core = static_cast<int64_t>(
          ss::memory::stats().total_memory() / 10);
        _server.start(std::move(scfg)).get();
    }

    template<typename Service, typename... Args>
    void register_service(Args&&... args) {
        check_server();
        _server
          .invoke_on_all(
            [this, args = std::make_tuple(std::forward<Args>(args)...)](
              rpc::server& s) mutable {
                std::apply(
                  [this, &s](Args&&... args) mutable {
                      auto proto = std::make_unique<rpc::simple_protocol>();
                      proto->register_service<Service>(
                        _sg, _ssg, std::forward<Args>(args)...);
                      s.set_protocol(std::move(proto));
                  },
                  std::move(args));
            })
          .get();
    }

private:
    void check_server() override {
        const bool all_initialized = ss::map_reduce(
                                       boost::irange<unsigned>(
                                         0, ss::smp::count),
                                       [this](unsigned /*c*/) {
                                           return ss::make_ready_future<bool>(
                                             _server.local_is_initialized());
                                       },
                                       true,
                                       std::logical_and<>())
                                       .get0();
        if (!all_initialized) {
            throw std::runtime_error("Configure server first!!!");
        }
    }

    ss::sharded<rpc::server> _server;
};

class rpc_integration_fixture : public rpc_simple_integration_fixture {
public:
    rpc_integration_fixture()
      : rpc_simple_integration_fixture(redpanda_rpc_port) {}

    void register_services() {
        register_service<movistar>();
        register_service<echo_impl>();
    }

    static constexpr uint16_t redpanda_rpc_port = 32147;
};
