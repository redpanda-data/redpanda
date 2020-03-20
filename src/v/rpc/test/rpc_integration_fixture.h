#pragma once
#include "config/tls_config.h"
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
    prefix_echo(echo::echo_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp>(
          echo::echo_resp{.str = fmt::format("prefix_{}", req.str)});
    }
    ss::future<echo::echo_resp>
    suffix_echo(echo::echo_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::echo_resp>(
          echo::echo_resp{.str = fmt::format("{}_suffix", req.str)});
    }

    ss::future<echo::echo_resp>
    sleep_1s(echo::echo_req&& req, rpc::streaming_context&) final {
        using namespace std::chrono_literals;
        return ss::sleep(1s).then(
          []() { return echo::echo_resp{.str = "Zzz..."}; });
    }

    ss::future<echo::cnt_resp>
    counter(echo::cnt_req&& req, rpc::streaming_context&) final {
        return ss::make_ready_future<echo::cnt_resp>(
          echo::cnt_resp{.expected = req.expected, .current = cnt++});
    }

    uint64_t cnt = 0;
};

class rpc_integration_fixture {
public:
    rpc_integration_fixture()
      : _listen_address(ss::net::inet_address("127.0.0.1"), 32147)
      , _ssg(ss::create_smp_service_group({5000}).get0()) {
        _sg = ss::create_scheduling_group("rpc scheduling group", 200).get0();
    }

    rpc::transport_configuration client_config(
      std::optional<ss::tls::credentials_builder> credentials
      = std::nullopt) const {
        return rpc::transport_configuration{.server_addr = _listen_address,
                                            .credentials = credentials};
    }

    void register_services() {
        check_server();
        auto proto = std::make_unique<rpc::simple_protocol>();
        proto->register_service<movistar>(_sg, _ssg);
        proto->register_service<echo_impl>(_sg, _ssg);
        _server->set_protocol(std::move(proto));
    }

    void configure_server(
      std::optional<ss::tls::credentials_builder> credentials = std::nullopt) {
        _server = std::make_unique<rpc::server>(rpc::server_configuration{
          .addrs = {_listen_address},
          .max_service_memory_per_core = static_cast<int64_t>(
            ss::memory::stats().total_memory() / 10),
          .credentials = std::move(credentials)});
    }

    void start_server() {
        check_server();
        _server->start();
    }

    ~rpc_integration_fixture() {
        if (_server) {
            _server->stop().get();
        }
        destroy_smp_service_group(_ssg).get0();
        destroy_scheduling_group(_sg).get0();
    }

private:
    void check_server() {
        if (!_server) {
            throw std::runtime_error("Configure server first!!!");
        }
    }
    ss::smp_service_group _ssg;
    ss::scheduling_group _sg;
    ss::socket_address _listen_address;
    std::unique_ptr<rpc::server> _server;
};
