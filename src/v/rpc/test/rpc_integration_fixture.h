/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "config/tls_config.h"
#include "net/dns.h"
#include "net/server.h"
#include "rpc/rpc_server.h"
#include "rpc/service.h"
#include "rpc/transport.h"
#include "rpc/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

class rpc_base_integration_fixture {
public:
    explicit rpc_base_integration_fixture(uint16_t port)
      : _listen_address("127.0.0.1", port)
      , _ssg(ss::create_smp_service_group({5000}).get())
      , _sg(ss::default_scheduling_group()) {}

    virtual ~rpc_base_integration_fixture() {
        destroy_smp_service_group(_ssg).get();
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
              ? credentials->build_reloadable_certificate_credentials().get()
              : nullptr};
    }

protected:
    net::unresolved_address _listen_address;
    ss::smp_service_group _ssg;
    ss::scheduling_group _sg;

private:
    virtual void check_server() = 0;
};

template<std::derived_from<net::server> T>
class rpc_fixture_swappable_proto : public rpc_base_integration_fixture {
public:
    explicit rpc_fixture_swappable_proto(uint16_t port)
      : rpc_base_integration_fixture(port) {}

    ~rpc_fixture_swappable_proto() override { stop_server(); }

    void start_server() override {
        check_server();
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
        net::server_configuration scfg("unit_test_rpc");
        scfg.disable_metrics = net::metrics_disabled::yes;
        auto resolved = net::resolve_dns(_listen_address).get();
        scfg.addrs.emplace_back(
          resolved,
          credentials
            ? credentials->build_reloadable_server_credentials(std::move(cb))
                .get()
            : nullptr);
        scfg.max_service_memory_per_core = static_cast<int64_t>(
          ss::memory::stats().total_memory() / 10);
        if constexpr (std::is_same_v<T, rpc::rpc_server>) {
            _server = std::make_unique<T>(std::move(scfg));
        } else {
            _server = std::make_unique<T>(std::move(scfg), rpc::rpclog);
        }
    }

    template<typename Service, typename... Args>
    void register_service(Args&&... args) {
        check_server();
        _server->template register_service<Service>(
          _sg, _ssg, std::forward<Args>(args)...);
    }

    T& server() {
        check_server();
        return *_server;
    }

private:
    void check_server() override {
        if (!_server) {
            throw std::runtime_error("Configure server first!!!");
        }
    }

    std::unique_ptr<T> _server;
};

using rpc_simple_integration_fixture
  = rpc_fixture_swappable_proto<rpc::rpc_server>;

class rpc_sharded_integration_fixture : public rpc_base_integration_fixture {
public:
    explicit rpc_sharded_integration_fixture(uint16_t port)
      : rpc_base_integration_fixture(port) {}

    ~rpc_sharded_integration_fixture() override { stop_server(); }

    void start_server() override {
        check_server();
        _server.invoke_on_all(&rpc::rpc_server::start).get();
    }

    void stop_server() override { _server.stop().get(); }

    void configure_server(
      std::optional<ss::tls::credentials_builder> credentials = std::nullopt,
      ss::tls::reload_callback&& cb = {}) override {
        net::server_configuration scfg("unit_test_rpc_sharded");
        scfg.disable_metrics = net::metrics_disabled::yes;
        scfg.disable_public_metrics = net::public_metrics_disabled::yes;
        auto resolved = net::resolve_dns(_listen_address).get();
        scfg.addrs.emplace_back(
          resolved,
          credentials
            ? credentials->build_reloadable_server_credentials(std::move(cb))
                .get()
            : nullptr);
        scfg.max_service_memory_per_core = static_cast<int64_t>(
          ss::memory::stats().total_memory() / 10);
        _server.start(std::move(scfg)).get();
    }

    ss::sharded<rpc::rpc_server>& server() { return _server; }

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
                                       .get();
        if (!all_initialized) {
            throw std::runtime_error("Configure server first!!!");
        }
    }

    ss::sharded<rpc::rpc_server> _server;
};
