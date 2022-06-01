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
#include "config/tls_config.h"
#include "net/dns.h"
#include "net/server.h"
#include "rpc/service.h"
#include "rpc/simple_protocol.h"
#include "rpc/transport.h"
#include "rpc/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

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
    net::unresolved_address _listen_address;
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
        net::server_configuration scfg("unit_test_rpc");
        scfg.disable_metrics = net::metrics_disabled::yes;
        auto resolved = net::resolve_dns(_listen_address).get();
        scfg.addrs.emplace_back(
          resolved,
          credentials
            ? credentials->build_reloadable_server_credentials(std::move(cb))
                .get0()
            : nullptr);
        scfg.max_service_memory_per_core = static_cast<int64_t>(
          ss::memory::stats().total_memory() / 10);
        _server = std::make_unique<net::server>(std::move(scfg));
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
    std::unique_ptr<net::server> _server;
};

class rpc_sharded_integration_fixture : public rpc_base_integration_fixture {
public:
    explicit rpc_sharded_integration_fixture(uint16_t port)
      : rpc_base_integration_fixture(port) {}

    void start_server() override {
        check_server();
        _server.invoke_on_all(&net::server::start).get();
    }

    void stop_server() override { _server.stop().get(); }

    void configure_server(
      std::optional<ss::tls::credentials_builder> credentials = std::nullopt,
      ss::tls::reload_callback&& cb = {}) override {
        net::server_configuration scfg("unit_test_rpc_sharded");
        scfg.disable_metrics = net::metrics_disabled::yes;
        auto resolved = net::resolve_dns(_listen_address).get();
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
              net::server& s) mutable {
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

    ss::sharded<net::server> _server;
};
