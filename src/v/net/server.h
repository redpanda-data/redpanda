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

#include "config/property.h"
#include "metrics/metrics.h"
#include "net/conn_quota.h"
#include "net/connection.h"
#include "net/connection_rate.h"
#include "net/types.h"
#include "ssx/semaphore.h"
#include "utils/log_hist.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>
#include <seastar/net/tls.hh>

#include <boost/intrusive/list.hpp>

#include <list>
#include <optional>
#include <type_traits>
#include <vector>

/*
 * TODO:
 *  - server_config has some simple_protocol bits
 */
namespace net {

struct server_endpoint {
    ss::sstring name;
    ss::socket_address addr;
    ss::shared_ptr<ss::tls::server_credentials> credentials;

    server_endpoint(ss::sstring name, ss::socket_address addr)
      : name(std::move(name))
      , addr(addr) {}

    server_endpoint(
      ss::sstring name,
      ss::socket_address addr,
      ss::shared_ptr<ss::tls::server_credentials> creds)
      : name(std::move(name))
      , addr(addr)
      , credentials(std::move(creds)) {}

    server_endpoint(
      ss::socket_address addr,
      ss::shared_ptr<ss::tls::server_credentials> creds)
      : server_endpoint("", addr, std::move(creds)) {}

    explicit server_endpoint(ss::socket_address addr)
      : server_endpoint("", addr) {}

    friend std::ostream& operator<<(std::ostream&, const server_endpoint&);
};

struct config_connection_rate_bindings {
    config::binding<std::optional<int64_t>> config_general_rate;
    config::binding<std::vector<ss::sstring>> config_overrides_rate;
};

struct tcp_keepalive_bindings {
    config::binding<std::chrono::seconds> keepalive_idle_time;
    config::binding<std::chrono::seconds> keepalive_interval;
    config::binding<uint32_t> keepalive_probes;
};

struct server_configuration {
    std::vector<server_endpoint> addrs;
    int64_t max_service_memory_per_core = 0;
    std::optional<int> listen_backlog;
    std::optional<int> tcp_recv_buf;
    std::optional<int> tcp_send_buf;
    std::optional<size_t> stream_recv_buf;
    net::metrics_disabled disable_metrics = net::metrics_disabled::no;
    net::public_metrics_disabled disable_public_metrics
      = net::public_metrics_disabled::no;
    ss::sstring name;
    std::optional<config_connection_rate_bindings> connection_rate_bindings;
    std::optional<tcp_keepalive_bindings> tcp_keepalive_bindings;
    // we use the same default as seastar for load balancing algorithm
    ss::server_socket::load_balancing_algorithm load_balancing_algo
      = ss::server_socket::load_balancing_algorithm::connection_distribution;

    std::optional<std::reference_wrapper<ss::sharded<conn_quota>>> conn_quotas;

    explicit server_configuration(ss::sstring n)
      : name(std::move(n)) {}

    friend std::ostream& operator<<(std::ostream&, const server_configuration&);
};

class server {
public:
    using hist_t = log_hist_internal;

    explicit server(server_configuration, ss::logger&);
    explicit server(ss::sharded<server_configuration>* s, ss::logger&);
    server(server&&) = delete;
    server& operator=(server&&) = delete;
    server(const server&) = delete;
    server& operator=(const server&) = delete;
    virtual ~server();

    void start();

    /**
     * The RPC server can be shutdown in two phases. First phase initiated with
     * `shutdown_input` prevents server from accepting new requests and
     * connections. In second phases `wait_for_shutdown` caller waits for all
     * pending requests to finish. This interface is convinient as it allows
     * stopping the server without waiting for downstream services to stop
     * requests processing
     */
    ss::future<> shutdown_input();
    ss::future<> wait_for_shutdown();
    /**
     * Stop function is a nop when `shutdown_input` was previously called. Left
     * here for convenience when dealing with `seastar::sharded` type
     */
    ss::future<> stop();

    const server_configuration cfg; // NOLINT
    const hist_t& histogram() const { return _hist; }

    virtual std::string_view name() const = 0;
    virtual ss::future<> apply(ss::lw_shared_ptr<net::connection>) = 0;

    server_probe& probe() { return *_probe; }
    ssx::semaphore& memory() { return _memory; }
    ss::gate& conn_gate() { return _conn_gate; }
    hist_t& hist() { return _hist; }
    ss::abort_source& abort_source() { return _as; }
    bool abort_requested() const { return _as.abort_requested(); }

private:
    struct listener {
        ss::sstring name;
        ss::server_socket socket;
        bool tls_enabled;

        listener(ss::sstring name, ss::server_socket socket, bool tls_enabled)
          : name(std::move(name))
          , socket(std::move(socket))
          , tls_enabled(tls_enabled) {}
    };

    ss::future<> accept(listener&);
    ss::future<ss::stop_iteration>
    accept_finish(ss::sstring, ss::future<ss::accept_result>, bool);
    void
    print_exceptional_future(ss::future<>, const char*, ss::socket_address);
    ss::future<>
      apply_proto(ss::lw_shared_ptr<net::connection>, conn_quota::units);
    void setup_metrics();
    void setup_public_metrics();

    ss::logger& _log;
    ssx::semaphore _memory;
    std::vector<std::unique_ptr<listener>> _listeners;
    boost::intrusive::list<net::connection> _connections;
    ss::abort_source _as;
    ss::gate _accept_gate;
    ss::gate _conn_gate;
    hist_t _hist;
    std::unique_ptr<server_probe> _probe;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;

    std::optional<config_connection_rate_bindings> connection_rate_bindings;
    std::optional<connection_rate<>> _connection_rates;
};

} // namespace net
