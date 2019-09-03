#include "rpc/server.h"

#include "prometheus/prometheus_sanitize.h"
#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/types.h"

#include <seastar/core/metrics.hh>

#include <fmt/format.h>

namespace rpc {
struct server_context_impl final : streaming_context {
    server_context_impl(server& s, header h)
      : _s(std::ref(s))
      , _h(std::move(h)) {
    }
    future<semaphore_units<>> reserve_memory(size_t ask) final {
        auto fut = get_units(_s.get()._memory, ask);
        if (_s.get()._memory.waiters()) {
            _s.get()._probe.waiting_for_available_memory();
        }
        return fut;
    }
    const header& get_header() const final {
        return _h;
    }
    void signal_body_parse() final {
        pr.set_value();
    }
    std::reference_wrapper<server> _s;
    header _h;
    promise<> pr;
};

server::server(server_configuration c)
  : cfg(std::move(c))
  , _memory(cfg.max_service_memory_per_core)
  , _creds(
      cfg.credentials ? (*cfg.credentials).build_server_credentials()
                      : nullptr) {
    setup_metrics();
}

server::~server() {
}

void server::start() {
    for (auto addr : cfg.addrs) {
        server_socket ss;
        try {
            listen_options lo;
            lo.reuse_address = true;
            if (!_creds) {
                ss = engine().listen(addr, lo);
            } else {
                ss = tls::listen(_creds, engine().listen(addr, lo));
            }
        } catch (...) {
            throw std::runtime_error(fmt::format(
              "Error attempting to listen on {}: {}",
              addr,
              std::current_exception()));
        }
        _listeners.emplace_back(std::move(ss));
        server_socket& ref = _listeners.back();
        // background
        (void)with_gate(_conn_gate, [this, &ref] { return accept(ref); });
    }
}

future<> server::accept(server_socket& s) {
    return repeat([this, &s]() mutable {
        return s.accept().then_wrapped(
          [this](future<accept_result> f_cs_sa) mutable {
              if (_as.abort_requested()) {
                  f_cs_sa.ignore_ready_future();
                  return stop_iteration::yes;
              }
              auto [ar] = f_cs_sa.get();
              ar.connection.set_nodelay(true);
              ar.connection.set_keepalive(true);
              auto conn = make_lw_shared<connection>(
                _connections,
                std::move(ar.connection),
                std::move(ar.remote_address),
                _probe);
              (void)with_gate(_conn_gate, [this, conn]() mutable {
                  return continous_method_dispath(conn).then_wrapped(
                    [conn](future<>&& f) {
                        rpclog().debug("closing client: {}", conn->addr);
                        conn->shutdown();
                        try {
                            f.get();
                        } catch (...) {
                            rpclog().error(
                              "Error dispatching method: {}",
                              std::current_exception());
                        }
                    });
              });
              return stop_iteration::no;
          });
    });
}

future<> server::continous_method_dispath(lw_shared_ptr<connection> conn) {
    return do_until(
      [this, conn] { return conn->input().eof() || _as.abort_requested(); },
      [this, conn] {
          return parse_header(conn->input())
            .then([this, conn](std::optional<header> h) {
                if (!h) {
                    rpclog().debug(
                      "could not parse header from client: {}", conn->addr);
                    _probe.header_corrupted();
                    return make_ready_future<>();
                }
                return dispatch_method_once(std::move(h.value()), conn);
            });
      });
}

future<>
server::dispatch_method_once(header h, lw_shared_ptr<connection> conn) {
    const auto method_id = h.meta;
    constexpr size_t header_size = sizeof(header);
    auto ctx = make_lw_shared<server_context_impl>(*this, std::move(h));
    auto it = std::find_if(
      _services.begin(),
      _services.end(),
      [method_id](std::unique_ptr<service>& srvc) {
          return srvc->method_from_id(method_id) != nullptr;
      });
    if (__builtin_expect(it == _services.end(), false)) {
        _probe.method_not_found();
        throw std::runtime_error(
          fmt::format("received invalid rpc request: {}", h));
    }
    auto fut = ctx->pr.get_future();
    method* m = it->get()->method_from_id(method_id);
    _probe.add_bytes_recieved(header_size + h.size);
    // background!
    (void)(*m)(conn->input(), *ctx)
      .then([ctx, conn, m = _hist.auto_measure()](netbuf n) mutable {
          n.set_correlation_id(ctx->get_header().correlation_id);
          auto view = n.scattered_view();
          view.on_delete([n = std::move(n)] {});
          return conn->write(std::move(view)).finally([m = std::move(m)] {});
      })
      .finally([& p = _probe, conn] { p.request_completed(); });
    return fut;
}
future<> server::stop() {
    rpclog().info("Stopping {} listeners", _listeners.size());
    for (auto&& l : _listeners) {
        l.abort_accept();
    }
    rpclog().debug("Service probes {}", _probe);
    rpclog().info("Shutting down {} connections", _connections.size());
    _as.request_abort();
    // dispatch the gate first, wait for all connections to drain
    return _conn_gate.close().then([this] {
        for (auto& c : _connections) {
            c.shutdown();
        }
    });
}
void server::setup_metrics() {
    namespace sm = metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("rpc::"),
      {sm::make_gauge(
         "services",
         [this] { return _services.size(); },
         sm::description("Number of registered services")),
       sm::make_gauge(
         "max_service_mem",
         [this] { return cfg.max_service_memory_per_core; },
         sm::description("Maximum amount of memory used by service per core")),
       sm::make_gauge(
         "consumed_mem",
         [this] { return cfg.max_service_memory_per_core - _memory.current(); },
         sm::description("Amount of memory consumed for requests processing")),
       sm::make_gauge(
         "active_connections",
         [this] { return _probe.get_connections(); },
         sm::description("Currently active connections")),
       sm::make_gauge(
         "connects",
         [this] { return _probe.get_connects(); },
         sm::description("Number of accepted connections")),
       sm::make_derive(
         "connection_close_errors",
         [this] { return _probe.get_connection_close_errors(); },
         sm::description("Number of errors when shutting down the connection")),
       sm::make_derive(
         "requests_completed",
         [this] { return _probe.get_requests_completed(); },
         sm::description("Number of successfully served requests")),
       sm::make_derive(
         "received_bytes",
         [this] { return _probe.get_in_bytes(); },
         sm::description(
           "Number of bytes received from the clients in valid requests")),
       sm::make_derive(
         "sent_bytes",
         [this] { return _probe.get_out_bytes(); },
         sm::description("Number of bytes sent to clients")),
       sm::make_derive(
         "method_not_found_errors",
         [this] { return _probe.get_method_not_found_errors(); },
         sm::description("Number of requests with not available RPC method")),
       sm::make_derive(
         "corrupted_headers",
         [this] { return _probe.get_corrupted_headers(); },
         sm::description("Number of requests with corrupted headeres")),
       sm::make_derive(
         "bad_requests",
         [this] { return _probe.get_bad_requests(); },
         sm::description("Total number of all bad requests")),
       sm::make_derive(
         "requests_blocked_memory",
         [this] { return _probe.get_requests_blocked_memory(); },
         sm::description(
           "Number of requests that have to"
           "wait for processing beacause of insufficient memory")),
       sm::make_histogram(
         "dispatch_handler_latency",
         [this] { return _hist.seastar_histogram_logform(); },
         sm::description("Latency of service handler dispatch"))});
}
} // namespace rpc
