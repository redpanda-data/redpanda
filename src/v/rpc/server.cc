#include "rpc/server.h"

#include "likely.h"
#include "prometheus/prometheus_sanitize.h"
#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/types.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/reactor.hh>

#include <fmt/format.h>

namespace rpc {

server::server(server_configuration c)
  : cfg(std::move(c))
  , _memory(cfg.max_service_memory_per_core)
  , _creds(
      cfg.credentials ? (*cfg.credentials).build_server_credentials()
                      : nullptr) {}

server::~server() = default;

void server::start() {
    vassert(_proto, "must have a registered protocol before starting");
    if (!cfg.disable_metrics) {
        setup_metrics();
        _probe.setup_metrics(_metrics, cfg.name.c_str());
    }
    for (auto addr : cfg.addrs) {
        ss::server_socket ss;
        try {
            ss::listen_options lo;
            lo.reuse_address = true;
            if (!_creds) {
                ss = ss::engine().listen(addr, lo);
            } else {
                ss = ss::tls::listen(_creds, ss::engine().listen(addr, lo));
            }
        } catch (...) {
            throw std::runtime_error(fmt::format(
              "{} - Error attempting to listen on {}: {}",
              _proto->name(),
              addr,
              std::current_exception()));
        }
        auto& b = _listeners.emplace_back(
          std::make_unique<ss::server_socket>(std::move(ss)));
        ss::server_socket& ref = *b;
        // background
        (void)with_gate(_conn_gate, [this, &ref] { return accept(ref); });
    }
}

// inline to get more context
static inline void print_exceptional_future(
  server::protocol* proto, ss::future<> f, const char* ctx) {
    try {
        f.get();
    } catch (...) {
        vlog(
          rpclog.error,
          "{} - Error ( {} ): {}",
          proto->name(),
          ctx,
          std::current_exception());
    }
}
static ss::future<>
apply_proto(server::protocol* proto, server::resources&& rs) {
    auto conn = rs.conn;
    return proto->apply(std::move(rs))
      .then_wrapped([proto, conn](ss::future<> f) {
          print_exceptional_future(proto, std::move(f), "applying proto");
          vlog(rpclog.debug, "{} Closing: {}", proto->name(), conn->addr);
          return conn->shutdown().then_wrapped([proto](ss::future<> f) {
              print_exceptional_future(proto, std::move(f), "shutting down");
          });
      })
      .finally([conn] {});
}
ss::future<> server::accept(ss::server_socket& s) {
    return ss::repeat([this, &s]() mutable {
        return s.accept().then_wrapped(
          [this](ss::future<ss::accept_result> f_cs_sa) mutable {
              if (_as.abort_requested()) {
                  f_cs_sa.ignore_ready_future();
                  return ss::make_ready_future<ss::stop_iteration>(
                    ss::stop_iteration::yes);
              }
              auto [ar] = f_cs_sa.get();
              ar.connection.set_nodelay(true);
              ar.connection.set_keepalive(true);
              auto conn = ss::make_lw_shared<connection>(
                _connections,
                std::move(ar.connection),
                ar.remote_address,
                _probe);
              vlog(
                rpclog.trace, "Incoming connection from {}", ar.remote_address);
              if (_conn_gate.is_closed()) {
                  return conn->shutdown().then([] {
                      return ss::make_exception_future<ss::stop_iteration>(
                        ss::gate_closed_exception());
                  });
              }
              (void)with_gate(_conn_gate, [this, conn]() mutable {
                  return apply_proto(_proto.get(), resources(this, conn));
              });
              return ss::make_ready_future<ss::stop_iteration>(
                ss::stop_iteration::no);
          });
    });
} // namespace rpc

ss::future<> server::stop() {
    ss::sstring proto_name = _proto ? _proto->name() : "protocol not set";
    vlog(
      rpclog.info, "{} - Stopping {} listeners", proto_name, _listeners.size());
    for (auto&& l : _listeners) {
        l->abort_accept();
    }
    vlog(rpclog.debug, "{} - Service probes {}", proto_name, _probe);
    vlog(
      rpclog.info,
      "{} - Shutting down {} connections",
      proto_name,
      _connections.size());
    _as.request_abort();
    // close the connections and wait for all dispatches to finish
    for (auto& c : _connections) {
        c.shutdown_input();
    }
    return _conn_gate.close().then([this] {
        return seastar::do_for_each(
          _connections, [](connection& c) { return c.shutdown(); });
    });
}
void server::setup_metrics() {
    namespace sm = ss::metrics;
    if (!_proto) {
        return;
    }
    _metrics.add_group(
      prometheus_sanitize::metrics_name(cfg.name),
      {sm::make_total_bytes(
         "max_service_mem_bytes",
         [this] { return cfg.max_service_memory_per_core; },
         sm::description(
           fmt::format("{}: Maximum memory allowed for RPC", cfg.name))),
       sm::make_total_bytes(
         "consumed_mem_bytes",
         [this] { return cfg.max_service_memory_per_core - _memory.current(); },
         sm::description(
           fmt::format("{}: Memory consumed by request processing", cfg.name))),
       sm::make_histogram(
         "dispatch_handler_latency",
         [this] { return _hist.seastar_histogram_logform(); },
         sm::description(fmt::format("{}: Latency ", cfg.name)))});
}
} // namespace rpc
