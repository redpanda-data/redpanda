#include "rpc/server.h"

#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/types.h"

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
            //_s._probe.waiting_for_available_memory();
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
  , _memory(cfg.max_service_memory_per_core) {
}
server::~server() {
}
void server::start() {
    for (auto addr : cfg.addrs) {
        server_socket ss;
        try {
            listen_options lo;
            lo.reuse_address = true;
            ss = engine().listen(addr, lo);
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
                std::move(ar.remote_address));
              (void)with_gate(_conn_gate, [this, conn]() mutable {
                  return continous_method_dispath(conn).then_wrapped(
                    [](future<>&& f) {
                        try {
                            f.get();
                        } catch (...) {
                            rpclog().info(
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
                    // likely connection closed
                    rpclog().info(
                      "could not parse header from client: {}", conn->addr);
                    return make_ready_future<>();
                }
                return dispatch_method_once(std::move(h.value()), conn);
            });
      });
}

future<>
server::dispatch_method_once(header h, lw_shared_ptr<connection> conn) {
    const auto method_id = h.meta;
    auto ctx = make_lw_shared<server_context_impl>(*this, std::move(h));
    auto it = std::find_if(
      _services.begin(),
      _services.end(),
      [method_id](std::unique_ptr<service>& srvc) {
          return srvc->method_from_id(method_id) != nullptr;
      });
    if (__builtin_expect(it == _services.end(), false)) {
        throw std::runtime_error(
          fmt::format("received invalid rpc request: {}", h));
    }
    auto fut = ctx->pr.get_future();
    method* m = it->get()->method_from_id(method_id);
    // background!
    (void)(*m)(conn->input(), *ctx)
      .then([ctx, conn](netbuf n) mutable {
          n.set_correlation_id(ctx->get_header().correlation_id);
          auto view = n.scattered_view();
          view.on_delete([n = std::move(n)] {});
          return conn->output().write(std::move(view)).then([conn]() mutable {
              return conn->output().flush();
          });
      })
      .finally([conn] {});
    return fut;
}
future<> server::stop() {
    rpclog().info("Stopping {} listeners", _listeners.size());
    for (auto&& l : _listeners) {
        l.abort_accept();
    }
    rpclog().info("Shutting down {} connections", _connections.size());
    _as.request_abort();
    for (auto& conn : _connections) {
        conn.shutdown();
    }
    return _conn_gate.close();
}
} // namespace rpc
