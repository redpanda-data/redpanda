#include "redpanda/kafka/transport/server.h"

#include "prometheus/prometheus_sanitize.h"
#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/requests/response.h"
#include "utils/utf8.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/api.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

namespace kafka::transport {

static logger klog("kafka_server");

kafka_server::kafka_server(
  probe p,
  sharded<cluster::metadata_cache>& metadata_cache,
  kafka_server_config config,
  sharded<quota_manager>& quota_mgr,
  sharded<group_router_type>& group_router) noexcept
  : _probe(std::move(p))
  , _metadata_cache(metadata_cache)
  , _max_request_size(config.max_request_size)
  , _memory_available(_max_request_size)
  , _smp_group(std::move(config.smp_group))
  , _quota_mgr(quota_mgr)
  , _group_router(group_router)
  , _creds(
      config.credentials ? (*config.credentials).build_server_credentials()
                         : nullptr) {
    _probe.setup_metrics(_metrics);
}

future<> kafka_server::listen(socket_address server_addr, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    server_socket ss;
    try {
        if (!_creds) {
            ss = engine().listen(server_addr, lo);
            klog.debug(
              "Started plaintext Kafka API server listening at {}",
              server_addr);
        } else {
            ss = tls::listen(_creds, engine().listen(server_addr, lo));
            klog.debug(
              "Started secured Kafka API server listening at {}", server_addr);
        }
    } catch (...) {
        return make_exception_future<>(std::runtime_error(fmt::format(
          "KafkaServer error while listening on {} -> {}",
          server_addr,
          std::current_exception())));
    }
    _listeners.emplace_back(std::move(ss));

    (void)with_gate(_listeners_and_connections, [this, keepalive, server_addr] {
        return do_accepts(_listeners.size() - 1, keepalive);
    });
    return make_ready_future<>();
}

future<> kafka_server::do_accepts(int which, bool keepalive) {
    return repeat([this, which, keepalive] {
        return _listeners[which]
          .accept()
          .then_wrapped([this, which, keepalive](
                          future<accept_result> f_ar) mutable {
              if (_as.abort_requested()) {
                  f_ar.ignore_ready_future();
                  return stop_iteration::yes;
              }
              auto [fd, addr] = f_ar.get0();
              fd.set_nodelay(true);
              fd.set_keepalive(keepalive);
              auto conn = std::make_unique<connection>(
                *this, std::move(fd), std::move(addr));
              (void)with_gate(
                _listeners_and_connections,
                [this, conn = std::move(conn)]() mutable {
                    auto f = conn->process();
                    return f.then_wrapped([conn = std::move(conn)](
                                            future<>&& f) {
                        try {
                            f.get();
                        } catch (...) {
                            klog.debug(
                              "Connection error: {}", std::current_exception());
                        }
                    });
                });
              return stop_iteration::no;
          })
          .handle_exception([](std::exception_ptr ep) {
              klog.debug("Accept failed: {}", ep);
              return stop_iteration::no;
          });
    });
}

future<> kafka_server::stop() {
    klog.debug("Aborting {} listeners", _listeners.size());
    for (auto&& l : _listeners) {
        l.abort_accept();
    }
    klog.debug("Shutting down {} connections", _connections.size());
    _as.request_abort();
    for (auto&& con : _connections) {
        con.shutdown();
    }
    return _listeners_and_connections.close();
}

kafka_server::connection::connection(
  kafka_server& server, connected_socket&& fd, socket_address addr)
  : _server(server)
  , _fd(std::move(fd))
  , _addr(std::move(addr))
  , _read_buf(_fd.input())
  , _write_buf(_fd.output()) {
    _server._probe.connection_established();
    _server._connections.push_back(*this);
}

kafka_server::connection::~connection() {
    _server._probe.connection_closed();
    _server._connections.erase(_server._connections.iterator_to(*this));
}

void kafka_server::connection::shutdown() {
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
        klog.debug(
          "Failed to shutdown conneciton: {}", std::current_exception());
    }
}

// clang-format off
future<> kafka_server::connection::process() {
    return do_until(
      [this] {
          return _read_buf.eof() || _server._as.abort_requested();
      },
      [this] {
          return process_request().handle_exception([this](std::exception_ptr e) {
              klog.error("Failed to process request with {}", e);
          });
      })
      .then([this] {
          return _ready_to_respond.then([this] {
              return _write_buf.close();
          });
      });
}
// clang-format on

future<> kafka_server::connection::write_response(
  requests::response_ptr&& response,
  requests::correlation_type correlation_id) {
    sstring header(sstring::initialized_later(), sizeof(raw_response_header));
    auto* raw_header = reinterpret_cast<raw_response_header*>(header.begin());
    auto size = size_type(
      sizeof(requests::correlation_type) + response->buf().size_bytes());
    raw_header->size = cpu_to_be(size);
    raw_header->correlation_id = cpu_to_be(correlation_id);

    scattered_message<char> msg;
    msg.append(std::move(header));
    for (auto&& chunk : response->buf()) {
        msg.append_static(
          reinterpret_cast<const char*>(chunk.get()), chunk.size());
    }
    msg.on_delete([response = std::move(response)] {});
    auto msg_size = msg.size();
    return _write_buf.write(std::move(msg))
      .then([this] { return _write_buf.flush(); })
      .then([this, msg_size] { _server._probe.add_bytes_sent(msg_size); });
}

// The server guarantees that on a single TCP connection, requests will be
// processed in the order they are sent and responses will return in that order
// as well.
future<> kafka_server::connection::process_request() {
    return _read_buf.read_exactly(sizeof(size_type))
      .then([this](temporary_buffer<char> buf) {
          if (!buf) {
              // EOF
              return make_ready_future<>();
          }
          auto size = process_size(std::move(buf));
          // Allow for extra copies and bookkeeping
          auto mem_estimate = size * 2 + 8000;
          if (mem_estimate >= _server._max_request_size) {
              // TODO: Create error response using the specific API?
              throw std::runtime_error(fmt::format(
                "Request size is too large (size: {}; estimate: {}; allowed: "
                "{}",
                size,
                mem_estimate,
                _server._max_request_size));
          }
          auto fut = get_units(_server._memory_available, mem_estimate);
          if (_server._memory_available.waiters()) {
              _server._probe.waiting_for_available_memory();
          }
          return fut.then([this, size](semaphore_units<> units) {
              return read_header().then(
                [this, size, units = std::move(units)](
                  requests::request_header header) mutable {
                    // update the throughput tracker for this client using the
                    // size of the current request and return any computed delay
                    // to apply for quota throttling.
                    //
                    // note that when throttling is first applied the request is
                    // allowed to pass through and subsequent requests and
                    // delayed. this is a similar strategy used by kafka: the
                    // response is important because it allows clients to
                    // distinguish throttling delays from real delays. delays
                    // applied to subsequent messages allow backpressure to take
                    // affect.
                    auto delay
                      = _server._quota_mgr.local().record_tp_and_throttle(
                        header.client_id, size);

                    // apply the throttling delay, if any.
                    auto throttle_delay = delay.first_violation
                                            ? make_ready_future<>()
                                            : seastar::sleep(delay.duration);
                    return throttle_delay.then([this,
                                                size,
                                                header = std::move(header),
                                                units = std::move(units),
                                                &delay]() mutable {
                        auto remaining = size - sizeof(raw_request_header)
                                         - header.client_id_buffer.size();
                        return _buffer_reader.read_exactly(_read_buf, remaining)
                          .then(
                            [this,
                             header = std::move(header),
                             units = std::move(units),
                             delay = std::move(delay)](fragbuf buf) mutable {
                                auto ctx = requests::request_context(
                                  _server._metadata_cache,
                                  std::move(header),
                                  std::move(buf),
                                  delay.duration,
                                  _server._group_router.local());
                                _server._probe.serving_request();
                                do_process(std::move(ctx), std::move(units));
                            });
                    });
                });
          });
      });
}

void kafka_server::connection::do_process(
  requests::request_context&& ctx, semaphore_units<>&& units) {
    auto correlation = ctx.header().correlation_id;
    auto f = requests::process_request(std::move(ctx), _server._smp_group);
    auto ready = std::move(_ready_to_respond);
    _ready_to_respond = f.then_wrapped(
      [this, units = std::move(units), ready = std::move(ready), correlation](
        future<requests::response_ptr>&& f) mutable {
          try {
              auto r = f.get0();
              return ready
                .then([this, r = std::move(r), correlation]() mutable {
                    return write_response(std::move(r), correlation);
                })
                .then([this] { _server._probe.request_served(); });
          } catch (...) {
              _server._probe.request_processing_error();
              klog.debug(
                "Failed to process request: {}", std::current_exception());
              return std::move(ready);
          }
      });
}

size_t kafka_server::connection::process_size(temporary_buffer<char>&& buf) {
    if (_read_buf.eof()) {
        return 0;
    }
    auto* raw = unaligned_cast<const size_type*>(buf.get());
    size_type size = be_to_cpu(*raw);
    if (size < 0) {
        throw std::runtime_error(
          fmt::format("Invalid request size of {}", size));
    }
    return size_t(size);
}

future<requests::request_header> kafka_server::connection::read_header() {
    constexpr int16_t no_client_id = -1;
    return _read_buf.read_exactly(sizeof(raw_request_header))
      .then([this](temporary_buffer<char> buf) {
          if (_read_buf.eof()) {
              throw std::runtime_error(
                fmt::format("Unexpected EOF for request header"));
          }
          auto client_id_size = be_to_cpu(
            reinterpret_cast<const raw_request_header*>(buf.get())
              ->client_id_size);
          auto make_header =
            [buf = std::move(buf)]() -> requests::request_header {
              auto* raw_header = reinterpret_cast<const raw_request_header*>(
                buf.get());
              return requests::request_header{
                requests::api_key(net::ntoh(raw_header->api_key)),
                requests::api_version(net::ntoh(raw_header->api_version)),
                net::ntoh(raw_header->correlation_id)};
          };
          if (client_id_size == 0) {
              auto header = make_header();
              header.client_id = std::string_view();
              return make_ready_future<requests::request_header>(
                std::move(header));
          }
          if (client_id_size == no_client_id) {
              return make_ready_future<requests::request_header>(make_header());
          }
          return _read_buf.read_exactly(client_id_size)
            .then([this, make_header = std::move(make_header)](
                    temporary_buffer<char> buf) {
                if (_read_buf.eof()) {
                    throw std::runtime_error(
                      fmt::format("Unexpected EOF for client ID"));
                }
                auto header = make_header();
                header.client_id_buffer = std::move(buf);
                header.client_id = std::string_view(
                  header.client_id_buffer.get(),
                  header.client_id_buffer.size());
                validate_utf8(*header.client_id);
                return header;
            });
      });
}

} // namespace kafka::transport
