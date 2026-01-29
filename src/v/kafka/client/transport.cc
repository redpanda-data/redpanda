/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/transport.h"

#include "bytes/iostream.h"
#include "kafka/client/logger.h"
#include "kafka/protocol/flex_versions.h"
#include "kafka/protocol/wire.h"
#include "net/connection.h"
#include "ssx/future-util.h"

namespace kafka::client {
namespace {
ss::future<correlation_id> read_correlation_id(ss::input_stream<char>& in) {
    auto buf = co_await read_iobuf_exactly(in, sizeof(correlation_id));
    protocol::decoder reader(std::move(buf));
    co_return correlation_id(reader.read_int32());
}
} // namespace
transport::transport(
  net::base_transport::configuration c,
  std::optional<ss::sstring> client_id) noexcept
  : net::base_transport(std::move(c), &kclog)
  , _client_id(std::move(client_id))
  , _logger(
      kclog,
      ssx::sformat(
        "{} - {}:{}",
        _client_id.value_or("redpanda-client"),
        server_address().host(),
        server_address().port())) {}

transport::transport(transport&& other) noexcept
  // WORKAROUND: https://github.com/llvm/llvm-project/issues/63202
  // NOLINTBEGIN(bugprone-use-after-move)
  : net::base_transport(std::move(other))
  , _dispatch_mutex("kafka::client::transport::dispatch_mutex")
  , _correlation(other._correlation)
  , _pending_requests(std::move(other._pending_requests))
  , _client_id(std::move(other._client_id))
  , _logger(std::move(other._logger)) {
    other._needs_stop = false; // we are moving, so we don't need to stop
    // NOLINTEND(bugprone-use-after-move)
}

transport::request_entry::request_entry(
  transport* parent_transport,
  correlation_id correlation,
  bool is_flexible,
  std::optional<model::timeout_clock::duration> timeout)
  : is_flexible(is_flexible) {
    if (timeout) {
        timeout_timer.set_callback([parent_transport, correlation] {
            parent_transport->on_timeout(correlation);
        });
        timeout_timer.arm(*timeout);
    }
}
void transport::request_entry::set_response(
  iobuf data, std::optional<tagged_fields> tags) {
    timeout_timer.cancel();
    response_promise.set_value(
      response_data{
        .tags = std::move(tags),
        .data = std::move(data),
      });
}
void transport::request_entry::set_error(errc ec) {
    timeout_timer.cancel();
    response_promise.set_value(ec);
}

ss::future<> transport::read_loop() {
    while (is_valid()) {
        auto reply_sz = co_await protocol::parse_size(in());

        if (!reply_sz) {
            // If we can't read the size, we are disconnected.
            co_return;
        }
        auto bytes_remaining = reply_sz.value();

        // TODO: add validation for correlation id here
        auto correlation_id = co_await read_correlation_id(in());
        bytes_remaining -= sizeof(correlation_id);
        auto it = _pending_requests.find(correlation_id);

        if (it == _pending_requests.end()) {
            vlog(
              _logger.trace,
              "No pending request for correlation_id: {}, most likely it "
              "timed "
              "out",
              correlation_id);
            continue;
        }
        auto entry = std::move(it->second);
        _pending_requests.erase(it);
        std::optional<tagged_fields> reply_tags;
        if (entry->is_flexible) {
            auto [tags, bytes_read] = co_await parse_tags(in());
            reply_tags = std::move(tags);
            bytes_remaining -= bytes_read;
        }
        vlog(
          _logger.trace,
          "reading response for correlation_id: {}, bytes_remaining: {}",
          correlation_id,
          bytes_remaining);
        auto reply_buffer = co_await read_iobuf_exactly(in(), bytes_remaining);

        entry->response_promise.set_value(
          response_data{
            .tags = std::move(reply_tags),
            .data = std::move(reply_buffer),
          });
    }
}

void transport::on_timeout(correlation_id correlation) {
    auto it = _pending_requests.find(correlation);
    if (it == _pending_requests.end()) {
        // do nothing, request already completed
        return;
    }
    auto entry = std::move(it->second);
    _pending_requests.erase(it);
    vlog(
      _logger.debug, "Request with correlation_id: {} timed out", correlation);
    entry->set_error(errc::timeout);
}

ss::future<> transport::stop() {
    vlog(_logger.trace, "Stopping client transport to: {}", server_address());
    _needs_stop = false;
    co_await base_transport::stop();
    co_await wait_input_shutdown();
}
using namespace std::chrono_literals;
constexpr std::chrono::seconds tcp_keepalive_idle = 360s;
constexpr std::chrono::seconds tcp_keepalive_interval = 120s;
constexpr unsigned int tcp_keepalive_probes = 10;

ss::future<>
transport::connect(model::timeout_clock::time_point connection_timeout) {
    vlog(_logger.trace, "Connecting to server: {}", server_address());
    _needs_stop = true;
    return base_transport::connect(connection_timeout).then([this] {
        // background
        ssx::spawn_with_gate(_dispatch_gate, [this] {
            set_keepalive_parameters(
              ss::net::tcp_keepalive_params{
                .idle = tcp_keepalive_idle,
                .interval = tcp_keepalive_interval,
                .count = tcp_keepalive_probes,
              });
            set_keepalive(true);
            return read_loop().then_wrapped([this](ss::future<> f) {
                fail_outstanding_futures();
                try {
                    f.get();
                } catch (...) {
                    auto e = std::current_exception();
                    if (net::is_disconnect_exception(e)) {
                        vlog(
                          _logger.info,
                          "Disconnected from server {}: {}",
                          server_address(),
                          e);
                    } else {
                        vlog(
                          _logger.error,
                          "Error reading from server {}: {}",
                          server_address(),
                          e);
                    }
                }
            });
        });
    });
}

void transport::fail_outstanding_futures() {
    shutdown();
    for (auto& [_, p] : _pending_requests) {
        p->set_error(errc::disconnected);
    }
    _pending_requests.clear();
}

void transport::write_header(
  protocol::encoder& wr,
  api_key key,
  api_version version,
  correlation_id correlation) {
    wr.write(int16_t(key()));
    wr.write(int16_t(version()));
    wr.write(int32_t(correlation));
    wr.write(_client_id);
    vassert(
      flex_versions::is_api_in_schema(key),
      "Attempted to send request to non-existent API: {}",
      key);
    if (flex_versions::is_flexible_request(key, version)) {
        /// Tags are unused by the client but to be protocol compliant
        /// with flex versions at least a 0 byte must be written
        wr.write_tags(tagged_fields{});
    }
}
} // namespace kafka::client
