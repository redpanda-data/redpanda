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

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "bytes/scattered_message.h"
#include "kafka/protocol/api_versions.h"
#include "kafka/protocol/flex_versions.h"
#include "kafka/protocol/fwd.h"
#include "kafka/protocol/wire.h"
#include "net/transport.h"
#include "ssx/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
namespace kafka::client {

class kafka_request_disconnected_exception : public std::runtime_error {
public:
    explicit kafka_request_disconnected_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

class kafka_request_timeout_exception : public std::runtime_error {
public:
    explicit kafka_request_timeout_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

/**
 * Kafka client transport. The transport supports sending multiple outstanding
 * requests to the server.
 * TODO:
 * - observability (metrics)
 * - limit memory usage ?
 * - support for flexible versions
 */
class transport : public net::base_transport {
public:
    enum class errc : int8_t {
        none,
        disconnected,
        timeout,
    };
    /**
     * The default timeout for requests sent to the Kafka broker.
     * This is used when the request does not specify a timeout.
     * The default is set to 60 seconds, which is a common timeout for Kafka
     * requests.
     */
    static constexpr std::chrono::milliseconds default_timeout
      = std::chrono::seconds(60);

    transport(
      net::base_transport::configuration c,
      std::optional<ss::sstring> client_id) noexcept;
    transport(const transport&) = delete;
    transport(transport&& other) noexcept;
    transport& operator=(const transport&) = delete;
    transport& operator=(transport&&) = delete;
    ~transport() override {
        vassert(
          _needs_stop == false, "Transport must be stopped before destruction");
    };

    const std::optional<ss::sstring>& client_id() const { return _client_id; }

    template<typename RequestT>
    requires(KafkaApi<typename RequestT::api_type>)
    ss::future<typename RequestT::api_type::response_type> dispatch(
      RequestT r,
      api_version version,
      std::optional<model::timeout_clock::duration> timeout = default_timeout) {
        return do_send_and_decode(std::move(r), version, timeout)
          .then(
            [this](
              checked<typename RequestT::api_type::response_type, errc> res) {
                if (res.has_error()) {
                    if (res.error() == errc::disconnected) {
                        throw kafka_request_disconnected_exception(
                          fmt::format(
                            "Broker {}:{} transport disconnected",
                            server_address().host(),
                            server_address().port()));
                    } else if (res.error() == errc::timeout) {
                        throw kafka_request_disconnected_exception(
                          fmt::format(
                            "Broker {}:{} request of type {} timed out",
                            server_address().host(),
                            server_address().port(),
                            RequestT::api_type::name));
                    }
                }

                return std::move(res.value());
            });
    }

    template<typename RequestT>
    requires(KafkaApi<typename RequestT::api_type>)
    ss::future<checked<typename RequestT::api_type::response_type, errc>>
    dispatch_errc(
      RequestT r,
      api_version version,
      std::optional<model::timeout_clock::duration> timeout = default_timeout) {
        return do_send_and_decode(std::move(r), version, timeout);
    }
    /**
     * Dispatches the request to the broker and returns the raw response
     * as an iobuf. This is useful for requests that have a non-standard
     * decoding rules f.e. API version request
     */
    template<typename RequestT>
    requires(KafkaApi<typename RequestT::api_type>)
    ss::future<iobuf> dispatch_request_raw_response(
      RequestT r,
      api_version version,
      std::optional<model::timeout_clock::duration> timeout = default_timeout) {
        return do_send(std::move(r), version, timeout)
          .then([this](checked<iobuf, errc> res) {
              if (res.has_error()) {
                  if (res.error() == errc::disconnected) {
                      throw kafka_request_disconnected_exception(
                        fmt::format(
                          "Broker {}:{} transport disconnected",
                          server_address().host(),
                          server_address().port()));
                  } else if (res.error() == errc::timeout) {
                      throw kafka_request_disconnected_exception(
                        fmt::format(
                          "Broker {}:{} request of type {} timed out",
                          server_address().host(),
                          server_address().port(),
                          RequestT::api_type::name));
                  }
              }

              return std::move(res.value());
          });
    }

    ss::future<> connect(
      model::timeout_clock::time_point connection_timeout
      = model::no_timeout) override;

    ss::future<> stop();

    void fail_outstanding_futures() override;

private:
    enum class reconnect_result_t : int8_t {
        connected,
        timed_out,
    };
    ss::future<reconnect_result_t> connect_with_retires(
      model::timeout_clock::time_point connection_timeout = model::no_timeout);

    struct response_data {
        std::optional<tagged_fields> tags;
        iobuf data;
    };

    struct request_entry {
        explicit request_entry(
          transport* parent_transport,
          correlation_id correlation,
          bool is_flexible,
          std::optional<model::timeout_clock::duration> timeout);
        void set_response(iobuf data, std::optional<tagged_fields> tags);
        void set_error(errc);

        ss::promise<checked<response_data, errc>> response_promise;
        bool is_flexible{false};
        ss::timer<model::timeout_clock> timeout_timer;
    };

    void write_header(
      protocol::encoder& wr,
      api_key key,
      api_version version,
      correlation_id correlation);

    ss::future<> read_loop();

    void on_timeout(correlation_id correlation);

    template<typename RequestT>
    ss::future<checked<iobuf, errc>> do_send(
      RequestT request,
      api_version version,
      std::optional<model::timeout_clock::duration> timeout) {
        using ret_t = checked<iobuf, errc>;
        // hold the mutex here before the message is written to the output
        // stream to ensure ordering or requests.
        auto u = co_await _dispatch_mutex.get_units();
        if (!is_valid() || _dispatch_gate.is_closed()) {
            co_return ret_t(errc::disconnected);
        }
        auto gate_holder = _dispatch_gate.hold();
        iobuf buf;
        auto placeholder = buf.reserve(sizeof(int32_t));
        auto start_size = buf.size_bytes();
        protocol::encoder wr(buf);
        auto key = RequestT::api_type::key;
        auto correlation = _correlation++;
        vlog(
          _logger.trace,
          "Dispatching '{}' request with version: {}, correlation: {}",
          RequestT::api_type::name,
          version,
          correlation);
        // encode request
        write_header(wr, RequestT::api_type::key, version, correlation);
        request.encode(wr, version);
        vassert(
          flex_versions::is_api_in_schema(key),
          "Attempted to send request to non-existent API: {}",
          key);

        /// KIP-511 bumps api_versions_request/response to 3 the first flex
        /// version for the API three however makes an exception that there will
        /// be no tags in the response header.
        const auto is_flexible = flex_versions::is_flexible_request(
                                   key, version)
                                 && key() != api_versions_api::key;

        // finalize by filling in the size prefix
        int32_t total_size = buf.size_bytes() - start_size;
        auto be_total_size = ss::cpu_to_be(total_size);
        auto* raw_size = reinterpret_cast<const char*>(&be_total_size);
        placeholder.write(raw_size, sizeof(be_total_size));

        auto [it, _] = _pending_requests.emplace(
          correlation,
          std::make_unique<request_entry>(
            this, correlation, is_flexible, timeout));
        auto response_future = it->second->response_promise.get_future();
        co_await out().write(iobuf_as_scattered(std::move(buf)));
        // return all units to the semaphore before flushing the output
        u.return_all();
        co_await out().flush();
        auto response_data = co_await std::move(response_future);
        if (response_data.has_error()) {
            co_return ret_t(response_data.error());
        }

        co_return std::move(response_data.value().data);
    }

    template<typename RequestT>
    ss::future<checked<typename RequestT::api_type::response_type, errc>>
    do_send_and_decode(
      RequestT request,
      api_version version,
      std::optional<model::timeout_clock::duration> timeout) {
        using ret_t = checked<typename RequestT::api_type::response_type, errc>;
        auto response_data = co_await do_send(
          std::move(request), version, timeout);
        if (response_data.has_error()) {
            co_return ret_t(response_data.error());
        }
        typename RequestT::api_type::response_type resp;
        resp.decode(std::move(response_data.value()), version);
        // TODO: If we need to handle tags, we can do it here.
        co_return ret_t(std::move(resp));
    }

    ssx::mutex _dispatch_mutex{"kafka::client::transport::dispatch_mutex"};
    bool _needs_stop = false;
    correlation_id _correlation{0};
    // We keep the entries sorted by correlation_id, as we are going to
    // implement the validation of correlation id in near future (Kafka protocol
    // defines the responses to be sent in the same order as requests were
    // sent). In this case the response should always match the first entry in
    // the map.
    absl::btree_map<correlation_id, std::unique_ptr<request_entry>>
      _pending_requests;
    std::optional<ss::sstring> _client_id;
    prefix_logger _logger;
};

} // namespace kafka::client
