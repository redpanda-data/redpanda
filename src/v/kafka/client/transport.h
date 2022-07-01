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

#include "kafka/protocol/api_versions.h"
#include "kafka/protocol/fwd.h"
#include "kafka/server/flex_versions.h"
#include "kafka/server/protocol_utils.h"
#include "kafka/types.h"
#include "net/transport.h"
#include "seastarx.h"

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

class unsupported_request_exception : public std::runtime_error {
public:
    explicit unsupported_request_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

/**
 * \brief Kafka client.
 *
 * Restrictions:
 *  - don't dispatch concurrent requests.
 */
class transport : public net::base_transport {
private:
    /*
     * send a request message and process the reply. note that the kafka
     * protocol requires that replies be sent in the same order they are
     * received at the server. in a future version of this client we will also
     * want to do some form of request/response correlation so that multiple
     * requests can be in flight.
     */
    template<typename Func>
    auto send_recv(api_key key, api_version request_version, Func&& func) {
        // size prefixed buffer for request
        iobuf buf;
        auto ph = buf.reserve(sizeof(int32_t));
        auto start_size = buf.size_bytes();
        response_writer wr(buf);

        // encode request
        func(wr);

        vassert(
          flex_versions::is_api_in_schema(key),
          "Attempted to send request to non-existent API: {}",
          key);

        /// KIP-511 bumps api_versions_request/response to 3 the first flex
        /// version for the API three however makes an exception that there will
        /// be no tags in the response header.
        const auto is_flexible = flex_versions::is_flexible_request(
                                   key, request_version)
                                 && key() != api_versions_api::key;

        // finalize by filling in the size prefix
        int32_t total_size = buf.size_bytes() - start_size;
        auto be_total_size = ss::cpu_to_be(total_size);
        auto* raw_size = reinterpret_cast<const char*>(&be_total_size);
        ph.write(raw_size, sizeof(be_total_size));

        return _out.write(iobuf_as_scattered(std::move(buf)))
          .then([this, is_flexible] {
              return parse_size(_in).then([this, is_flexible](
                                            std::optional<size_t> sz) {
                  if (!sz) {
                      return ss::make_exception_future<iobuf>(
                        kafka_request_disconnected_exception(
                          "Request disconnected, no response recieved"));
                  }
                  auto size = sz.value();
                  return _in.read_exactly(sizeof(correlation_id))
                    .then([this, size, is_flexible](
                            ss::temporary_buffer<char>) {
                        if (is_flexible) {
                            return parse_tags(_in).then([size](auto p) {
                                auto& [_, bytes_read] = p;
                                return size
                                       - (sizeof(correlation_id) + bytes_read);
                            });
                        }
                        return ss::make_ready_future<size_t>(
                          size - sizeof(correlation_id));
                    })
                    .then([this](size_t remaining) {
                        // Finally, read the rest of the response from the
                        // buffer
                        return read_iobuf_exactly(_in, remaining);
                    });
              });
          });
    }

public:
    using net::base_transport::base_transport;

    /*
     * TODO: the concept here can be improved once we convert all of the request
     * types to encode their type relationships between api/request/response.
     */
    template<typename T>
    requires(KafkaApi<typename T::api_type>)
      ss::future<typename T::api_type::response_type> dispatch(
        T r, api_version request_version, api_version response_version) {
        return send_recv(
                 T::api_type::key,
                 request_version,
                 [this, request_version, r = std::move(r)](
                   response_writer& wr) mutable {
                     write_header(wr, T::api_type::key, request_version);
                     r.encode(wr, request_version);
                 })
          .then([response_version](iobuf buf) mutable {
              using response_type = typename T::api_type::response_type;
              response_type r;
              if constexpr (std::is_same_v<T, api_versions_request>) {
                  request_reader rdr(buf.copy());
                  if (
                    (kafka::error_code)rdr.read_int16()
                    == kafka::error_code::unsupported_version) {
                      response_version = api_version(0);
                  }
              }
              r.decode(std::move(buf), response_version);
              return ss::make_ready_future<response_type>(std::move(r));
          });
    }

    template<typename T>
    requires(KafkaApi<typename T::api_type>)
      ss::future<typename T::api_type::response_type> dispatch(
        T r, api_version ver) {
        return dispatch(std::move(r), ver, ver);
    }

    /*
     * Invokes dispatch with the given request type at the max supported level
     * between the client and server. If its not desired to perform automatic
     * version negotiation, then use the 2 or 3 argument version of dispatch
     */
    template<typename T>
    requires(KafkaApi<typename T::api_type>)
      ss::future<typename T::api_type::response_type> dispatch(T r) {
        if (!_max_versions) {
            /// If request fails, this throws
            _max_versions = co_await get_api_versions();
        }
        co_return co_await dispatch(
          std::move(r), negotiate_version(T::api_type::key));
    }

    void force_api_refresh() { _max_versions = std::nullopt; }

private:
    void write_header(response_writer& wr, api_key key, api_version version);

    api_version negotiate_version(api_key key) const;

    ss::future<std::vector<api_version>> get_api_versions();

private:
    correlation_id _correlation{0};
    std::optional<std::vector<api_version>> _max_versions;
};

} // namespace kafka::client
