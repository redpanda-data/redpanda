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

#include "kafka/protocol/fwd.h"
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
    auto send_recv(Func&& func) {
        // size prefixed buffer for request
        iobuf buf;
        auto ph = buf.reserve(sizeof(int32_t));
        auto start_size = buf.size_bytes();
        response_writer wr(buf);

        // encode request
        func(wr);

        // finalize by filling in the size prefix
        int32_t total_size = buf.size_bytes() - start_size;
        auto be_total_size = ss::cpu_to_be(total_size);
        auto* raw_size = reinterpret_cast<const char*>(&be_total_size);
        ph.write(raw_size, sizeof(be_total_size));

        return _out.write(iobuf_as_scattered(std::move(buf))).then([this] {
            return parse_size(_in).then([this](std::optional<size_t> sz) {
                auto size = sz.value();
                return _in.read_exactly(sizeof(correlation_id))
                  .then([this, size](ss::temporary_buffer<char>) {
                      // drops the correlation id on the floor
                      auto remaining = size - sizeof(correlation_id);
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
    CONCEPT(requires(KafkaApi<typename T::api_type>))
    ss::future<typename T::api_type::response_type> dispatch(
      T r, api_version request_version, api_version response_version) {
        return send_recv([this, request_version, r = std::move(r)](
                           response_writer& wr) mutable {
                   write_header(wr, T::api_type::key, request_version);
                   r.encode(wr, request_version);
               })
          .then([response_version](iobuf buf) {
              using response_type = typename T::api_type::response_type;
              response_type r;
              r.decode(std::move(buf), response_version);
              return ss::make_ready_future<response_type>(std::move(r));
          });
    }

    template<typename T>
    CONCEPT(requires(KafkaApi<typename T::api_type>))
    ss::future<typename T::api_type::response_type> dispatch(
      T r, api_version ver) {
        return dispatch(std::move(r), ver, ver);
    }

    /*
     * Invokes dispatch with the given request type at the max supported level
     * of the redpanda kafka server.
     *
     * TODO: this will go away once the kafka client implements version
     * negotiation and can track on a per-broker basis the supported version
     * range.
     */
    template<typename T>
    CONCEPT(requires(KafkaApi<typename T::api_type>))
    ss::future<typename T::api_type::response_type> dispatch(T r) {
        using type = std::remove_reference_t<std::decay_t<T>>;
        if constexpr (std::is_same_v<type, offset_fetch_request>) {
            return dispatch(std::move(r), api_version(4));
        } else if constexpr (std::is_same_v<type, fetch_request>) {
            return dispatch(std::move(r), api_version(10));
        } else if constexpr (std::is_same_v<type, list_offsets_request>) {
            return dispatch(std::move(r), api_version(3));
        } else if constexpr (std::is_same_v<type, produce_request>) {
            return dispatch(std::move(r), api_version(7));
        } else if constexpr (std::is_same_v<type, offset_commit_request>) {
            return dispatch(std::move(r), api_version(7));
        } else if constexpr (std::is_same_v<type, describe_groups_request>) {
            return dispatch(std::move(r), api_version(2));
        } else if constexpr (std::is_same_v<type, heartbeat_request>) {
            return dispatch(std::move(r), api_version(3));
        } else if constexpr (std::is_same_v<type, join_group_request>) {
            return dispatch(std::move(r), api_version(4));
        } else if constexpr (std::is_same_v<type, sync_group_request>) {
            return dispatch(std::move(r), api_version(3));
        } else if constexpr (std::is_same_v<type, leave_group_request>) {
            return dispatch(std::move(r), api_version(2));
        } else if constexpr (std::is_same_v<type, metadata_request>) {
            return dispatch(std::move(r), api_version(7));
        } else if constexpr (std::is_same_v<type, find_coordinator_request>) {
            return dispatch(std::move(r), api_version(2));
        } else if constexpr (std::is_same_v<type, list_groups_request>) {
            return dispatch(std::move(r), api_version(2));
        } else if constexpr (std::is_same_v<type, create_topics_request>) {
            return dispatch(std::move(r), api_version(4));
        } else if constexpr (std::is_same_v<type, sasl_handshake_request>) {
            return dispatch(std::move(r), api_version(1));
        } else if constexpr (std::is_same_v<type, sasl_authenticate_request>) {
            return dispatch(std::move(r), api_version(1));
        }
    }

private:
    void write_header(response_writer& wr, api_key key, api_version version) {
        wr.write(int16_t(key()));
        wr.write(int16_t(version()));
        wr.write(int32_t(_correlation()));
        wr.write(std::string_view("test_client"));
        _correlation = _correlation + correlation_id(1);
    }

    correlation_id _correlation{0};
};

} // namespace kafka::client
