/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "likely.h"
#include "outcome.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <type_traits>
#include <vector>

namespace rpc {
class netbuf;

using clock_type = ss::lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = ss::timer<clock_type>;
static constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();

enum class compression_type : uint8_t {
    none = 0,
    zstd,
    min = none,
    max = zstd,
};

struct negotiation_frame {
    int8_t version = 0;
    /// \brief 0 - no compression
    ///        1 - zstd
    compression_type compression = compression_type::none;
};

/// Response status, we use well known HTTP response codes for readability
enum class status : uint32_t {
    success = 200,
    method_not_found = 404,
    request_timeout = 408,
    server_error = 500,
};

/// \brief core struct for communications. sent with _each_ payload
struct header {
    /// \brief version is unused. always 0. can be used for bitflags as well
    uint8_t version{0};
    /// \brief everything below the checksum is hashed with crc32
    uint32_t header_checksum{0};
    /// \breif compression on the wire
    compression_type compression{0};
    /// \brief size of the payload
    uint32_t payload_size{0};
    /// \brief used to find the method id on the server side and propagate error
    /// to the client
    uint32_t meta{0};
    /// \brief every client/tcp connection will need to match
    /// the ss::future<> that dispatched the method
    uint32_t correlation_id{0};
    /// \brief xxhash64
    uint64_t payload_checksum{0};
};

static constexpr size_t size_of_rpc_header
  = sizeof(header::version)                            // 1
    + sizeof(header::header_checksum)                  // 4
    + sizeof(std::underlying_type_t<compression_type>) // 1
    + sizeof(header::payload_size)                     // 4
    + sizeof(header::meta)                             // 4
    + sizeof(header::correlation_id)                   // 4
    + sizeof(header::payload_checksum)                 // 8
  ;
static_assert(
  size_of_rpc_header == 26, "Be gentil when extending this header. expensive");

uint32_t checksum_header_only(const header& h);

struct client_opts {
    client_opts(
      clock_type::time_point client_send_timeout,
      compression_type ct,
      size_t compression_bytes) noexcept
      : timeout(client_send_timeout)
      , compression(ct)
      , min_compression_bytes(compression_bytes) {}

    explicit client_opts(clock_type::time_point client_send_timeout) noexcept
      : client_opts(client_send_timeout, compression_type::none, 1024) {}

    client_opts(const client_opts&) = delete;
    client_opts(client_opts&&) = default;

    client_opts& operator=(const client_opts&) = delete;
    client_opts& operator=(client_opts&&) = default;
    ~client_opts() noexcept = default;

    clock_type::time_point timeout;
    compression_type compression;
    size_t min_compression_bytes;
};

/// \brief used to pass environment context to the class
/// actually doing the work
class streaming_context {
public:
    streaming_context() noexcept = default;
    streaming_context(streaming_context&&) noexcept = default;
    streaming_context& operator=(streaming_context&&) noexcept = default;
    streaming_context(const streaming_context&) = delete;
    streaming_context& operator=(const streaming_context&) = delete;

    virtual ~streaming_context() noexcept = default;
    virtual ss::future<ss::semaphore_units<>> reserve_memory(size_t) = 0;
    virtual const header& get_header() const = 0;
    /// \brief because we parse the input as a _stream_ we need to signal
    /// to the dispatching thread that it can resume parsing for a new RPC
    virtual void signal_body_parse() = 0;

    /// \brief keep these units until destruction of context.
    /// usually, we want to keep the reservation of the memory size permanently
    /// until destruction of object without doing a .finally() and moving things
    /// around
    ss::future<> permanent_memory_reservation(size_t n) {
        return reserve_memory(n).then([this](ss::semaphore_units<> units) {
            _reservations.push_back(std::move(units));
        });
    }

private:
    std::vector<ss::semaphore_units<>> _reservations;
};

/// \brief most method implementations will be codegenerated
/// by $root/tools/rpcgen.py
using method = ss::noncopyable_function<ss::future<netbuf>(
  ss::input_stream<char>&, streaming_context&)>;

/// \brief used in returned types for client::send_typed() calls
template<typename T>
struct client_context {
    explicit client_context(header h)
      : hdr(std::move(h)) {}
    header hdr;
    T data;
};

template<typename T>
inline result<T> get_ctx_data(result<client_context<T>>&& ctx) {
    if (unlikely(!ctx)) {
        return result<T>(ctx.error());
    }
    return result<T>(std::move(ctx.value().data));
}

using metrics_disabled = ss::bool_class<struct metrics_disabled_tag>;

struct server_endpoint {
    ss::sstring name;
    ss::socket_address addr;

    server_endpoint(ss::sstring name, ss::socket_address addr)
      : name(std::move(name))
      , addr(addr) {}

    explicit server_endpoint(ss::socket_address addr)
      : server_endpoint("", addr) {}
};

struct server_configuration {
    std::vector<server_endpoint> addrs;
    int64_t max_service_memory_per_core;
    ss::shared_ptr<ss::tls::server_credentials> credentials;
    metrics_disabled disable_metrics = metrics_disabled::no;
    ss::sstring name;
    // we use the same default as seastar for load balancing algorithm
    ss::server_socket::load_balancing_algorithm load_balancing_algo
      = ss::server_socket::load_balancing_algorithm::connection_distribution;

    explicit server_configuration(ss::sstring n)
      : name(std::move(n)) {}
};
struct transport_configuration {
    ss::socket_address server_addr;
    /// \ brief The default timeout PER connection body. After we
    /// parse the header of the connection we need to
    /// make sure that we at some point receive some
    /// bytes or expire the (connection).
    duration_type recv_timeout = std::chrono::minutes(1);
    uint32_t max_queued_bytes = std::numeric_limits<uint32_t>::max();
    ss::shared_ptr<ss::tls::certificate_credentials> credentials;
    metrics_disabled disable_metrics = metrics_disabled::no;
};

std::ostream& operator<<(std::ostream&, const header&);
std::ostream& operator<<(std::ostream&, const server_endpoint&);
std::ostream& operator<<(std::ostream&, const server_configuration&);
std::ostream& operator<<(std::ostream&, const status&);
} // namespace rpc
