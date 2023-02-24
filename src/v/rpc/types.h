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

#include "bytes/iobuf.h"
#include "likely.h"
#include "net/types.h"
#include "net/unresolved_address.h"
#include "outcome.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "utils/hdr_hist.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <type_traits>
#include <vector>

using namespace std::chrono_literals;

namespace rpc {

using clock_type = net::clock_type;
using duration_type = typename clock_type::duration;
using timer_type = ss::timer<clock_type>;
static constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();
static constexpr clock_type::duration max_duration
  = clock_type::duration::max();

template<typename T>
concept RpcDurationOrPoint
  = (std::same_as<T, clock_type::time_point> || std::convertible_to<T, clock_type::duration>);

/**
 * @brief Specifies a timeout, including both the start point and the timeout
 * duration, which is more useful than a single time point when it comes to
 * diagnosing timeouts.
 *
 */
struct timeout_spec {
    /**
     * Constant indiciating specifying no timeout.
     * timeout_at() will return time_point::max(), i.e., the furthest possible
     * point in the future.
     */
    const static timeout_spec none;

    constexpr timeout_spec(
      clock_type::time_point timeout_point, clock_type::duration timeout_period)
      : timeout_point{timeout_point}
      , timeout_period{timeout_period} {}

    /**
     * The moment in time after which the timeout should occur.
     */
    clock_type::time_point timeout_point;

    /**
     * The period associated with this timeout. I.e., the period after some
     * starting point after which the timeout should occur. The timeout period
     * runs from start_point to timeout_point, and so start_point may be
     * calculated as timeout_point - timeout_period.
     */
    clock_type::duration timeout_period;

    /**
     * The point in time after which a timeout should trigger.
     */
    constexpr clock_type::time_point timeout_at() const {
        return timeout_point;
    }

    /**
     * @brief True if the represented timeout has timed out based on the current
     * time.
     */
    bool has_timed_out() const { return clock_type::now() > timeout_at(); }

    /**
     * @brief Create a timeout specification starting now with the given timeout
     * period.
     *
     * @param timeout_period the duration after which a timeout should occur
     * (starting from now)
     */
    static constexpr timeout_spec
    from_now(clock_type::duration timeout_period) {
        return timeout_period == max_duration
                 ? none
                 : timeout_spec{
                   clock_type::now() + timeout_period, timeout_period};
    }

    /**
     * @brief Create a timeout from a time_point.
     *
     * This assumes that the timeout duration was timeout_point - now(),
     * essentially taking a guess at the original duration, assuming the
     * spec is created shortly after the original calculation.
     *
     * It is better to use one of the other constructors with the true
     * start/end point and duration, but this is provided as a transitional API
     * for cases where
     * we are only passing the timeout point and it is too difficult to
     * thread a timeout_spec through everywhere.
     *
     * @param timeout_point the point at which the timeout should occur
     * @return timeout_spec
     */
    static constexpr timeout_spec
    from_point(clock_type::time_point timeout_point) {
        if (timeout_point == rpc::no_timeout) {
            return none;
        }
        return {timeout_point, timeout_point - clock_type::now()};
    }

    /**
     * @brief Accepts either a time_point or duration and constructs the timeout
     * in the corresponding way referenced to now().
     */
    static constexpr timeout_spec
    from_either(RpcDurationOrPoint auto point_or_duration) {
        if constexpr (std::is_same<
                        decltype(point_or_duration),
                        clock_type::time_point>::value) {
            return from_point(point_or_duration);
        } else {
            return from_now(point_or_duration);
        }
    }
};

inline constexpr timeout_spec timeout_spec::none = {
  rpc::no_timeout, max_duration};

using connection_cache_label
  = named_type<ss::sstring, struct connection_cache_label_tag>;

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
    version_not_supported = 505,

    // NOTE: Redpanda versions <= 22.3.x won't properly parse error codes they
    // don't know about; error codes below should be used only if
    // feature::rpc_transport_unknown_errc is active.
    service_unavailable = 503,
};

enum class transport_version : uint8_t {
    /*
     * the first version used by rpc simple protocol. at this version level
     * clients and servers (1) assume adl encoding, (2) ignore the version when
     * handling a request, and (3) always respond with version 0.
     *
     * Since Redpanda 23.2, we no longer speak this format
     */
    v0 = 0,

    /*
     * starting with version v1 clients and servers no longer ignore the
     * version. v1 indicates adl encoding.
     *
     * Since Redpanda 23.2, we no longer speak this format
     */
    v1 = 1,

    /**
     * v2 is exactly like v1, but using serde instead of ADL for serialization
     */
    v2 = 2,

    max_supported = v2,
    min_supported = v2,

    /*
     * unsupported is a convenience name used in tests to construct a message
     * with an unsupported version. the bits should not be considered reserved.
     */
    unsupported = std::numeric_limits<uint8_t>::max()
};

/// \brief core struct for communications. sent with _each_ payload
struct header {
    /// \brief version is unused. always 0. can be used for bitflags as well
    transport_version version{transport_version::v0};
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

    friend std::ostream& operator<<(std::ostream&, const header&);
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
    using resource_units_t
      = ss::foreign_ptr<ss::lw_shared_ptr<std::vector<ssx::semaphore_units>>>;

    // Sentinel used to indicate that a given time point has not been set.
    static constexpr clock_type::time_point unset
      = clock_type::time_point::min();

    /**
     * @brief Construct a new transport client options object.
     *
     * @param timeout specifies the timeout in use: specifically we will time
     * out after timeout.timeout_point().
     * @param ct the compression type to use
     * @param compression_bytes the compression threshold: smaller messages are
     * not compressed
     */
    explicit client_opts(
      timeout_spec timeout,
      compression_type ct = compression_type::none,
      size_t compression_bytes = 1024,
      resource_units_t resource_u = nullptr) noexcept
      : timeout(timeout)
      , compression(ct)
      , min_compression_bytes(compression_bytes)
      , resource_units(std::move(resource_u)) {}

    explicit client_opts(
      clock_type::time_point client_send_timeout_point) noexcept
      : client_opts(timeout_spec::from_point(client_send_timeout_point)) {}

    explicit client_opts(
      clock_type::duration client_send_timeout_duration) noexcept
      : client_opts(timeout_spec::from_now(client_send_timeout_duration)) {}

    client_opts(const client_opts&) = delete;
    client_opts(client_opts&&) = default;

    client_opts& operator=(const client_opts&) = delete;
    client_opts& operator=(client_opts&&) = default;
    ~client_opts() noexcept = default;

    timeout_spec timeout;
    compression_type compression;
    size_t min_compression_bytes;
    /**
     * Resource protecting semaphore units, those units will be relased after
     * data are sent over the wire and send buffer is released. May be helpful
     * to control caller resources.
     */
    resource_units_t resource_units;
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
    virtual ss::future<ssx::semaphore_units> reserve_memory(size_t) = 0;
    virtual const header& get_header() const = 0;
    /// \brief because we parse the input as a _stream_ we need to signal
    /// to the dispatching thread that it can resume parsing for a new RPC
    virtual void signal_body_parse() = 0;
    virtual void body_parse_exception(std::exception_ptr) = 0;

    /// \brief keep these units until destruction of context.
    /// usually, we want to keep the reservation of the memory size permanently
    /// until destruction of object without doing a .finally() and moving things
    /// around
    ss::future<> permanent_memory_reservation(size_t n) {
        return reserve_memory(n).then([this](ssx::semaphore_units units) {
            _reservations.push_back(std::move(units));
        });
    }

private:
    std::vector<ssx::semaphore_units> _reservations;
};

/**
 * @brief Bundles the method_id and assoicated method name.
 */
struct method_info {
    /**
     * The name of this method, which should be used only for logging and other
     * diagnosis, and should not be assumed unique or unchanged across commits.
     * The pointer-to string must have indefinite lifetime (e.g., a string
     * literal is probably the way to go).
     */
    const char* name;

    /**
     * @brief The unique 32-bit integer representing this rpc.
     */
    uint32_t id;
};

class netbuf {
public:
    /// \brief used to send the bytes down the wire
    /// we re-compute the header-checksum on every call
    ss::future<ss::scattered_message<char>> as_scattered() &&;

    void set_status(rpc::status);
    void set_correlation_id(uint32_t);
    void set_compression(rpc::compression_type c);
    void set_service_method(method_info);
    void set_min_compression_bytes(size_t);
    void set_version(transport_version v) { _hdr.version = v; }
    iobuf& buffer();

    /**
     * @brief Return the service method name, to be used only for diagnosis and
     * logging.
     */
    const char* name() const { return _name; }

    /**
     * @brief Get the correlation ID, if set or 0 otherwise.
     */
    uint32_t correlation_id() const { return _hdr.correlation_id; }

private:
    const char* _name = nullptr;
    size_t _min_compression_bytes{1024};
    header _hdr;
    iobuf _out;
};

inline iobuf& netbuf::buffer() { return _out; }
inline void netbuf::set_compression(rpc::compression_type c) {
    vassert(
      c >= compression_type::min && c <= compression_type::max,
      "invalid compression type: {}",
      int(c));
    _hdr.compression = c;
}
inline void netbuf::set_status(rpc::status st) {
    _hdr.meta = std::underlying_type_t<rpc::status>(st);
}
inline void netbuf::set_correlation_id(uint32_t x) { _hdr.correlation_id = x; }
inline void netbuf::set_service_method(method_info info) {
    _name = info.name;
    _hdr.meta = info.id;
}
inline void netbuf::set_min_compression_bytes(size_t min) {
    _min_compression_bytes = min;
}

class method_probes {
public:
    hdr_hist& latency_hist() { return _latency_hist; }
    const hdr_hist& latency_hist() const { return _latency_hist; }

private:
    // roughly 2024 bytes
    hdr_hist _latency_hist{120s, 1ms};
};

/// \brief most method implementations will be codegenerated
/// by $root/tools/rpcgen.py
struct method {
    using handler = ss::noncopyable_function<ss::future<netbuf>(
      ss::input_stream<char>&, streaming_context&)>;

    handler handle;
    method_probes probes;

    explicit method(handler h)
      : handle(std::move(h)) {}
};

/// \brief used in returned types for client::send_typed() calls
template<typename T>
struct client_context {
    client_context(header h, T data)
      : hdr(h)
      , data(std::move(data)) {}

    header hdr;
    T data;
};

/*
 * wrapper to hold context associated with a response that can be inspected even
 * in cases where there is no valid response, such as recoverable errors.
 */
template<typename T>
struct result_context {
    transport_version version;
    result<client_context<T>> ctx;
};

template<typename T>
inline result<T> get_ctx_data(result<client_context<T>>&& ctx) {
    if (unlikely(!ctx)) {
        return result<T>(ctx.error());
    }
    return result<T>(std::move(ctx.value().data));
}

struct transport_configuration {
    net::unresolved_address server_addr;
    /// \ brief The default timeout PER connection body. After we
    /// parse the header of the connection we need to
    /// make sure that we at some point receive some
    /// bytes or expire the (connection).
    duration_type recv_timeout = std::chrono::minutes(1);
    uint32_t max_queued_bytes = std::numeric_limits<uint32_t>::max();
    ss::shared_ptr<ss::tls::certificate_credentials> credentials;
    net::metrics_disabled disable_metrics = net::metrics_disabled::no;
    transport_version version{transport_version::v2};
};

std::ostream& operator<<(std::ostream&, const status&);
std::ostream& operator<<(std::ostream&, transport_version);
} // namespace rpc
