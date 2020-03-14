#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <type_traits>
#include <vector>

namespace rpc {
class netbuf;

using clock_type = ss::lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = ss::timer<clock_type>;
static constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();

enum class compression_type : uint8_t { none = 0, zstd };

struct negotiation_frame {
    int8_t version = 0;
    /// \brief 0 - no compression
    ///        1 - zstd
    compression_type compression = compression_type::none;
};

/// \brief core struct for communications. sent with _each_ payload
struct header {
    /// \brief version is unused. always 0. can be used for bitflags as well
    uint8_t version{0};
    /// \brief everything below the checksum is hashed with crc-16-ccitt
    uint16_t header_checksum{0};
    /// \breif compression on the wire
    compression_type compression{0};
    /// \brief size of the payload
    uint32_t payload_size{0};
    /// \brief used to find the method id on the server side
    uint32_t meta{0};
    /// \brief every client/tcp connection will need to match
    /// the ss::future<> that dispatched the method
    uint32_t correlation_id{0};
    /// \brief xxhash64
    uint64_t payload_checksum{0};
};

static constexpr size_t size_of_rpc_header
  = sizeof(header::version)                            // 1
    + sizeof(header::header_checksum)                  // 2
    + sizeof(std::underlying_type_t<compression_type>) // 1
    + sizeof(header::payload_size)                     // 4
    + sizeof(header::meta)                             // 4
    + sizeof(header::correlation_id)                   // 4
    + sizeof(header::payload_checksum)                 // 8
  ;
static_assert(
  size_of_rpc_header == 24, "Be gentil when extending this header. expensive");

uint16_t checksum_header_only(const header& h);

/// \brief used to pass environment context to the class
/// actually doing the work
struct streaming_context {
    virtual ~streaming_context() noexcept = default;
    virtual ss::future<ss::semaphore_units<>> reserve_memory(size_t) = 0;
    virtual const header& get_header() const = 0;
    /// \brief because we parse the input as a _stream_ we need to signal
    /// to the dispatching thread that it can resume parsing for a new RPC
    virtual void signal_body_parse() = 0;
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

using metrics_disabled = ss::bool_class<struct metrics_disabled_tag>;

struct server_configuration {
    std::vector<ss::socket_address> addrs;
    int64_t max_service_memory_per_core;
    std::optional<ss::tls::credentials_builder> credentials;
    metrics_disabled disable_metrics = metrics_disabled::no;
};
struct transport_configuration {
    ss::socket_address server_addr;
    /// \ brief The default timeout PER connection body. After we
    /// parse the header of the connection we need to
    /// make sure that we at some point receive some
    /// bytes or expire the (connection).
    duration_type recv_timeout = std::chrono::minutes(1);
    uint32_t max_queued_bytes = std::numeric_limits<uint32_t>::max();
    std::optional<ss::tls::credentials_builder> credentials;
    metrics_disabled disable_metrics = metrics_disabled::no;
};

std::ostream& operator<<(std::ostream&, const header&);
std::ostream& operator<<(std::ostream&, const server_configuration&);
} // namespace rpc
