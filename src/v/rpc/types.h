#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <vector>

namespace rpc {
class netbuf;

using clock_type = ss::lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = ss::timer<clock_type>;
static constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();

struct negotiation_frame {
    int8_t version = 0;
    /// \brief 0 - no compression
    ///        1 - zstd
    int8_t compression = 0;
};

/// \brief core struct for communications. sent with _each_ payload
struct [[gnu::packed]] header {
    /// \brief size of the payload _after_ this header
    ss::unaligned<uint32_t> size = 0;
    /// \brief used to find the method id on the server side
    ss::unaligned<uint32_t> meta = 0;
    /// \brief every client/tcp connection will need to match
    /// the ss::future<> that dispatched the method
    ss::unaligned<uint32_t> correlation_id = 0;
    /// \bitflags for payload
    ss::unaligned<uint32_t> bitflags = 0;
    /// \brief xxhash64
    ss::unaligned<uint64_t> checksum = 0;
};
static_assert(sizeof(header) == 24, "This is expensive. Expand gently");

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

} // namespace rpc
