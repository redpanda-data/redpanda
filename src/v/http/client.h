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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "http/chunk_encoding.h"
#include "http/iobuf_body.h"
#include "http/probe.h"
#include "net/transport.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>

#include <boost/beast/core.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/verb.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

namespace http {

using namespace std::chrono_literals;

using http_response
  = boost::beast::http::response<boost::beast::http::string_body>;
using http_request
  = boost::beast::http::request<boost::beast::http::string_body>;
using http_serializer
  = boost::beast::http::request_serializer<boost::beast::http::string_body>;

constexpr ss::lowres_clock::duration default_connect_timeout = 5s;

enum class reconnect_result_t {
    connected,
    timed_out,
};

enum class content_type {
    json,
};

/**
 * Return the HTTP content-type string for the given type.
 */
std::string_view content_type_string(content_type type);

// A fully drained, collected response. The iobuf contains the data from the
// response stream ref which has been read. Only suitable for short responses,
// should be avoided when reading large objects like segments.
struct downloaded_response {
    boost::beast::http::status status;
    iobuf body;
    // Response header fields, verbatim from the wire (e.g. Retry-After).
    boost::beast::http::fields headers;
};

// Interface to allow testing the http client with mocks
class abstract_client {
public:
    // Helper expected to fully drain the response stream and return it.
    // Intended for use with small objects such as JSON/XML responses which can
    // be easily held in memory
    virtual ss::future<downloaded_response> request_and_collect_response(
      boost::beast::http::request_header<>&& request,
      std::optional<iobuf> payload = std::nullopt,
      ss::lowres_clock::duration timeout = default_connect_timeout) = 0;

    virtual ss::future<> shutdown_and_stop() = 0;

    virtual ~abstract_client() = default;
};

/// Http client
class client
  : protected net::base_transport
  , public abstract_client {
public:
    using request_header = boost::beast::http::request_header<>;
    using response_header = boost::beast::http::response_header<>;
    using response_parser = boost::beast::http::response_parser<iobuf_body>;
    using field = boost::beast::http::field;
    using verb = boost::beast::http::verb;

    /// Thrown by the dial abort check between dialed addresses when
    /// shutdown_now() was requested; converted by get_connected into a
    /// graceful timed_out result.
    struct shutdown_requested_exception : ss::abort_requested_exception {};

    explicit client(const net::base_transport::configuration& cfg);
    client(
      const net::base_transport::configuration& cfg,
      const ss::abort_source& as);
    client(
      const net::base_transport::configuration& cfg,
      const ss::abort_source* as,
      ss::shared_ptr<client_probe> probe);
    client(
      const net::base_transport::configuration& cfg,
      const ss::abort_source* as,
      ss::shared_ptr<client_probe> probe,
      ss::lowres_clock::duration max_idle_time);

    // Request and response streams and in-flight continuations hold
    // back-pointers to the client.
    client(const client&) = delete;
    client& operator=(const client&) = delete;
    client(client&&) = delete;
    client& operator=(client&&) = delete;

    /// Stop must be called before destroying the client object.
    ss::future<> stop();
    using net::base_transport::shutdown;
    using net::base_transport::wait_input_shutdown;

    /**
     * Calling transport::shutdown will shutdown the underlying transport and
     * cause active connections and on-going connection attempts to fail.
     * However, the underlying transport can be reused by calling
     * transport::connect. This behavior is not sufficient to interrupt a
     * connection attempt in progress in `client::get_connected`. Instead, we
     * abort the in-flight dial with a typed exception that `get_connected`
     * converts into a graceful timed_out result instead of an abort error.
     *
     * Note that we avoid doing this by closing the _connect_gate because http
     * clients are stored in a pool and reused. Closing this gate is reserved
     * for shutting down the pool and all the connections, making reuse more
     * difficult to orchestrate correctly.
     */
    void shutdown_now() noexcept {
        _shutdown_as.request_abort_ex(shutdown_requested_exception{});
        shutdown();
    }

    // close the connect gate and fail_outstanding_futures which calls shutdown
    ss::future<> shutdown_and_stop() final { co_return co_await stop(); }

    /// Make a single connection attempt bounded by \p timeout, dialing
    /// every resolved address in sequence. Failures to connect surface
    /// as reconnect_result_t::timed_out; retrying is up to the caller.
    ss::future<reconnect_result_t>
    get_connected(ss::lowres_clock::duration timeout, prefix_logger ctxlog);

    void fail_outstanding_futures() noexcept override;

    // Response state machine
    class response_stream final
      : public ss::enable_shared_from_this<response_stream> {
    public:
        using verb = boost::beast::http::verb;
        /// C-tor can only be called by http_request
        explicit response_stream(client* client, verb v);
        ~response_stream() override;

        response_stream(response_stream&&) = delete;
        response_stream(const response_stream&) = delete;
        response_stream& operator=(const response_stream&) = delete;
        response_stream operator=(response_stream&&) = delete;

        /// Return true if the whole http payload is received and parsed
        bool is_done() const;

        /// Return true if the header parsing is done
        bool is_header_done() const { return _is_header_done; }

        /// Access response headers (should only be called if is_headers_done()
        /// == true)
        const response_header& get_headers() const;

        /// Prefetch HTTP headers. Read data from the socket until the header is
        /// received and parsed (is_headers_done = true).
        ///
        /// \return future that becomes ready when the header is received
        ss::future<> prefetch_headers();

        /// Recv new portion of the payload, this method should be called untill
        /// is_done returns false. It's possible to get an empty iobuf which
        /// should be ignored (deosn't mean EOF).
        /// The method doesn't return bytes that belong to HTTP header or chunk
        /// headers.
        ///
        /// \return future that becomes ready when the next portion of data is
        /// received
        ss::future<iobuf> recv_some();

        /// Returns input_stream that can be used to fetch response body.
        /// Can be used instead of 'recv_some'.
        ss::input_stream<char> as_input_stream();

    private:
        client* _client;
        prefix_logger& _ctxlog;
        response_parser _parser;
        bool _is_header_done{false};
        iobuf _buffer; /// store incomplete tail elements
        iobuf _prefetch;
        client_probe::subprobe _sprobe;
    };

    using response_stream_ref = ss::shared_ptr<response_stream>;

    // Request state machine
    // can only be created by the http_client
    class request_stream final
      : public ss::enable_shared_from_this<request_stream> {
    public:
        explicit request_stream(client* client, request_header&& hdr);

        request_stream(request_stream&&) = delete;
        request_stream(const request_stream&) = delete;
        request_stream& operator=(const request_stream&) = delete;
        request_stream operator=(request_stream&&) = delete;
        ~request_stream() override = default;

        /// Send data, if heders weren't sent they should be sent first
        /// followed by the data. BufferSeq is supposed to be an iobuf
        /// or temporary_buffer<char>.
        ss::future<> send_some(iobuf&& seq);
        ss::future<> send_some(ss::temporary_buffer<char>&& buf);

        // True if done, false otherwise
        bool is_done();

        // Wait until remaining data will be transmitted
        ss::future<> send_eof();

        /// Returns output_stream that can be used to send request headers and
        /// the body. Can be used instead of 'send_some' and 'send_eof'.
        ss::output_stream<char> as_output_stream();

    private:
        client* _client;
        prefix_logger& _ctxlog;
        http_request _request;
        http_serializer _serializer;
        chunked_encoder _chunk_encode;
        ss::gate _gate;

        enum {
            max_chunk_size = 32_KiB,
        };
    };

    using request_stream_ref = ss::shared_ptr<request_stream>;
    using request_response_t
      = std::tuple<request_stream_ref, response_stream_ref>;

    /// Utility function that executes request with the body and returns
    /// stream. Returned future becomes ready when the body is sent.
    /// Using the stream returned by the future client can pull response.
    ///
    /// \param header is a prepred request header
    /// \param input in an input stream that contains request body octets
    /// \param limits is a set of limitation for a query
    /// \returns response stream future
    ss::future<response_stream_ref> request(
      request_header&& header,
      ss::input_stream<char>& input,
      ss::lowres_clock::duration timeout = default_connect_timeout);
    ss::future<response_stream_ref> request(
      request_header&& header,
      ss::lowres_clock::duration timeout = default_connect_timeout);

    ss::future<downloaded_response> request_and_collect_response(
      request_header&& request,
      std::optional<iobuf> payload = std::nullopt,
      ss::lowres_clock::duration timeout = default_connect_timeout) final;

    /// Whether the client has a valid connection.
    using net::base_transport::is_valid;

    /// Server address.
    using net::base_transport::server_address;

    /**
     * Dispatch a request with the provided headers and body.
     *
     * Returns the response stream.
     */
    seastar::future<response_stream_ref> request(
      request_header header,
      iobuf body,
      ss::lowres_clock::duration timeout = default_connect_timeout);

    /**
     * Dispach a POST request to the provided path.
     *
     * @param path request target path
     * @param body body the request
     * @param type content type (e.g. json)
     * @return the response stream
     */
    seastar::future<response_stream_ref> post(
      std::string_view path,
      iobuf body,
      content_type type,
      ss::lowres_clock::duration timeout = default_connect_timeout);

private:
    template<class BufferSeq>
    static ss::future<> forward(client* client, BufferSeq&& seq);

    /// Unlike the base implementation which dials only the first
    /// resolved address, try all of them in sequence (e.g. both IPv4
    /// and IPv6 for dual-stack endpoints).
    ss::future<ss::connected_socket> dial(
      const net::unresolved_address& target,
      net::clock_type::time_point deadline) override;

    // Make http_request, if the transport is not yet connected it will connect
    // first otherwise the future will resolve immediately.
    ss::future<request_response_t>
    make_request(request_header&& header, ss::lowres_clock::duration timeout);

    /// Receive bytes from the remote endpoint
    ss::future<ss::temporary_buffer<char>> receive();
    /// Send bytes to the remote endpoint
    ss::future<> send(scattered_buffer bufs);

    /// Throw exception if _as is aborted
    void check() const;

    prefix_logger _ctxlog;
    /// Recreated at the start of every connection attempt;
    /// shutdown_now() aborts it.
    ss::abort_source _shutdown_as;
    std::string _host_with_port;
    ss::gate _connect_gate;
    const ss::abort_source* _as;
    ss::shared_ptr<http::client_probe> _probe;
    // Stores point in time when the last response was received
    // from the server.
    ss::lowres_clock::time_point _last_response{
      ss::lowres_clock::time_point::min()};
    ss::lowres_clock::duration _max_idle_time;
};

/// Utility function for producing a copy of the request header with some
/// fields redacted for logging.
///
/// \param original a request header with potentially sensitive header values
/// \return redacted header with sensitive values removed
client::request_header redacted_header(client::request_header original);

template<class BufferSeq>
inline ss::future<> client::forward(client* client, BufferSeq&& seq) {
    auto bufs = std::forward<BufferSeq>(seq).as_scattered();
    return client->send(std::move(bufs));
}

/// Helper to close an http client after a function has been called on it.
/// Modeled after ss::with_file
template<typename Func>
auto with_client(std::unique_ptr<client> cl, Func func) {
    static_assert(
      std::is_nothrow_move_constructible_v<Func>,
      "Func's move constructor must not throw");
    return ss::do_with(
      std::move(cl),
      [func = std::move(func)](std::unique_ptr<client>& cl) mutable {
          return ss::futurize_invoke(func, *cl).finally([&cl] {
              return cl->stop().then([&cl] {
                  cl->shutdown();
                  return ss::make_ready_future<>();
              });
          });
      });
}

const std::unordered_set<std::variant<boost::beast::http::field, std::string>>
redacted_fields();

ss::future<boost::beast::http::status>
status(client::response_stream_ref response);

template<typename T = iobuf>
requires std::same_as<T, iobuf> || std::same_as<T, void>
ss::future<T> drain(client::response_stream_ref response);

template<>
ss::future<iobuf> drain(client::response_stream_ref response);

template<>
ss::future<> drain(client::response_stream_ref response);

} // namespace http

template<>
struct fmt::formatter<http::client::request_header> {
    constexpr auto parse(fmt::format_parse_context& ctx)
      -> decltype(ctx.begin()) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const http::client::request_header& h, FormatContext& ctx) const
      -> decltype(ctx.out()) {
        auto redacted = http::redacted_header(h);
        return fmt::format_to(ctx.out(), "{}", fmt_streamed(redacted));
    }
};

template<>
struct fmt::formatter<http::client::response_header> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }

    auto format(const http::client::response_header& h, auto& ctx) const {
        return fmt::format_to(ctx.out(), "{}", fmt_streamed(h));
    }
};
