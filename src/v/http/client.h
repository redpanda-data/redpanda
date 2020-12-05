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

#include "bytes/details/io_fragment.h"
#include "bytes/iobuf.h"
#include "http/chunk_encoding.h"
#include "http/iobuf_body.h"
#include "http/logger.h"
#include "rpc/transport.h"
#include "rpc/types.h"
#include "seastarx.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/beast/core.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/optional/optional.hpp>
#include <boost/system/system_error.hpp>

#include <exception>
#include <stdexcept>
#include <string>

namespace http {

using http_response
  = boost::beast::http::response<boost::beast::http::string_body>;
using http_request
  = boost::beast::http::request<boost::beast::http::string_body>;
using http_serializer
  = boost::beast::http::request_serializer<boost::beast::http::string_body>;

/// Http client
class client : protected rpc::base_transport {
    enum {
        protocol_version = 11,
    };

public:
    using request_header = boost::beast::http::request_header<>;
    using response_header = boost::beast::http::response_header<>;
    using response_parser = boost::beast::http::response_parser<iobuf_body>;

    explicit client(const rpc::base_transport::configuration& cfg);

    // Response state machine
    class response_stream {
    public:
        /// C-tor can only be called by http_request
        explicit response_stream(client* client);

        response_stream(response_stream&&) = delete;
        response_stream(response_stream const&) = delete;
        response_stream& operator=(response_stream const&) = delete;
        response_stream operator=(response_stream&&) = delete;
        ~response_stream() = default;

        /// Return true if the parsing is done
        bool is_done() const;

        /// Return true if the header parsing is done
        bool is_header_done() const;

        /// Access response headers (should only be called if is_header_done()
        /// == true)
        response_header const& get_headers() const;

        /// Recv new portion of the payload, this method should be called untill
        /// is_done returns false. It's possible to get an empty iobuf which
        /// should be ignored (deosn't mean EOF).
        ss::future<iobuf> recv_some();

    private:
        client* _client;
        response_parser _parser;
        iobuf _buffer; /// store incomplete tail elements
    };

    using response_stream_ref = ss::shared_ptr<response_stream>;

    // Request state machine
    // can only be created by the http_client
    class request_stream {
    public:
        explicit request_stream(client* client, request_header&& hdr);

        request_stream(request_stream&&) = delete;
        request_stream(request_stream const&) = delete;
        request_stream& operator=(request_stream const&) = delete;
        request_stream operator=(request_stream&&) = delete;
        ~request_stream() = default;

        /// Send data, if heders weren't sent they should be sent first
        /// followed by the data. BufferSeq is supposed to be an iobuf
        /// or temporary_buffer<char>.
        ss::future<> send_some(iobuf&& seq);

        // True if done, false otherwise
        bool is_done();

        // Wait until remaining data will be transmitted
        ss::future<> send_eof();

    private:
        client* _client;
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

    // Make http_request, if the transport is not yet connected it will connect
    // first otherwise the future will resolve immediately.
    ss::future<request_response_t> make_request(request_header&& header);

private:
    template<class BufferSeq>
    static ss::future<>
    forward(rpc::batched_output_stream& stream, BufferSeq&& seq);
};

template<class BufferSeq>
inline ss::future<>
client::forward(rpc::batched_output_stream& stream, BufferSeq&& seq) {
    auto scattered = iobuf_as_scattered(std::forward<BufferSeq>(seq));
    return stream.write(std::move(scattered));
}
} // namespace http