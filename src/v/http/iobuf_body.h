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
#include "bytes/bytes.h"
#include "bytes/iobuf.h"

#include <seastar/core/temporary_buffer.hh>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http/message.hpp>

#include <tuple>
#include <vector>

namespace http {

/// Boost.Beast Body implementation
/// Integrates boost::beast::parser with iobuf.
/// The iobuf or ss::temporary_buffer can be used as a source of data
/// for parser. After the buffer was parsed client should call
/// 'consume' method to produce an iobuf instance. Same buffer that
/// was used to call parser.put should be used to call body.consume.
/// Every call to parser.put shold be matched with the body.consume
/// call. Otherwise the error will be triggered.
///
/// \code
///     http::response_parser<iobuf_body> parser;
///     ss::temporary_buffer<char> buf = ...;
///     boost::asio::const_buffer cbuf{buf.get(), buf.size()};
///     boost::asio::error_code ec = {};
///     size_t nret = parser.put(cbuf, ec);
///     iobuf out = parser.get().body().consume(buf);
/// \endcode
///
/// Single call to body.consume may produce iobuf with multiple fragments
/// in case if the body is encoded used chunked transfer encoding and input
/// buffer contains multiple chunks.
struct iobuf_body {
    /// The algorithm used during parsing
    class reader;

    /// The type of message::body when used
    class value_type {
        /// Result of the offset lookup in the temporary_buffer
        struct offset_match {
            bool found;
            size_t offset;
            size_t length;
        };

        // Calculate offset of the region 'subseq' inside the 'source'
        // buffer. If the region is not inside source buffer return
        // false in the first element of the tuple. Otherwise return [true,
        // offset, len].
        static offset_match
        range_to_offset(std::string_view source, std::string_view subseq);

    public:
        value_type() = default;
        ~value_type() = default;
        value_type(value_type&&) = default;
        value_type& operator=(value_type&&) = default;
        value_type(const value_type&) = delete;
        const value_type& operator=(const value_type&) = delete;

        size_t size() const;

        bool is_done() const;

        /// Produce buffer
        void append(boost::asio::const_buffer buf);

        /// Construct new iobuf using the pointers from the produced sequence
        iobuf consume();

        /// Set source buffer for zero-copy optimization
        ///
        /// \note The buffer is used by the following 'append' calls.
        /// The 'consume' method resets the buffer. The method call
        /// is optional. If it's not called before 'append' zero copy
        /// mechanics won't be used. If wrong buffer will be passed to
        /// the method nothing bad will happen as well.
        void set_temporary_source(iobuf& buffer);

        /// This method is called by the parser when the response
        /// body is consumed.
        void finish();

    private:
        friend class reader;

        size_t _size_bytes = 0;
        bool _done = false;
        std::optional<std::reference_wrapper<iobuf>> _zc_source;
        iobuf _produced;
    };

    /// Reader implementation that pushes data to iobuf
    /// instance. Supposed to be used with boost.beast
    /// message parser.
    class reader {
    public:
        template<bool isRequest, class Fields>
        explicit reader(
          boost::beast::http::header<isRequest, Fields>&, value_type& b);

        void init(
          const boost::optional<std::uint64_t>&, boost::beast::error_code& ec);

        /// Generic 'put' implementation that work with any ConstBufferSequence
        template<class ConstBufferSequence>
        size_t
        put(const ConstBufferSequence& buffers, boost::beast::error_code& ec);

        void finish(boost::beast::error_code& ec);

    private:
        value_type& _body;
    };

    /// Returns the body's payload size
    static std::uint64_t size(const value_type& body);
};

template<bool isRequest, class Fields>
iobuf_body::reader::reader(
  boost::beast::http::header<isRequest, Fields>&, value_type& b)
  : _body(b) {}

template<class ConstBufferSequence>
size_t iobuf_body::reader::put(
  const ConstBufferSequence& buffers, boost::beast::error_code& ec) {
    ec = {};
    size_t nwritten = 0;
    for (auto buffer : boost::beast::buffers_range_ref(buffers)) {
        boost::asio::const_buffer chunk{buffer.data(), buffer.size()};
        _body.append(chunk);
        nwritten += buffer.size();
    }
    return nwritten;
}

} // namespace http
