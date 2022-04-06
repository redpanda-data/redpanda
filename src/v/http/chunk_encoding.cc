// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/chunk_encoding.h"

#include "bytes/iobuf.h"

#include <seastar/core/temporary_buffer.hh>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/http/chunk_encode.hpp>
#include <boost/beast/http/field.hpp>

namespace http {

void chunked_encoder::append_chunk_body(
  iobuf& seq, ss::temporary_buffer<char>&& payload) {
    size_t sz = payload.size();
    boost::beast::http::chunk_header header(sz);
    for (const auto& buf : header) {
        seq.append(static_cast<const uint8_t*>(buf.data()), buf.size());
    }
    seq.append(std::move(payload));
    boost::beast::http::chunk_crlf crlf;
    for (const auto& buf : crlf) {
        seq.append(static_cast<const uint8_t*>(buf.data()), buf.size());
    }
}

void chunked_encoder::encode_impl(
  iobuf& seq, ss::temporary_buffer<char>&& buf) const {
    for (size_t pos = 0; pos < buf.size(); pos += _max_chunk_size) {
        size_t chunk_size = std::min(_max_chunk_size, buf.size() - pos);
        auto tmp = buf.share(pos, chunk_size);
        append_chunk_body(seq, std::move(tmp));
    }
}

chunked_encoder::chunked_encoder(bool bypass, size_t max_chunk_size)
  : _max_chunk_size(max_chunk_size)
  , _bypass(bypass) {}

iobuf chunked_encoder::encode(ss::temporary_buffer<char>&& buf) const {
    iobuf seq;
    if (_bypass) {
        seq.append(std::move(buf));
    } else {
        encode_impl(seq, std::move(buf));
    }
    return seq;
}

iobuf chunked_encoder::encode(iobuf&& inp) {
    if (_bypass) {
        return std::move(inp);
    }
    iobuf out;
    for (auto& frag : inp) {
        auto tmp = frag.share();
        encode_impl(out, std::move(tmp));
    }
    return out;
}

iobuf chunked_encoder::encode_eof() const {
    iobuf seq;
    if (!_bypass) {
        boost::beast::http::chunk_last<> last;
        for (const auto& buf : last) {
            seq.append(static_cast<const uint8_t*>(buf.data()), buf.size());
        }
    }
    return seq;
}

} // namespace http
