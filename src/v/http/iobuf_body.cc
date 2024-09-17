// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/iobuf_body.h"

#include "base/vlog.h"
#include "http/logger.h"

#include <boost/beast/core/buffer_traits.hpp>
#include <boost/beast/core/detail/buffers_ref.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/http.hpp>
#include <boost/optional/optional.hpp>

namespace http {

// iobuf_body::value_type implementation

iobuf_body::value_type::offset_match iobuf_body::value_type::range_to_offset(
  std::string_view source, std::string_view subseq) {
    if (
      (source.begin() <= subseq.begin() && subseq.begin() < source.end())
      && (source.begin() < subseq.end() && subseq.end() <= source.end())) {
        return {
          .found = true,
          .offset = static_cast<size_t>(subseq.begin() - source.begin()),
          .length = static_cast<size_t>(subseq.length())};
    }
    return {.found = false, .offset = 0, .length = 0};
}

size_t iobuf_body::value_type::size() const { return _size_bytes; }

bool iobuf_body::value_type::is_done() const { return _done; }

void iobuf_body::value_type::append(boost::asio::const_buffer buf) {
    if (_zc_source) {
        // check that the buffer is inside the _zc_source
        std::string_view vbuf(static_cast<const char*>(buf.data()), buf.size());
        for (auto& frag : _zc_source->get()) {
            std::string_view vsrc(frag.get(), frag.size());
            auto [incl, offset, length] = range_to_offset(vsrc, vbuf);
            if (incl) {
                _produced.append(frag.share(offset, length));
                _size_bytes += buf.size();
                return;
            }
        }
    }
    _size_bytes += buf.size();
    _produced.append(static_cast<const char*>(buf.data()), buf.size());
}

void iobuf_body::value_type::set_temporary_source(iobuf& buffer) {
    _zc_source = std::ref(buffer);
}

iobuf iobuf_body::value_type::consume() {
    _zc_source = std::nullopt;
    iobuf tmp;
    std::swap(tmp, _produced);
    return tmp;
}

void iobuf_body::value_type::finish() { _done = true; }

// iobuf_body::reader implementation

void iobuf_body::reader::init(
  const boost::optional<std::uint64_t>&, boost::beast::error_code& ec) {
    ec = {};
}

void iobuf_body::reader::finish(boost::beast::error_code& ec) {
    vlog(http_log.debug, "reader - finish called");
    ec = {};
    _body.finish();
}

std::uint64_t iobuf_body::size(const value_type& body) { return body.size(); }

static_assert(
  boost::beast::http::is_body<iobuf_body>::value,
  "Body type requirements not met");

static_assert(
  boost::beast::http::is_body_reader<iobuf_body>::value,
  "BodyReader type requirements not met");

} // namespace http
