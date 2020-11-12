// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/iobuf_body.h"

#include "http/logger.h"
#include "vlog.h"

#include <boost/beast/core/buffer_traits.hpp>
#include <boost/beast/core/detail/buffers_ref.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/http.hpp>
#include <boost/core/ignore_unused.hpp>
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
    _seq.push_back(buf);
}

iobuf iobuf_body::value_type::consume(ss::temporary_buffer<char>& source) {
    vlog(http_log.trace, "iobuf_body.consume, _seq.size() = {}", _seq.size());
    iobuf tmp;
    std::string_view source_view(source.get(), source.size());
    for (auto buf : _seq) {
        // check that the buf is inside the source (should always be the case if
        // iobuf_body is used correctly)
        std::string_view buf_view(
          static_cast<const char*>(buf.data()), buf.size());
        auto [found, offset, length] = range_to_offset(source_view, buf_view);
        vassert(found, "Invalid buffer sequence");
        // share the source and put it into the resulting iobuf
        tmp.append(source.share(offset, length));
        _size_bytes += buf.size();
    }
    _seq.clear();
    return tmp;
}

iobuf iobuf_body::value_type::consume(iobuf& source, size_t limit) {
    // Invariant 1: '_seq' contains subsets of 'source', every element in '_seq'
    // fits
    //              inside some fragment in 'source' fully. The memory order is
    //              the same in both containers.
    // Invariant 2: 'limit' is greater or equal to combined '_seq' sizes
    vlog(
      http_log.trace,
      "iobuf_body.consume, _seq.size() = {}, limit = {}",
      _seq.size(),
      limit);
    boost::ignore_unused(limit);
#ifndef NDEBUG
    size_t sumsize = 0;
    for (auto const& cbuf : _seq) {
        sumsize += cbuf.size();
    }
    vassert(
      sumsize <= limit, "consume wasn't invoked suffecient number of times");
#endif
    iobuf tmp;
    // _seq is expected to have only one element because of eager parsing
    // in rare and tricky cases it might have two
    for (auto buf : _seq) {
        // check that the buf is inside the source (should always be the case if
        // iobuf_body is used correctly)
        std::string_view buf_view(
          static_cast<const char*>(buf.data()), buf.size());

        bool offset_found = false;
        for (auto& frag : source) {
            // Should hit right fragment on a first iteration
            std::string_view source_view(frag.get(), frag.size());
            auto [incl, offset, length] = range_to_offset(
              source_view, buf_view);
            if (incl) {
                // share the source and put it into the resulting iobuf
                tmp.append(frag.share(offset, length));
                _size_bytes += buf.size();
                offset_found = true;
                break;
            }
        }
        vassert(
          offset_found,
          "incorect use of iobuf_consume, should be called after every parser "
          "inovcation");
    }
    _seq.clear();
    return tmp;
}

void iobuf_body::value_type::finish() { _done = true; }

// iobuf_body::reader implementation

void iobuf_body::reader::init(
  boost::optional<std::uint64_t> const&, boost::beast::error_code& ec) {
    ec = {};
}

void iobuf_body::reader::finish(boost::beast::error_code& ec) {
    vlog(http_log.debug, "reader - finish called");
    ec = {};
    _body.finish();
}

std::uint64_t iobuf_body::size(value_type const& body) { return body.size(); }

static_assert(
  boost::beast::http::is_body<iobuf_body>::value,
  "Body type requirements not met");

static_assert(
  boost::beast::http::is_body_reader<iobuf_body>::value,
  "BodyReader type requirements not met");

} // namespace http
