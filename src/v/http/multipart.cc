/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "multipart.h"

#include "bytes/details/io_byte_iterator.h"
#include "bytes/details/io_fragment.h"
#include "bytes/iobuf.h"
#include "utils/utf8.h"
#include "vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream-impl.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/later.hh>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>

namespace {

static constexpr std::string_view crlf = "\r\n";
static constexpr std::string_view dash_dash = "--";

static constexpr auto CHAR_TAB_SIZE = std::numeric_limits<unsigned char>::max();

consteval std::array<char, CHAR_TAB_SIZE> build_bchar_table() {
    std::array<char, CHAR_TAB_SIZE> tab = {};

    // DIGIT
    for (unsigned char c = '0'; c <= '9'; c++) {
        tab.at(c) = 1;
    }

    // ALPHA (lower)
    for (unsigned char c = 'a'; c <= 'z'; c++) {
        tab.at(c) = 1;
    }

    // ALPHA (upper)
    for (unsigned char c = 'A'; c <= 'Z'; c++) {
        tab.at(c) = 1;
    }

    tab['\''] = 1;
    tab['('] = 1;
    tab[')'] = 1;
    tab['+'] = 1;
    tab['_'] = 1;
    tab[','] = 1;
    tab['-'] = 1;
    tab['.'] = 1;
    tab['/'] = 1;
    tab[':'] = 1;
    tab['='] = 1;
    tab['?'] = 1;
    tab[' '] = 1;

    return tab;
}

static constexpr auto max_bsize = 70;
static constexpr auto bchar_tab = build_bchar_table();

[[nodiscard]] inline bool valid_bhcar(unsigned char c) {
    static_assert(
      std::numeric_limits<decltype(c)>::min() >= 0
      && std::numeric_limits<decltype(c)>::max() <= bchar_tab.size());

    // Valid bounds are guaranteed by the static_assert call above.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    return bchar_tab[c];
}

[[nodiscard]] bool valid_boundary(const std::string_view value) {
    if (value.empty() || value.size() > max_bsize) {
        return false;
    }

    for (unsigned char c : value) {
        if (!valid_bhcar(c)) {
            return false;
        }
    }

    // Extra requirement for the last character: Must not be a space.
    if (value.ends_with(' ')) {
        return false;
    }

    return true;
}

/// field-name  =  1*<any CHAR, excluding CTLs, SPACE, and ":">
/// CHAR        =  <any ASCII character>        ; ( 0.-127.)
/// CTL         =  <any ASCII control           ; ( 0.- 31.)
///                 character and DEL>          ; ( 127.)
/// SPACE                                       ; ( 32.)
/// https://datatracker.ietf.org/doc/html/rfc822#appendix-B.1
[[nodiscard]] bool valid_field_name_char(unsigned char c) {
    return c > 32 && c < 127 && c != ':';
}
} // namespace

namespace http {

multipart_writer::multipart_writer(
  ss::output_stream<char>& out, std::string boundary)
  : _out(out)
  , _boundary(std::move(boundary)) {
    if (!valid_boundary(_boundary)) {
        throw std::runtime_error("invalid boundary");
    }
};

ss::future<> multipart_writer::write_part(
  multipart_fields&& header, ss::temporary_buffer<char>&& body) {
    vassert(!_closed, "called write_part on a closed multipart_writer");

    return ss::do_with(std::move(body), [this, &header](auto& body) {
        return write_part_header(std::move(header)).then([this, &body] {
            return _out.write(std::move(body));
        });
    });
}

ss::future<>
multipart_writer::write_part(multipart_fields&& header, iobuf&& body) {
    vassert(!_closed, "called write_part on a closed multipart_writer");

    return ss::do_with(std::move(body), [this, &header](auto& body) {
        return write_part_header(std::move(header)).then([this, &body] {
            return ss::do_for_each(body, [this](const auto& f) {
                return _out.write(f.get(), f.size());
            });
        });
    });
}

ss::future<> multipart_writer::close() {
    vassert(
      !std::exchange(_closed, true), "the multipart_writer was already closed");

    if (!_first_part_sent) {
        return ss::make_exception_future(
          std::runtime_error("RFC2046 requires a multipart message to have at "
                             "least one body part"));
    }

    return _out.write(crlf.data(), crlf.size())
      .then([this] { return _out.write(dash_dash.data(), dash_dash.size()); })
      .then([this] { return _out.write(_boundary.data(), _boundary.size()); })
      .then([this] { return _out.write(dash_dash.data(), dash_dash.size()); })
      .then([this] { return _out.write(crlf.data(), crlf.size()); })
      .then([this] { return _out.close(); });
}

ss::future<> multipart_writer::write_delimiter() {
    auto prefix_f = ss::now();

    if (_first_part_sent) {
        prefix_f = _out.write(crlf.data(), crlf.size());
    } else {
        _first_part_sent = true;
    }

    return prefix_f
      .then([this] { return _out.write(dash_dash.data(), dash_dash.size()); })
      .then([this] { return _out.write(_boundary.data(), _boundary.size()); })
      .then([this] { return _out.write(crlf.data(), crlf.size()); });
}

ss::future<> multipart_writer::write_part_header(multipart_fields&& header) {
    return ss::do_with(std::move(header), [this](auto& header) {
        return write_delimiter().then([this, &header] {
            return ss::do_with(
              multipart_fields::writer(header), [this](auto& w) {
                  const auto buffers = w.get();
                  return ss::do_for_each(buffers, [this](const auto& b) {
                      return _out.write(
                        static_cast<const char*>(b.data()), b.size());
                  });
              });
        });
    });
}

std::string random_multipart_boundary() {
    return boost::uuids::to_string(boost::uuids::random_generator()());
}

} // namespace http
