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

multipart_reader::multipart_reader(
  ss::input_stream<char>& in, const std::string_view& boundary)
  : _in(in)
  , _dash_boundary_crlf(fmt::format("--{}\r\n", boundary))
  , _dash_boundary(
      _dash_boundary_crlf.data(), _dash_boundary_crlf.size() - crlf.size()) {
    if (!valid_boundary(_dash_boundary)) {
        throw std::runtime_error("invalid boundary");
    }
};

ss::future<std::optional<multipart_body_part>> multipart_reader::next() {
    if (_state == parse_state::epilogue) {
        co_return std::optional<multipart_body_part>();
    }

    for (;;) {
        auto parse_result = do_parse_step();
        if (parse_result) {
            _state = parse_result.value(); // Do the state transition.

            if (_state == parse_state::body_part_done) {
                // Change parser state to handle the next body part and return
                // the current one to the caller.
                _state = parse_state::body_part;
                co_return std::optional<multipart_body_part>(
                  std::move(_current_part_scratch));
            } else if (_state == parse_state::epilogue) {
                co_return std::optional<multipart_body_part>();
            }
        } else {
            switch (parse_result.error()) {
            case parse_error::need_more:
                if (_in.eof()) {
                    if (_state == parse_state::init && _buf.empty()) {
                        // Nothing was ever read.
                        // The input stream was empty to start with.
                        co_return std::optional<multipart_body_part>();
                    }
                    throw std::runtime_error(
                      fmt::format("incomplete message; state = {}", _state));
                }

                _buf.append(co_await _in.read());
                break;
            case parse_error::bad_input:
                throw std::runtime_error(
                  fmt::format("bad message; state = {}", _state));
            }
        }
    }
}

ss::future<> multipart_reader::close() {
    _buf = {};
    return ss::do_until(
      [this] { return _in.eof(); },
      [this] { return _in.read().discard_result(); });
}

multipart_reader::parse_outcome multipart_reader::do_parse_step() {
    switch (_state) {
    case parse_state::body_part_done:
        vassert(
          false,
          "message should be handled by the caller before calling "
          "do_parse_step() again");

    case parse_state::init: {
        if (_buf.size_bytes() < _dash_boundary_crlf.size()) {
            return parse_error::need_more;
        }
        // Check if the message starts with a dash boundary or with an
        // optional preamble which we will ignore.
        if (_buf.share(0, _dash_boundary_crlf.size()) == _dash_boundary_crlf) {
            skip(_dash_boundary_crlf.size());
            return parse_state::body_part;
        } else {
            return parse_state::preamble;
        }
    }

    case parse_state::preamble: {
        auto [found_crlf, prefix] = find_crlf();
        if (found_crlf) {
            // If it is the boundary then we've reached the end of the preamble.
            switch (expect(_dash_boundary_crlf, eat_action::yes)) {
            case expect_result::success:
                return parse_state::body_part;
            case expect_result::failure:
                [[fallthrough]];
            case expect_result::need_more_data:
                skip(prefix + crlf.size());
                return parse_state::preamble;
            }
        } else {
            // Safe to trim only if we are sure it is not a dash boundary ending
            // in CRLF. If it were to be found, then we would be in the other
            // branch.
            if (prefix >= _dash_boundary_crlf.size()) {
                skip(prefix);
            }

            return parse_error::need_more;
        }
    }

    case parse_state::body_part: {
        // Need to be able to disambiguate a) between empty body part, b)
        // close delimiter, and c) body part fields. The close delimiter can
        // happen only at the end so we read an extra byte to check if we
        // hit EOF.
        auto need_bytes = std::max(
          crlf.size() + _dash_boundary.size(), dash_dash.size() + 1);
        const bool enough_data = (_in.eof() || _buf.size_bytes() >= need_bytes);
        if (!enough_data) {
            return parse_error::need_more;
        }

        bool is_empty_body_part = _buf.share(0, crlf.size()) == crlf
                                  && _buf.share(
                                       crlf.size(), _dash_boundary.size())
                                       == _dash_boundary;
        if (is_empty_body_part) {
            // Implementer discretion: skip over empty messages and
            // don't return anything.
            skip(crlf.size() + _dash_boundary.size());
            return parse_state::body_part;
        } else if (_buf.share(0, dash_dash.size()) == dash_dash) {
            return parse_state::epilogue;
        } else {
            return parse_state::message_field;
        }
    }

    case parse_state::message_field: {
        if (_buf.size_bytes() < crlf.size()) {
            return parse_error::need_more;
        }

        if (_buf.share(0, crlf.size()) == crlf) {
            _buf.trim_front(crlf.size());
            return parse_state::message_text;
        } else {
            _field_name_scratch = {};
            _field_value_scratch = {};
            return parse_state::message_field_name;
        }
    }

    case parse_state::message_field_name: {
        auto begin = iobuf::byte_iterator(_buf.begin(), _buf.end());
        auto end = iobuf::byte_iterator(_buf.end(), _buf.end());

        size_t bytes_consumed = 0;
        for (auto it = begin; it != end; ++it) {
            if (*it == ':') {
                if (bytes_consumed < 1) {
                    return parse_error::bad_input;
                }

                _field_name_scratch.resize(bytes_consumed);
                std::copy_n(begin, bytes_consumed, _field_name_scratch.begin());
                _buf.trim_front(bytes_consumed + 1);
                return parse_state::message_field_body;
            }

            if (!valid_field_name_char(*it)) {
                return parse_error::bad_input;
            }

            bytes_consumed += 1;
        }

        return parse_error::need_more;
    }

    case parse_state::message_field_body: {
        auto begin = iobuf::byte_iterator(_buf.begin(), _buf.end());
        auto end = iobuf::byte_iterator(_buf.end(), _buf.end());

        size_t bytes_consumed = 0;
        int next_crlf_match_pos = 0;
        for (auto it = begin; it != end; ++it) {
            if (next_crlf_match_pos == crlf.size()) {
                const auto read_body_size = bytes_consumed - crlf.size();
                size_t offset = _field_value_scratch.size();
                _field_value_scratch.resize(
                  _field_value_scratch.size() + read_body_size);
                std::copy_n(
                  begin, read_body_size, _field_value_scratch.begin() + offset);
                _buf.trim_front(bytes_consumed);

                if (*it == ' ' || *it == '\t') {
                    _buf.trim_front(1);
                    return parse_state::message_field_body;
                } else {
                    _current_part_scratch.header.insert(
                      _field_name_scratch, _field_value_scratch);
                    return parse_state::message_field;
                }
            } else if (*it == crlf[next_crlf_match_pos]) {
                next_crlf_match_pos += 1;
            } else {
                next_crlf_match_pos = 0;
            }

            bytes_consumed += 1;
        }

        return parse_error::need_more;
    }

    case parse_state::message_text: {
        auto it = iobuf::byte_iterator(_buf.begin(), _buf.end());
        auto end = iobuf::byte_iterator(_buf.end(), _buf.end());

        size_t bytes_consumed = 0;
        int crlf_pos = 0;
        size_t dash_boundary_pos = 0;
        for (; it != end; ++it) {
            if (*it == '\r') {
                crlf_pos = 1;
            } else if (crlf_pos == 1 && *it == '\n') {
                crlf_pos = 2;
            } else if (crlf_pos == 2) {
                const auto rem_bytes = _buf.size_bytes() - bytes_consumed;
                if (rem_bytes < _dash_boundary.size() - dash_boundary_pos) {
                    return parse_error::need_more;
                } else {
                    // Try matching the boundary.
                    if (dash_boundary_pos == _dash_boundary.size()) {
                        const auto delimiter_size = crlf.size()
                                                    + _dash_boundary.size();
                        _current_part_scratch.body = _buf.share(
                          0, bytes_consumed - delimiter_size);
                        _buf.trim_front(bytes_consumed);
                        return parse_state::body_part_done;
                    } else if (_dash_boundary[dash_boundary_pos] == *it) {
                        dash_boundary_pos += 1;
                    } else {
                        // No match.
                        crlf_pos = 0;
                        dash_boundary_pos = 0;
                    }
                }
            }

            bytes_consumed += 1;
        }

        return parse_error::need_more;
    }

    case parse_state::epilogue:
        vassert(false, "epilogue state should be handled by the caller");
    }
}

std::pair<bool, size_t> multipart_reader::find_crlf() {
    size_t prefix = 0;
    size_t next_match_pos = 0;

    for (const auto& frag : _buf) {
        for (size_t i = 0; i < frag.size(); i += 1) {
            if (*(frag.get() + i) == crlf[next_match_pos]) {
                next_match_pos += 1;
            } else {
                next_match_pos = 0;
            }
            prefix++;

            if (next_match_pos == crlf.size()) {
                return std::make_pair(true, prefix - crlf.size());
            }
        }
    }

    return std::make_pair(
      next_match_pos == crlf.size(), prefix - next_match_pos);
}

multipart_reader::expect_result
multipart_reader::expect(std::string_view e, eat_action action) {
    if (_buf.size_bytes() < e.size()) {
        return expect_result::need_more_data;
    } else if (_buf.share(0, e.size()) == e) {
        if (action == eat_action::yes) {
            skip(e.size());
        }
        return expect_result::success;
    } else {
        return expect_result::failure;
    }
}

inline void multipart_reader::skip(size_t n) {
    vassert(
      _buf.size_bytes() >= n,
      "the internal buffer must be filled before calling skip");
    _buf.trim_front(n);
}

std::ostream&
operator<<(std::ostream& o, const multipart_reader::parse_state& s) {
    switch (s) {
    case multipart_reader::parse_state::init:
        return o << "{init}";
    case multipart_reader::parse_state::preamble:
        return o << "{preamble}";
    case multipart_reader::parse_state::body_part:
        return o << "{body_part}";
    case multipart_reader::parse_state::message_field:
        return o << "{message_field}";
    case multipart_reader::parse_state::message_field_name:
        return o << "{message_field_name}";
    case multipart_reader::parse_state::message_field_body:
        return o << "{message_field_body}";
    case multipart_reader::parse_state::message_text:
        return o << "{message_text}";
    case multipart_reader::parse_state::body_part_done:
        return o << "{body_part_read}";
    case multipart_reader::parse_state::epilogue:
        return o << "{epilogue}";
    }
}

std::string random_multipart_boundary() {
    return boost::uuids::to_string(boost::uuids::random_generator()());
}

} // namespace http
