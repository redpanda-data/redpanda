/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/json/parser.h"

#include "container/chunked_vector.h"
#include "serde/json/detail/numeric.h"
#include "serde/json/detail/string.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <variant>

#define ENABLE_TRACE 0

#if ENABLE_TRACE
#define TRACE(...)                                                             \
    do {                                                                       \
        fmt::print(__VA_ARGS__);                                               \
    } while (false)
#else
#define TRACE(...)                                                             \
    do {                                                                       \
    } while (false)
#endif

namespace serde::json {

class iobuf_wrapper {
public:
    explicit iobuf_wrapper(iobuf&& buf)
      : _buf(std::move(buf))
      , _size(_buf.size_bytes())
      , _frag(_buf.begin())
      , _frag_end(_buf.end()) {
        if (_frag != _frag_end) {
            _frag_index = _frag->get();
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            _frag_index_end = _frag->get() + _frag->size();
            if (_frag_index == _frag_index_end) {
                next_fragment();
            }
        }
    }

    char peek() const {
        if (empty()) {
            throw std::out_of_range("peek on empty iobuf_wrapper");
        }
        return *_frag_index;
    }

    iobuf_wrapper& operator++() {
        ++_pos;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (++_frag_index == _frag_index_end) {
            next_fragment();
        }
        return *this;
    }

    void skip(size_t n) {
        if (n > remaining()) {
            throw std::out_of_range("trying to skip past end of iobuf_wrapper");
        }

        while (n > 0) {
            vassert(
              _frag_index != _frag_index_end,
              "the check when we entered this function should prevent this");

            size_t skip = std::min(
              n, static_cast<size_t>(_frag_index_end - _frag_index));
            _pos += skip;
            n -= skip;

            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            _frag_index += skip;
            if (_frag_index == _frag_index_end) {
                next_fragment();
            }
        }
    }

    ss::temporary_buffer<char> peek_buf() {
        if (empty()) {
            throw std::out_of_range("get on empty iobuf_wrapper");
        }

        return _frag->share(
          _frag_index - _frag->get(), _frag_index_end - _frag_index);
    }

    bool empty() const { return _frag == _frag_end; }

    // Return the number of bytes remaining in the buffer.
    size_t remaining() const { return _size - _pos; }

private:
    void next_fragment() {
        while (true) {
            ++_frag;
            if (_frag != _frag_end) {
                _frag_index = _frag->get();
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                _frag_index_end = _frag->get() + _frag->size();
                // handle an empty fragment
                if (_frag_index == _frag_index_end) {
                    continue;
                }
                return;
            }
            _frag_index = nullptr;
            _frag_index_end = nullptr;
            return;
        }
    }

private:
    iobuf _buf;
    size_t _pos = 0;
    size_t _size = 0;
    iobuf::iterator _frag;
    iobuf::const_iterator _frag_end;
    const char* _frag_index = nullptr;
    const char* _frag_index_end = nullptr;
};

class parser::impl {
    using token = serde::json::token;

public:
    explicit impl(iobuf&& buf, parser_config config)
      : _buf(std::move(buf))
      , _config(config) {}

    // The parser will suspend upon parsing a token that must be signaled to
    // the caller.
    ss::future<token> next_token() {
        TRACE("next_token, remaining: {}\n", _buf.remaining());

        // Reset the current value to a monostate to avoid reusing old
        // values.
        _current_value = std::monostate{};

        co_await skip_whitespace();

        // If suspension stack is empty, we are not expecting any more
        // bytes in the buffer. However, if additional bytes are present
        // it must mean that the input was malformed.
        if (_suspension_stack.empty()) {
            if (_buf.empty()) {
                co_return suspend_with_token(token::eof);
            } else {
                co_return fuse_with_failure();
            }
        }

        // Try to parse next byte(s) from the buffer based on the current
        // suspension point.
        switch (_suspension_stack.back()) {
        case suspension_point::json_document:
            _suspension_stack.pop_back();
            co_return co_await parse_json_value();

        case suspension_point::first_array_member:
            // Replace the top of the stack to allow for subsequent array
            // members to be parsed.
            _suspension_stack.pop_back();
            _suspension_stack.push_back(suspension_point::array_member);
            co_return co_await parse_array_member(false);

        case suspension_point::array_member:
            // We don't pop the state because additional array members may
            // follow after the first one. We will pop it when we reach the
            // end of the array.
            co_return co_await parse_array_member(true);

        case suspension_point::first_object_key:
            // Replace the top of the stack to allow for subsequent object
            // members to be parsed.
            _suspension_stack.pop_back();
            _suspension_stack.push_back(suspension_point::object_key);
            co_return co_await parse_object_member(false);

        case suspension_point::object_key:
            // We don't pop the state because additional object members may
            // follow after the first one. We will pop it when we reach the
            // end of the object.
            co_return co_await parse_object_member(true);

        case suspension_point::object_value:
            _suspension_stack.pop_back();
            co_return co_await parse_object_value();
        }

        std::unreachable();
    }

    // NOLINTNEXTLINE(misc-no-recursion): At most one level of recursion
    // is used in this function.
    ss::future<> skip_value() {
        if (!_current_token) {
            // Nothing was parsed yet. Skip the entire document.
            TRACE("skip_value: no current token, skipping entire document\n");
            co_await next_token();

            dassert(
              _current_token.has_value(), "expected a token to be parsed");

            if (
              _current_token == token::error || _current_token == token::eof) {
                // If we are at the end of the document or an error occurred,
                // we have nothing to skip.
                TRACE("skip_value: no value to skip\n");
                co_return;
            }
        }

        switch (*_current_token) {
        case token::start_object:
        case token::start_array: {
            auto stack_size = _suspension_stack.size();
            TRACE(
              "skip_value: start_object/array, stack size: {}\n", stack_size);
            while (_suspension_stack.size() >= stack_size) {
                TRACE(
                  "skip_value: stack value: {}\n",
                  std::to_underlying(_suspension_stack.back()));

                // Skip the values until the stack size decreases meaning that
                // we are past the end of the object or array.
                co_await next_token();

                if (
                  _current_token == token::error
                  || _current_token == token::eof) {
                    // If we are at the end of the document or an error
                    // occurred, we have nothing to skip.
                    TRACE("skip_value: no value to skip\n");
                    co_return;
                }
            }

            TRACE(
              "skip_value: end_object/array, stack size: {}\n",
              _suspension_stack.size());

            dassert(
              *_current_token == token::end_object
                || *_current_token == token::end_array,
              "expected end of object or array but got {}",
              *_current_token);

            co_return;
        }

        case token::value_null:
        case token::value_true:
        case token::value_false:
        case token::value_double:
        case token::value_int:
        case token::value_string:
            co_return;

        case token::key:
            // The caller just inspected the key and intends to skip the
            // value so we skip both the key and the value.
            co_await next_token(); // skip the key
            if (
              _current_token == token::error || _current_token == token::eof) {
                // If we are at the end of the document or an error occurred,
                // we have nothing to skip.
                TRACE("skip_value: no value to skip after key\n");
                co_return;
            }
            // Skip the value of the key. Maximum recursion depth is 1 by
            // construction as the current token at this point can't be a key
            // anymore.
            TRACE("skip_value: skipping value of key\n");
            co_await skip_value();
            co_return;

        case token::eof:
        case token::error:
        case token::end_object:
        case token::end_array:
            throw std::runtime_error(fmt::format(
              "skip_value called with unexpected token: {}", *_current_token));
        }

        dassert(false, "Unreachable. All cases should be handled.");
        std::unreachable();
    }

    ss::future<token> parse_json_value() {
        TRACE("parse_json_value, peek: {}\n", _buf.peek());

        if (_buf.empty()) {
            TRACE("parse_json_value: empty buffer\n");
            co_return fuse_with_failure();
        }

        switch (_buf.peek()) {
        case '{':
            co_return parse_object();
        case '[':
            co_return parse_array();
        case 'n':
            co_return parse_literal("null", token::value_null);
        case 't':
            co_return parse_literal("true", token::value_true);
        case 'f':
            co_return parse_literal("false", token::value_false);
        case '"':
            co_return co_await parse_string();
        default:
            co_return co_await parse_number();
        }
    }

    token parse_array() {
        TRACE("parse_array\n");
        dassert(_buf.peek() == '[', "expected '[' but got {}", _buf.peek());

        if (_suspension_stack.size() >= _config.max_depth) {
            return fuse_with_failure();
        }

        ++_buf; // consume '['
        _suspension_stack.push_back(suspension_point::first_array_member);
        return suspend_with_token(token::start_array);
    }

    ss::future<token> parse_array_member(bool expect_separator) {
        TRACE("parse_array_value, expect_separator {}\n", expect_separator);
        if (_buf.empty()) {
            co_return fuse_with_failure();
        }

        auto b = _buf.peek();

        if (b == ',') {
            if (!expect_separator) {
                co_return fuse_with_failure();
            }
            ++_buf; // consume ','
            co_await skip_whitespace();
        } else if (b == ']') {
            ++_buf;                       // consume ']'
            _suspension_stack.pop_back(); // pop the array member state
            co_return suspend_with_token(token::end_array);
        } else if (expect_separator) {
            co_return fuse_with_failure();
        }

        co_return co_await parse_json_value();
    }

    token parse_object() {
        TRACE("parse_object\n");
        dassert(_buf.peek() == '{', "expected '{{' but got {}", _buf.peek());

        if (_suspension_stack.size() >= _config.max_depth) {
            return fuse_with_failure();
        }

        ++_buf; // consume '{'
        _suspension_stack.push_back(suspension_point::first_object_key);
        return suspend_with_token(token::start_object);
    }

    ss::future<token> parse_object_member(bool expect_separator) {
        TRACE("parse_object_member, expect_separator {}\n", expect_separator);
        if (_buf.empty()) {
            co_return fuse_with_failure();
        }

        auto b = _buf.peek();

        if (b == ',') {
            if (!expect_separator) {
                co_return fuse_with_failure();
            }
            ++_buf; // consume ','
            co_await skip_whitespace();
        } else if (b == '}') {
            ++_buf;                       // consume '}'
            _suspension_stack.pop_back(); // pop the object key state
            co_return suspend_with_token(token::end_object);
        } else if (expect_separator) {
            co_return fuse_with_failure();
        }

        co_return co_await parse_string(true);
    }

    ss::future<token> parse_object_value() {
        if (_buf.empty()) {
            TRACE("parse_object_value: empty buffer\n");
            co_return fuse_with_failure();
        }

        TRACE("parse_object_value, peek {}\n", _buf.peek());

        auto b = _buf.peek();

        if (b == ':') {
            ++_buf; // consume ':'
            co_await skip_whitespace();
            co_return co_await parse_json_value();
        } else {
            co_return fuse_with_failure();
        }
    }

    token parse_literal(std::string_view literal, token t) {
        TRACE(
          "parse_literal, literal {}, rem: {}\n", literal, _buf.remaining());

        if (_buf.remaining() < literal.size()) {
            return fuse_with_failure();
        }

        for (auto c : literal) {
            if (_buf.peek() != c) {
                return fuse_with_failure();
            }
            ++_buf; // consume the character
        }

        return suspend_with_token(t);
    }

    ss::future<token> parse_string(bool is_key = false) {
        if (_buf.empty()) {
            TRACE("parse_string: empty buffer\n");
            co_return fuse_with_failure();
        }
        TRACE("parse_string, is_key {}, peek {}\n", is_key, _buf.peek());

        auto string_parser = detail::string_parser();
        auto string_parse_result
          = detail::string_parser::result::need_more_data;
        while (string_parse_result
               == detail::string_parser::result::need_more_data) {
            if (_buf.empty()) {
                TRACE("parse_string: empty buffer, need more data\n");
                co_return fuse_with_failure();
            }

            auto tmpbuf = _buf.peek_buf();
            auto pos = string_parser.advance(tmpbuf, string_parse_result);
            switch (string_parse_result) {
            case detail::string_parser::result::need_more_data:
                _buf.skip(pos);
                continue;
            case detail::string_parser::result::invalid_json_string:
                co_return fuse_with_failure();
            case detail::string_parser::result::done:
                _buf.skip(pos);

                TRACE("parse_string: done, moving value\n");

                _current_value = std::move(string_parser).value();

                // We have a complete string.
                TRACE(
                  "parse_string: done, is_key {}, buf.peek() {}, val: {}\n",
                  is_key,
                  _buf.empty() ? 'z' : _buf.peek(),
                  std::get<iobuf>(_current_value).hexdump(1024));

                if (is_key) {
                    _suspension_stack.push_back(suspension_point::object_value);
                    co_return suspend_with_token(token::key);
                } else {
                    co_return suspend_with_token(token::value_string);
                }
            }
        }

        std::unreachable();
    }

    ss::future<token> parse_number() {
        if (_buf.empty()) {
            TRACE("parse_number: empty buffer\n");
            co_return fuse_with_failure();
        }
        TRACE("parse_number, peek {}\n", _buf.peek());

        auto numeric_parser = detail::numeric_parser();
        auto numeric_parse_result
          = detail::numeric_parser::result::need_more_data;
        while (numeric_parse_result
               == detail::numeric_parser::result::need_more_data) {
            if (_buf.empty()) {
                if (_suspension_stack.empty()) {
                    TRACE(
                      "parse_number: EOF at top level, trying to finalize\n");
                    numeric_parser.finalize(numeric_parse_result);
                    // If the number is done produce it, else fall through
                    // to the incomplete case.
                    if (
                      numeric_parse_result
                      == detail::numeric_parser::result::done) {
                        if (numeric_parser.is_int()) {
                            _current_value
                              = std::move(numeric_parser).value_int64();
                            co_return suspend_with_token(token::value_int);
                        } else {
                            _current_value
                              = std::move(numeric_parser).value_double();
                            co_return suspend_with_token(token::value_double);
                        }
                    }
                }
                TRACE("parse_number: empty buffer mid-number\n");
                co_return fuse_with_failure();
            }

            auto tmpbuf = _buf.peek_buf();
            auto pos = numeric_parser.advance(tmpbuf, numeric_parse_result);
            switch (numeric_parse_result) {
            case detail::numeric_parser::result::need_more_data:
                _buf.skip(pos);
                continue;
            case detail::numeric_parser::result::invalid_json_string:
                co_return fuse_with_failure();
            case detail::numeric_parser::result::done:
                _buf.skip(pos);

                if (numeric_parser.is_int()) {
                    _current_value = std::move(numeric_parser).value_int64();
                    co_return suspend_with_token(token::value_int);
                } else {
                    _current_value = std::move(numeric_parser).value_double();
                    co_return suspend_with_token(token::value_double);
                }
            }
        }
        std::unreachable();
    }

    // Return the current token without advancing the parser.
    std::optional<token> current_token() const { return _current_token; }

    int64_t value_int() {
        auto tmp = std::get<int64_t>(std::move(_current_value));
        _current_value = std::monostate{};
        return tmp;
    }

    double value_double() {
        auto tmp = std::get<double>(std::move(_current_value));
        _current_value = std::monostate{};
        return tmp;
    }

    iobuf value_string() {
        auto tmp = std::get<iobuf>(std::move(_current_value));
        _current_value = std::monostate{};
        return tmp;
    }

private:
    token fuse_with_failure() {
        _current_token = token::error;
        _suspension_stack.clear();
        return token::error;
    }

    token suspend_with_token(token t) {
        _current_token = t;
        return t;
    }

    ss::future<> skip_whitespace() {
        while (!_buf.empty()) {
            char c = _buf.peek();
            if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                ++_buf;
            } else {
                break;
            }
            co_await ss::coroutine::maybe_yield();
        }
    }

private:
    // The buffer containing the JSON document.
    iobuf_wrapper _buf;

    // The current token as-of suspension.
    std::optional<token> _current_token{std::nullopt};

    // The parser will suspend upon parsing a token that must be signaled to
    // the caller.
    enum class suspension_point {
        // The initial state of the parser, expecting a number, string,
        // boolean, null, or the start of an object or array.
        json_document,

        // The parser is expecting the first element of an array.
        // This is the state after the `[` token.
        first_array_member,

        // The parser is expecting a subsequent value of an array lead by a
        // `,`.
        array_member,

        // The parse is expecting the first key of an object.
        // This is the state after the `{` token.
        first_object_key,
        // The parser is expecting a subsequent key of an object lead by a
        // `,`.
        object_key,
        // The parser is expecting the value of an object key lead by a `:`.
        object_value,
    };

    // The stack is used to keep track of the current position in the
    // JSON document. When we suspend, we push the current position on
    // the stack. When we resume, we pop the position from the stack and
    // continue parsing from there.
    chunked_vector<suspension_point> _suspension_stack{
      suspension_point::json_document,
    };

    std::variant<std::monostate, iobuf, int64_t, double> _current_value;

    parser_config _config;
};

parser::parser(iobuf buf, parser_config config)
  : _impl(std::make_unique<impl>(std::move(buf), config)) {};

parser::~parser() = default;

ss::future<bool> parser::next() {
    auto current_token = _impl->current_token();
    if (
      current_token.has_value()
      && (current_token.value() == token::eof || current_token.value() == token::error)) {
        return ss::as_ready_future(false);
    }

    return _impl->next_token().then([](auto&&) { return true; });
}

ss::future<> parser::skip_value() { co_await _impl->skip_value(); }

token parser::token() const { return _impl->current_token().value(); }

int64_t parser::value_int() { return _impl->value_int(); }

double parser::value_double() { return _impl->value_double(); }

iobuf parser::value_string() { return _impl->value_string(); }

} // namespace serde::json
