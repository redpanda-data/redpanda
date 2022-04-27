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

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "seastarx.h"
#include "utils/utf8.h"
#include "utils/vint.h"

#include <seastar/core/sstring.hh>

#include <memory>

/**
 * iobuf parser interface suitable for an iobuf passed by const-ref. also
 * accepts an iobuf value. in both cases it is safe to move this type, but when
 * constructed from a const-ref the non-owned iobuf reference must not move.
 */
class iobuf_parser_base {
public:
    struct tag_owned_buf {};
    struct tag_const_ref {};

    iobuf_parser_base(iobuf buf, tag_owned_buf)
      : _buf(std::make_unique<iobuf>(std::move(buf)))
      , _in(ref().cbegin(), ref().cend())
      , _original_size(ref().size_bytes()) {}

    iobuf_parser_base(const iobuf& buf, tag_const_ref)
      : _buf(&buf)
      , _in(cref().cbegin(), cref().cend())
      , _original_size(cref().size_bytes()) {}

    size_t bytes_left() const { return _original_size - _in.bytes_consumed(); }

    size_t bytes_consumed() const { return _in.bytes_consumed(); }

    std::pair<int64_t, uint8_t> read_varlong() {
        auto [val, length_size] = vint::deserialize(_in);
        _in.skip(length_size);
        return {val, length_size};
    }

    std::pair<uint32_t, uint8_t> read_unsigned_varint() {
        auto [val, length_size] = unsigned_vint::deserialize(_in);
        _in.skip(length_size);
        return {val, length_size};
    }

    ss::sstring read_string(size_t len) {
        ss::sstring str = ss::uninitialized_string(len);
        _in.consume_to(str.size(), str.begin());
        validate_utf8(str);
        return str;
    }

    bytes read_bytes(size_t n) {
        auto b = ss::uninitialized_string<bytes>(n);
        _in.consume_to(n, b.begin());
        return b;
    }

    bool read_bool() { return bool(consume_type<int8_t>()); }

    template<typename T>
    T consume_type() {
        return _in.consume_type<T>();
    }

    template<typename T>
    T consume_be_type() {
        return _in.consume_be_type<T>();
    }

    void skip(size_t n) { _in.skip(n); }

    template<typename Consumer>
    requires requires(Consumer c, const char* src, size_t max) {
        { c(src, max) } -> std::same_as<ss::stop_iteration>;
    }
    size_t consume(const size_t n, Consumer&& f) {
        return _in.consume(n, std::forward<Consumer>(f));
    }

    template<typename Output>
    [[gnu::always_inline]] void consume_to(size_t n, Output out) {
        return _in.template consume_to(n, std::forward<Output>(out));
    }

    iobuf copy(size_t len) { return iobuf_copy(_in, len); }

    iobuf peek(size_t len) const {
        auto in = _in;
        return iobuf_copy(in, len);
    }

protected:
    iobuf& ref() { return *std::get<owned_buf>(_buf); }

private:
    using const_ref = const iobuf*;
    using owned_buf = std::unique_ptr<iobuf>;

    // See io_iterator_consumer for notes on iterator validity.
    std::variant<const_ref, owned_buf> _buf;
    iobuf::iterator_consumer _in;
    size_t _original_size;

    const iobuf& cref() const { return *std::get<const_ref>(_buf); }
};

class iobuf_const_parser final : public iobuf_parser_base {
public:
    explicit iobuf_const_parser(const iobuf& buf)
      : iobuf_parser_base(buf, tag_const_ref{}) {}
};

/**
 * iobuf parser suitable for sharing.
 */
class iobuf_parser final : public iobuf_parser_base {
public:
    explicit iobuf_parser(iobuf buf)
      : iobuf_parser_base(std::move(buf), tag_owned_buf{}) {}

    iobuf share(size_t len) {
        auto ret = ref().share(bytes_consumed(), len);
        skip(len);
        return ret;
    }

    iobuf share_no_consume(size_t len) {
        return ref().share(bytes_consumed(), len);
    }

    friend std::ostream& operator<<(std::ostream& o, const iobuf_parser& p) {
        return o << "{bytes_left:" << p.bytes_left()
                 << ", bytes_consumed:" << p.bytes_consumed() << "}";
    }
};
