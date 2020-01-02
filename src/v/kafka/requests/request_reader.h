#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"
#include "utils/utf8.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <optional>
#include <string_view>
#include <type_traits>

namespace kafka {

class request_reader {
public:
    explicit request_reader(iobuf io) noexcept
      : _io(std::move(io))
      , _in(_io.cbegin(), _io.cend())
      , _original_size(_io.size_bytes()) {}
    size_t bytes_left() const { return _original_size - _in.bytes_consumed(); }
    size_t bytes_consumed() const { return _in.bytes_consumed(); }

    bytes_view read_raw_bytes_view(size_t n) { return do_read_bytes_view(n); }

    bool read_bool() { return bool(read_int8()); }

    int8_t read_int8() { return _in.consume_type<int8_t>(); }

    static int16_t _read_int16(iobuf::iterator_consumer& in) {
        return be_to_cpu(in.consume_type<int16_t>());
    }
    static int32_t _read_int32(iobuf::iterator_consumer& in) {
        return be_to_cpu(in.consume_type<int32_t>());
    }
    static int64_t _read_int64(iobuf::iterator_consumer& in) {
        return be_to_cpu(in.consume_type<int64_t>());
    }

    int16_t read_int16() { return _read_int16(_in); }
    int32_t read_int32() { return _read_int32(_in); }
    int64_t read_int64() { return _read_int64(_in); }

    uint32_t read_uint32() { return be_to_cpu(_in.consume_type<uint32_t>()); }

    int32_t read_varint() { return vint::value_type(read_varlong()); }

    int64_t read_varlong() {
        auto [val, length_size] = vint::deserialize(_in);
        _in.skip(length_size);
        return val;
    }

    sstring read_string() { return do_read_string(read_int16()); }

    std::string_view read_string_view() {
        return do_read_string_view(read_int16());
    }

    std::optional<sstring> read_nullable_string() {
        auto n = read_int16();
        if (n < 0) {
            return std::nullopt;
        }
        return {do_read_string(n)};
    }

    std::optional<std::string_view> read_nullable_string_view() {
        auto n = read_int16();
        if (n < 0) {
            return std::nullopt;
        }
        return {do_read_string_view(n)};
    }

    void skip_nullable_string() {
        auto n = read_int16();
        if (n > 0) {
            _in.skip(n);
        }
    }

    bytes read_bytes() { return do_read_bytes(read_int32()); }

    bytes_view read_bytes_view() { return do_read_bytes_view(read_int32()); }

    bytes_opt read_nullable_bytes() {
        auto len = read_int32();
        if (len < 0) {
            return std::nullopt;
        }
        return do_read_bytes(len);
    }

    std::optional<bytes_view> read_nullable_bytes_view() {
        auto len = read_int32();
        if (len < 0) {
            return std::nullopt;
        }
        return do_read_bytes_view(len);
    }

    std::optional<iobuf> read_fragmented_nullable_bytes() {
        auto len = read_int32();
        if (len < 0) {
            return std::nullopt;
        }
        auto ret = _io.share(_in.bytes_consumed(), len);
        _in.skip(len);
        return ret;
    }

    template<
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, request_reader&>>
    std::vector<T> read_array(ElementParser&& parser) {
        auto len = read_int32();
        return do_read_array(len, std::forward<ElementParser>(parser));
    }

    template<
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, request_reader&>>
    std::optional<std::vector<T>> read_nullable_array(ElementParser&& parser) {
        auto len = read_int32();
        if (len < 0) {
            return std::nullopt;
        }
        return do_read_array(len, std::forward<ElementParser>(parser));
    }

private:
    sstring do_read_string(int16_t n) {
        if (__builtin_expect(n < 0, false)) {
            /// FIXME: maybe return empty string?
            throw std::out_of_range("Asked to read a negative byte string");
        }
        sstring s(sstring::initialized_later(), n);
        _in.consume_to(n, s.begin());
        validate_utf8(s);
        return s;
    }
    std::string_view string_view_raw(int64_t n) {
        if (_in.segment_bytes_left() >= n) {
            auto ret = std::string_view(_in.begin().get(), n);
            _in.skip(n);
            return ret;
        }
        auto ph = _io.reserve(n);
        auto ret = std::string_view(ph.index(), n);
        _in.consume_to(n, ph);
        return ret;
    }

    std::string_view do_read_string_view(int64_t n) {
        auto s = string_view_raw(n);
        validate_utf8(s);
        return s;
    }

    bytes do_read_bytes(int64_t n) {
        bytes b(bytes::initialized_later(), n);
        _in.consume_to(n, b.begin());
        return b;
    }

    bytes_view do_read_bytes_view(int64_t n) {
        auto s = string_view_raw(n);
        return bytes_view(
          reinterpret_cast<const bytes_view::value_type*>(s.data()), s.size());
    }

    // clang-format off
    template<typename ElementParser,
             typename T = std::invoke_result_t<ElementParser, request_reader&>>
    CONCEPT(requires requires(ElementParser parser, request_reader& rr) {
        { parser(rr) } -> T;
    })
    // clang-format on
    std::vector<T> do_read_array(int32_t len, ElementParser&& parser) {
        std::vector<T> res;
        res.reserve(std::max(0, len));
        while (len-- > 0) {
            res.push_back(parser(*this));
        }
        return res;
    }

    iobuf _io;
    iobuf::iterator_consumer _in;
    /// needed to compute bytes_left()
    size_t _original_size;
};

} // namespace kafka
