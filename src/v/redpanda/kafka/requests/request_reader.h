#pragma once

#include "bytes/bytes.h"
#include "bytes/bytes_ostream.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"
#include "utils/fragmented_temporary_buffer.h"
#include "utils/utf8.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <optional>
#include <string_view>
#include <type_traits>

namespace kafka::requests {

class request_reader {
    struct exception_thrower {
        [[noreturn]] [[gnu::cold]] static void
        throw_out_of_range(size_t attempted_read, size_t actual_left) {
            // FIXME: Kafka errors
            throw std::runtime_error(fmt::format(
              "Truncated request: expected {} bytes, length is {}",
              attempted_read,
              actual_left));
        };
    };

    sstring do_read_string(int16_t n) {
        if (n < 0) {
            // FIXME: Kafka errors
        }
        sstring s(sstring::initialized_later(), n);
        _in.read_to(n, s.begin(), exception_thrower());
        validate_utf8(s);
        return s;
    }

    std::string_view do_read_string_view(int16_t n) {
        auto bv = _in.read_bytes_view(
          n, _linearization_buffer, exception_thrower());
        auto s = std::string_view(
          reinterpret_cast<const char*>(bv.data()), bv.size());
        validate_utf8(s);
        return s;
    }

    bytes do_read_bytes(int32_t n) {
        bytes b(bytes::initialized_later(), n);
        _in.read_to(n, b.begin(), exception_thrower());
        return b;
    }

    bytes_view do_read_bytes_view(int32_t n) {
        auto bv = _in.read_bytes_view(
          n, _linearization_buffer, exception_thrower());
        return bv;
    }

public:
    explicit request_reader(fragmented_temporary_buffer::istream in) noexcept
      : _in(std::move(in)) {
    }

    size_t bytes_left() const {
        return _in.bytes_left();
    }

    bytes_view read_raw_bytes_view(size_t n) {
        return _in.read_bytes_view(
          n, _linearization_buffer, exception_thrower());
    }

    bool read_bool() {
        return bool(read_int8());
    }

    int8_t read_int8() {
        return _in.read<int8_t>(exception_thrower());
    }

    int16_t read_int16() {
        return be_to_cpu(_in.read<int16_t>(exception_thrower()));
    }

    int32_t read_int32() {
        return be_to_cpu(_in.read<int32_t>(exception_thrower()));
    }

    int64_t read_int64() {
        return be_to_cpu(_in.read<int64_t>(exception_thrower()));
    }

    uint32_t read_uint32() {
        return be_to_cpu(_in.read<uint32_t>(exception_thrower()));
    }

    int32_t read_varint() {
        return vint::value_type(read_varlong());
    }

    int64_t read_varlong() {
        auto [res, bytes_read] = vlong::deserialize(_in);
        _in.skip(bytes_read);
        return res;
    }

    sstring read_string() {
        return do_read_string(read_int16());
    }

    std::string_view read_string_view() {
        return do_read_string_view(read_int16());
    }

    std::optional<sstring> read_nullable_string() {
        auto n = read_int16();
        if (n < 0) {
            return std::nullopt;
        }
        return {do_read_string(read_int16())};
    }

    std::optional<std::string_view> read_nullable_string_view() {
        auto n = read_int16();
        if (n < 0) {
            return std::nullopt;
        }
        return {do_read_string_view(read_int16())};
    }

    bytes read_bytes() {
        return do_read_bytes(read_int32());
    }

    bytes_view read_bytes_view() {
        return do_read_bytes_view(read_int32());
    }

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

    // clang-format off
    template<typename ElementParser,
             typename T = std::invoke_result_t<ElementParser, request_reader&>>
    CONCEPT(requires requires(ElementParser parser, request_reader& rr) {
        { parser(rr) } -> T;
    })
    // clang-format on
    std::vector<T> read_array(ElementParser&& parser) {
        auto len = read_int32();
        std::vector<T> res;
        res.reserve(std::max(0, len));
        while (len-- > 0) {
            res.push_back(parser(*this));
        }
        return res;
    }

private:
    fragmented_temporary_buffer::istream _in;
    // Buffer into which we linearize a subset of the underlying fragmented
    // buffer, in case the content we're reading spans multiple fragments and
    // we want to return a bytes_view (we can't return one over non-contiguous
    // memory).
    bytes_ostream _linearization_buffer;
};

} // namespace kafka::requests
