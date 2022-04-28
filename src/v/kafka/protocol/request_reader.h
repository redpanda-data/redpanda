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
#include "bytes/iobuf_parser.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/types.h"
#include "likely.h"
#include "seastarx.h"
#include "utils/utf8.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <optional>
#include <type_traits>

namespace kafka {

class request_reader {
public:
    explicit request_reader(iobuf io) noexcept
      : _parser(std::move(io)) {}

    size_t bytes_left() const { return _parser.bytes_left(); }
    size_t bytes_consumed() const { return _parser.bytes_consumed(); }
    bool read_bool() { return _parser.read_bool(); }
    int8_t read_int8() { return _parser.consume_type<int8_t>(); }
    int16_t read_int16() { return _parser.consume_be_type<int16_t>(); }
    int32_t read_int32() { return _parser.consume_be_type<int32_t>(); }
    int64_t read_int64() { return _parser.consume_be_type<int64_t>(); }
    int32_t read_varint() { return static_cast<int32_t>(read_varlong()); }
    int64_t read_varlong() {
        auto [i, _] = _parser.read_varlong();
        return i;
    }
    uint32_t read_unsigned_varint() {
        auto [i, _] = _parser.read_unsigned_varint();
        return i;
    }

    ss::sstring read_string() { return do_read_string(read_int16()); }

    ss::sstring read_flex_string() {
        return do_read_flex_string(read_unsigned_varint());
    }

    std::optional<ss::sstring> read_nullable_string() {
        auto n = read_int16();
        if (n < 0) {
            return std::nullopt;
        }
        return {do_read_string(n)};
    }

    std::optional<ss::sstring> read_nullable_flex_string() {
        auto n = read_unsigned_varint();
        if (n == 0) {
            return std::nullopt;
        }
        return {do_read_flex_string(n)};
    }

    bytes read_bytes() { return _parser.read_bytes(read_int32()); }

    bytes read_flex_bytes() {
        auto n = read_unsigned_varint();
        if (unlikely(n == 0)) {
            throw std::out_of_range("Asked to read a negative byte string");
        }
        return _parser.read_bytes(n - 1);
    }

    // Stronly suggested to use read_nullable_iobuf
    std::optional<iobuf> read_fragmented_nullable_bytes() {
        auto [io, count] = read_nullable_iobuf();
        if (count < 0) {
            return std::nullopt;
        }
        return std::move(io);
    }

    std::optional<iobuf> read_fragmented_nullable_flex_bytes() {
        auto len = read_unsigned_varint();
        if (len == 0) {
            return std::nullopt;
        }
        auto ret = _parser.share(len - 1);
        return iobuf{std::move(ret)};
    }

    std::pair<iobuf, int32_t> read_nullable_iobuf() {
        auto len = read_int32();
        if (len < 0) {
            return {iobuf(), len};
        }
        auto ret = _parser.share(len);
        return {std::move(ret), len};
    }

    std::optional<batch_reader> read_nullable_batch_reader() {
        auto io = read_fragmented_nullable_bytes();
        if (!io) {
            return std::nullopt;
        }
        return batch_reader(std::move(*io));
    }

    std::optional<batch_reader> read_nullable_flex_batch_reader() {
        auto io = read_fragmented_nullable_flex_bytes();
        if (!io) {
            return std::nullopt;
        }
        return batch_reader(std::move(*io));
    }

    template<
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, request_reader&>>
    std::vector<T> read_array(ElementParser&& parser) {
        auto len = read_int32();
        if (len < 0) {
            throw std::out_of_range(
              "Attempt to read array with negative length");
        }
        return do_read_array(len, std::forward<ElementParser>(parser));
    }

    template<
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, request_reader&>>
    std::vector<T> read_flex_array(ElementParser&& parser) {
        auto len = read_unsigned_varint();
        if (len == 0) {
            throw std::out_of_range(
              "Attempt to read non-null flex array with 0 length");
        }
        return do_read_array(len - 1, std::forward<ElementParser>(parser));
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

    template<
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, request_reader&>>
    std::optional<std::vector<T>>
    read_nullable_flex_array(ElementParser&& parser) {
        auto len = read_unsigned_varint();
        if (len == 0) {
            return std::nullopt;
        }
        return do_read_array(len - 1, std::forward<ElementParser>(parser));
    }

    // Only relevent when reading flex requests
    tagged_fields read_tags() {
        tagged_fields tags;
        auto num_tags = read_unsigned_varint(); // consume total num of tags
        while (num_tags-- > 0) {
            auto tag_id = read_unsigned_varint(); // consume tag id
            auto size = read_unsigned_varint();   // consume size in bytes
            tags.emplace_back(tag_id, _parser.share(size)); // consume tag
        }
        return tags;
    }

    void consume_tags() {
        // Reads tags only with the intention of moving ahead the parser read
        // head to the next correct index
        auto num_tags = read_unsigned_varint(); // consume total num of tags
        while (num_tags-- > 0) {
            (void)read_unsigned_varint();           // consume tag id
            auto next_len = read_unsigned_varint(); // consume size in bytes
            _parser.skip(next_len);                 // consume tag element
        }
    }

private:
    ss::sstring do_read_string(int16_t n) {
        if (unlikely(n < 0)) {
            throw std::out_of_range("Asked to read a negative byte string");
        }
        return _parser.read_string(n);
    }

    ss::sstring do_read_flex_string(uint32_t n) {
        if (unlikely(n == 0)) {
            throw std::out_of_range("Asked to read a 0 byte flex string");
        }
        return _parser.read_string(n - 1);
    }

    template<
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, request_reader&>>
    requires requires(ElementParser parser, request_reader& rr) {
        { parser(rr) } -> std::same_as<T>;
    }
    std::vector<T> do_read_array(int32_t len, ElementParser&& parser) {
        if (len < 0) {
            throw std::out_of_range("Attempt to parse array w/ negative len");
        }
        std::vector<T> res;
        res.reserve(len);
        while (len-- > 0) {
            res.push_back(parser(*this));
        }
        return res;
    }

    iobuf_parser _parser;
};

} // namespace kafka
