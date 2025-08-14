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

#include "absl/time/time.h"
#include "base/units.h"
#include "bytes/iobuf_parser.h"
#include "container/chunked_vector.h"
#include "serde/protobuf/field_mask.h"
#include "serde/protobuf/wire_format.h"
#include "utils/fixed_string.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace serde::pb {

/**
 * This is a set of helper methods to read the protobuf wire format that is used
 * in our code generate to cut down on boilerplate.
 *
 * It also enforces max recursion depth to prevent stack overflows.
 */
class wire_format_parser {
    constexpr static size_t max_depth = 64;

public:
    explicit wire_format_parser(iobuf b, size_t depth = 0)
      : _parser(std::move(b))
      , _depth(depth) {}

    size_t bytes_left() const { return _parser.bytes_left(); }

    void check_empty() {
        if (_parser.bytes_left() != 0) {
            throw std::runtime_error(fmt::format(
              "expected empty parser, but {} bytes left",
              _parser.bytes_left()));
        }
    }

    tag read_tag() { return tag::read(&_parser); }

    template<
      fixed_string full_name,
      typename T,
      zigzag needs_decoding = std::is_signed_v<T> ? zigzag::yes : zigzag::no>
    T read_singular_varint(tag t) {
        if (t.wire_type == wire_type::varint) [[likely]] {
            return read_varint<T, needs_decoding>(&_parser);
        } else if (t.wire_type == wire_type::length) {
            auto length = read_length(&_parser);
            auto target = _parser.bytes_consumed() + length;
            T last{};
            while (_parser.bytes_consumed() < target) {
                last = read_varint<T, needs_decoding>(&_parser);
            }
            return last;
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name, typename T>
    T read_singular_enum(tag t) {
        if (t.wire_type == wire_type::varint) [[likely]] {
            T v;
            enum_from_proto(&_parser, &v);
            return v;
        } else if (t.wire_type == wire_type::length) {
            auto length = read_length(&_parser);
            auto target = _parser.bytes_consumed() + length;
            T last{};
            while (_parser.bytes_consumed() < target) {
                enum_from_proto(&_parser, &last);
            }
            return last;
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name>
    bool read_singular_bool(tag t) {
        return static_cast<bool>(read_singular_varint<full_name, int32_t>(t));
    }

    template<
      fixed_string full_name,
      typename T,
      zigzag needs_decoding = std::is_signed_v<T> ? zigzag::yes : zigzag::no>
    ss::future<> read_repeated_varint(tag t, chunked_vector<T>* out) {
        if (t.wire_type == wire_type::varint) {
            out->push_back(read_varint<T, needs_decoding>(&_parser));
        } else if (t.wire_type == wire_type::length) {
            auto length = read_length(&_parser);
            auto target = _parser.bytes_consumed() + length;
            while (_parser.bytes_consumed() < target) {
                out->push_back(read_varint<T, needs_decoding>(&_parser));
                co_await ss::coroutine::maybe_yield();
            }
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name, typename T>
    ss::future<> read_repeated_enum(tag t, chunked_vector<T>* out) {
        if (t.wire_type == wire_type::varint) {
            enum_from_proto(&_parser, &out->emplace_back());
        } else if (t.wire_type == wire_type::length) {
            auto length = read_length(&_parser);
            auto target = _parser.bytes_consumed() + length;
            while (_parser.bytes_consumed() < target) {
                enum_from_proto(&_parser, &out->emplace_back());
                co_await ss::coroutine::maybe_yield();
            }
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name>
    ss::future<> read_repeated_bool(tag t, chunked_vector<bool>* out) {
        if (t.wire_type == wire_type::varint) {
            out->push_back(
              static_cast<int32_t>(read_varint<int32_t>(&_parser)));
        } else if (t.wire_type == wire_type::length) {
            auto length = read_length(&_parser);
            auto target = _parser.bytes_consumed() + length;
            while (_parser.bytes_consumed() < target) {
                out->push_back(
                  static_cast<int32_t>(read_varint<int32_t>(&_parser)));
                co_await ss::coroutine::maybe_yield();
            }
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name, typename T>
    T read_fixed(tag t) {
        constexpr static wire_type expected = sizeof(T) == sizeof(uint32_t)
                                                ? wire_type::i32
                                                : wire_type::i64;
        if (t.wire_type == expected) [[likely]] {
            return _parser.consume_type<T>();
        } else if (t.wire_type == wire_type::length) {
            auto length = read_length(&_parser);
            auto target = _parser.bytes_consumed() + length;
            T last{};
            while (_parser.bytes_consumed() < target) {
                last = _parser.consume_type<T>();
            }
            return last;
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name, typename T>
    ss::future<> read_repeated_fixed(tag t, chunked_vector<T>* out) {
        constexpr static wire_type expected = sizeof(T) == sizeof(uint32_t)
                                                ? wire_type::i32
                                                : wire_type::i64;
        if (t.wire_type == expected) {
            out->push_back(_parser.consume_type<T>());
        } else if (t.wire_type == wire_type::length) {
            auto length = read_length(&_parser);
            auto target = _parser.bytes_consumed() + length;
            while (_parser.bytes_consumed() < target) {
                out->push_back(_parser.consume_type<T>());
                co_await ss::coroutine::maybe_yield();
            }
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name>
    ss::sstring read_string(tag t) {
        if (t.wire_type == wire_type::length) [[likely]] {
            auto length = read_length(&_parser);
            constexpr size_t allocation_limit = 128_KiB;
            if (std::cmp_greater(length, allocation_limit)) {
                throw std::runtime_error(fmt::format(
                  "string length {} exceeds allocation limit {} for field {}",
                  length,
                  allocation_limit,
                  full_name));
            }
            return _parser.read_string(length);
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name>
    iobuf read_bytes(tag t) {
        if (t.wire_type == wire_type::length) [[likely]] {
            auto length = read_length(&_parser);
            return _parser.share(length);
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name>
    wire_format_parser read_message(tag t) {
        if (t.wire_type == wire_type::length) [[likely]] {
            auto length = read_length(&_parser);
            if (_depth >= max_depth) {
                throw std::runtime_error(fmt::format(
                  "max depth {} exceeded for field {}", _depth, full_name));
            }
            return wire_format_parser(_parser.share(length), _depth + 1);
        } else [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "unexpected wire type {} for field {}",
              std::to_underlying(t.wire_type),
              full_name));
        }
    }

    template<fixed_string full_name>
    absl::Duration read_wellknown_duration(tag t) {
        auto parser = read_message<full_name>(t);
        int64_t seconds = 0;
        int32_t nanos = 0;
        while (parser.bytes_left() > 0) {
            auto tag = parser.read_tag();
            if (tag.field_number == 1) {
                seconds
                  = parser.template read_singular_varint<full_name, int64_t>(
                    tag);
            } else if (tag.field_number == 2) {
                nanos
                  = parser.template read_singular_varint<full_name, int32_t>(
                    tag);
            } else {
                parser.skip_unknown(tag);
            }
        }
        return absl::Seconds(seconds) + absl::Nanoseconds(nanos);
    }

    template<fixed_string full_name>
    absl::Time read_wellknown_timestamp(tag t) {
        return absl::UnixEpoch() + read_wellknown_duration<full_name>(t);
    }

    template<fixed_string full_name>
    field_mask read_wellknown_field_mask(tag t) {
        auto parser = read_message<full_name>(t);
        field_mask mask;
        while (parser.bytes_left() > 0) {
            auto tag = parser.read_tag();
            if (tag.field_number == 1) {
                mask.paths.push_back(
                  parser.template read_string<full_name>(tag));
            } else {
                parser.skip_unknown(tag);
            }
        }
        return mask;
    }

    void skip_unknown(tag t) { skip_unknown_field(&_parser, t.wire_type); }

private:
    iobuf_parser _parser;
    size_t _depth;
};

namespace wellknown {

inline iobuf duration_to_proto(absl::Duration d) {
    iobuf buf;
    // seconds
    serde::pb::tag::write(
      {.wire_type = serde::pb::wire_type::varint, .field_number = 1}, &buf);
    serde::pb::write_varint(d / absl::Seconds(1), &buf);
    // nanos
    serde::pb::tag::write(
      {.wire_type = serde::pb::wire_type::varint, .field_number = 2}, &buf);
    serde::pb::write_varint(
      static_cast<int32_t>((d % absl::Seconds(1)) / absl::Nanoseconds(1)),
      &buf);
    return buf;
}

inline iobuf timestamp_to_proto(absl::Time t) {
    return duration_to_proto(t - absl::UnixEpoch());
}

inline iobuf field_mask_to_proto(const field_mask& mask) {
    iobuf buf;
    // paths
    for (const auto& path : mask.paths) {
        serde::pb::tag::write(
          {.wire_type = serde::pb::wire_type::length, .field_number = 1}, &buf);
        serde::pb::write_length(path.size(), &buf);
        buf.append_str(path);
    }
    return buf;
}

} // namespace wellknown

} // namespace serde::pb
