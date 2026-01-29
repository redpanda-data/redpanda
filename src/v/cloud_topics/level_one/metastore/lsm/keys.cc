/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/keys.h"

#include "bytes/bytes.h"

#include <seastar/core/byteorder.hh>

#include <boost/algorithm/hex.hpp>

namespace cloud_topics::l1 {

// Fixed-length hex encoding sizes
constexpr size_t enum_hex_len = 2;   // 1 byte = 2 hex chars
constexpr size_t uuid_hex_len = 32;  // 16 bytes = 32 hex chars
constexpr size_t int32_hex_len = 8;  // 4 bytes = 8 hex chars
constexpr size_t int64_hex_len = 16; // 8 bytes = 16 hex chars
constexpr size_t tidp_hex_len = uuid_hex_len + int32_hex_len; // 40 hex chars

constexpr size_t metadata_key_len = enum_hex_len + tidp_hex_len;
constexpr size_t extent_key_len = enum_hex_len + tidp_hex_len + int64_hex_len;
constexpr size_t term_key_len = enum_hex_len + tidp_hex_len + int64_hex_len;
constexpr size_t compaction_key_len = enum_hex_len + tidp_hex_len;
constexpr size_t object_key_len = enum_hex_len + uuid_hex_len;

bytes hex_to_bytes(std::string_view hex) {
    std::vector<uint8_t> rv;
    rv.reserve(hex.size() / 2);
    try {
        boost::algorithm::unhex(hex.begin(), hex.end(), std::back_inserter(rv));
    } catch (...) {
        return bytes{};
    }
    return bytes{rv.data(), rv.size()};
}

// Write hex-encoded numeric value at specified position.
// Returns the number of characters written.
template<typename T>
size_t encode_numeric_at(ss::sstring& out, size_t pos, T value) {
    T be_value;
    if constexpr (sizeof(T) == 1) {
        be_value = value;
    } else {
        be_value = ss::cpu_to_be(value);
    }
    auto buf = std::bit_cast<std::array<uint8_t, sizeof(be_value)>>(be_value);
    return to_hex(out, pos, bytes_view(buf.data(), sizeof(buf)));
}

template<typename T>
std::optional<T> decode_numeric(std::string_view hex) {
    constexpr size_t expected_hex_len = sizeof(T) * 2;
    if (hex.size() != expected_hex_len) {
        return std::nullopt;
    }
    auto b = hex_to_bytes(hex);
    if (b.size() != sizeof(T)) {
        return std::nullopt;
    }
    T be_value;
    std::memcpy(&be_value, b.data(), sizeof(T));
    if constexpr (sizeof(T) == 1) {
        return be_value;
    } else {
        return ss::be_to_cpu(be_value);
    }
}

size_t encode_uuid_at(ss::sstring& out, size_t pos, const uuid_t& uuid) {
    auto vec = uuid.to_vector();
    return to_hex(out, pos, bytes_view(vec.data(), vec.size()));
}

std::optional<uuid_t> decode_uuid(std::string_view hex) {
    if (hex.size() != uuid_hex_len) {
        return std::nullopt;
    }
    auto b = hex_to_bytes(hex);
    if (b.size() != 16) {
        return std::nullopt;
    }
    try {
        std::vector<uint8_t> vec(b.begin(), b.end());
        return uuid_t(vec);
    } catch (...) {
        return std::nullopt;
    }
}

size_t encode_tidp_at(
  ss::sstring& out, size_t pos, const model::topic_id_partition& tidp) {
    size_t written = 0;
    written += encode_uuid_at(out, pos + written, tidp.topic_id());
    written += encode_numeric_at(out, pos + written, tidp.partition());
    return written;
}

std::optional<model::topic_id_partition> decode_tidp(std::string_view s) {
    if (s.size() < tidp_hex_len) {
        return std::nullopt;
    }
    auto tid_opt = decode_uuid(s.substr(0, uuid_hex_len));
    if (!tid_opt) {
        return std::nullopt;
    }
    auto pid_opt = decode_numeric<int32_t>(
      s.substr(uuid_hex_len, int32_hex_len));
    if (!pid_opt) {
        return std::nullopt;
    }
    return model::topic_id_partition(
      model::topic_id(*tid_opt), model::partition_id(*pid_opt));
}

std::optional<metadata_row_key> metadata_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + tidp_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }
    auto enum_opt = decode_numeric<row_type>(s.substr(0, enum_hex_len));
    if (!enum_opt || *enum_opt != row_type::metadata) {
        return std::nullopt;
    }
    auto tidp = decode_tidp(s.substr(enum_hex_len));
    if (!tidp) {
        return std::nullopt;
    }
    return metadata_row_key{
      .tidp = *tidp,
    };
}

ss::sstring metadata_row_key::encode(const model::topic_id_partition& tidp) {
    ss::sstring result(ss::sstring::initialized_later{}, metadata_key_len);
    size_t pos = 0;
    pos += encode_numeric_at(result, pos, row_type::metadata);
    pos += encode_tidp_at(result, pos, tidp);
    return result;
}

std::optional<extent_row_key> extent_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + tidp_hex_len + int64_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }
    auto enum_opt = decode_numeric<row_type>(s.substr(0, enum_hex_len));
    if (!enum_opt || *enum_opt != row_type::extent) {
        return std::nullopt;
    }
    auto tidp = decode_tidp(s.substr(enum_hex_len));
    if (!tidp) {
        return std::nullopt;
    }
    auto offset_opt = decode_numeric<int64_t>(
      s.substr(enum_hex_len + tidp_hex_len));
    if (!offset_opt) {
        return std::nullopt;
    }
    return extent_row_key{
      .tidp = *tidp,
      .base_offset = kafka::offset(*offset_opt),
    };
}

ss::sstring extent_row_key::encode(
  const model::topic_id_partition& tidp, kafka::offset base_offset) {
    ss::sstring result(ss::sstring::initialized_later{}, extent_key_len);
    size_t pos = 0;
    pos += encode_numeric_at(result, pos, row_type::extent);
    pos += encode_tidp_at(result, pos, tidp);
    pos += encode_numeric_at(result, pos, base_offset());
    return result;
}

std::optional<term_row_key> term_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + tidp_hex_len + int64_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }
    auto enum_opt = decode_numeric<row_type>(s.substr(0, enum_hex_len));
    if (!enum_opt || *enum_opt != row_type::term_start) {
        return std::nullopt;
    }
    auto tidp = decode_tidp(s.substr(enum_hex_len));
    if (!tidp) {
        return std::nullopt;
    }
    auto term_opt = decode_numeric<int64_t>(
      s.substr(enum_hex_len + tidp_hex_len));
    if (!term_opt) {
        return std::nullopt;
    }
    return term_row_key{
      .tidp = *tidp,
      .term = model::term_id(*term_opt),
    };
}

ss::sstring term_row_key::encode(
  const model::topic_id_partition& tidp, model::term_id term) {
    ss::sstring result(ss::sstring::initialized_later{}, term_key_len);
    size_t pos = 0;
    pos += encode_numeric_at(result, pos, row_type::term_start);
    pos += encode_tidp_at(result, pos, tidp);
    pos += encode_numeric_at(result, pos, static_cast<int64_t>(term()));
    return result;
}

std::optional<compaction_row_key>
compaction_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + tidp_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }
    auto enum_opt = decode_numeric<row_type>(s.substr(0, enum_hex_len));
    if (!enum_opt || *enum_opt != row_type::compaction) {
        return std::nullopt;
    }
    auto tidp = decode_tidp(s.substr(enum_hex_len));
    if (!tidp) {
        return std::nullopt;
    }
    return compaction_row_key{
      .tidp = *tidp,
    };
}

ss::sstring compaction_row_key::encode(const model::topic_id_partition& tidp) {
    ss::sstring result(ss::sstring::initialized_later{}, compaction_key_len);
    size_t pos = 0;
    pos += encode_numeric_at(result, pos, row_type::compaction);
    pos += encode_tidp_at(result, pos, tidp);
    return result;
}

std::optional<object_row_key> object_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + uuid_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }
    auto enum_opt = decode_numeric<row_type>(s.substr(0, enum_hex_len));
    if (!enum_opt || *enum_opt != row_type::object) {
        return std::nullopt;
    }
    auto uuid_opt = decode_uuid(s.substr(enum_hex_len));
    if (!uuid_opt) {
        return std::nullopt;
    }
    return object_row_key{
      .oid = object_id(*uuid_opt),
    };
}

ss::sstring object_row_key::encode(const object_id& oid) {
    ss::sstring result(ss::sstring::initialized_later{}, object_key_len);
    size_t pos = 0;
    pos += encode_numeric_at(result, pos, row_type::object);
    pos += encode_uuid_at(result, pos, oid());
    return result;
}

} // namespace cloud_topics::l1
