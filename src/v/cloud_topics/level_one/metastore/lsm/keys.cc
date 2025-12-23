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

// Helper to convert hex string to bytes
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

// Generic helper to encode numeric types as big-endian hex
template<typename T>
ss::sstring encode_numeric(T value) {
    T be_value;
    if constexpr (sizeof(T) == 1) {
        be_value = value;
    } else {
        be_value = ss::cpu_to_be(value);
    }
    std::array<uint8_t, sizeof(be_value)> buf;
    std::memcpy(buf.data(), &be_value, sizeof(be_value));
    return to_hex(bytes_view(buf.data(), sizeof(buf)));
}

// Generic helper to decode numeric types from big-endian hex
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

// Helper to encode enum as fixed-length hex
ss::sstring encode_enum(row_type type) {
    return encode_numeric<row_type>(type);
}

// Helper to decode enum from hex
std::optional<row_type> decode_enum(std::string_view hex) {
    return decode_numeric<row_type>(hex);
}

// Helper to encode UUID as fixed-length hex
ss::sstring encode_uuid(const uuid_t& uuid) {
    auto vec = uuid.to_vector();
    return to_hex(bytes_view(vec.data(), vec.size()));
}

// Helper to decode UUID from hex
std::optional<uuid_t> decode_uuid(std::string_view hex) {
    constexpr size_t uuid_hex_len = sizeof(uuid_t) * 2;
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

// Helper to encode int32 as fixed-length hex (big-endian)
ss::sstring encode_int32(int32_t value) {
    return encode_numeric<int32_t>(value);
}

// Helper to decode int32 from hex (big-endian)
std::optional<int32_t> decode_int32(std::string_view hex) {
    return decode_numeric<int32_t>(hex);
}

// Helper to encode int64 as fixed-length hex (big-endian)
ss::sstring encode_int64(int64_t value) {
    return encode_numeric<int64_t>(value);
}

// Helper to decode int64 from hex (big-endian)
std::optional<int64_t> decode_int64(std::string_view hex) {
    return decode_numeric<int64_t>(hex);
}

// Helper to encode topic_id_partition
ss::sstring encode_tidp(const model::topic_id_partition& tidp) {
    return encode_uuid(tidp.topic_id()) + encode_int32(tidp.partition());
}

// Helper to decode topic_id_partition
std::optional<model::topic_id_partition> decode_tidp(std::string_view s) {
    if (s.size() < tidp_hex_len) {
        return std::nullopt;
    }

    auto uuid_hex = s.substr(0, uuid_hex_len);
    auto tid_opt = decode_uuid(uuid_hex);
    if (!tid_opt) {
        return std::nullopt;
    }

    auto int32_hex = s.substr(uuid_hex_len, int32_hex_len);
    auto pid_opt = decode_int32(int32_hex);
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

    auto enum_opt = decode_enum(s.substr(0, enum_hex_len));
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
    return encode_enum(row_type::metadata) + encode_tidp(tidp);
}

std::optional<extent_row_key> extent_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + tidp_hex_len + int64_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }

    auto enum_opt = decode_enum(s.substr(0, enum_hex_len));
    if (!enum_opt || *enum_opt != row_type::extent) {
        return std::nullopt;
    }

    auto tidp = decode_tidp(s.substr(enum_hex_len));
    if (!tidp) {
        return std::nullopt;
    }

    auto offset_opt = decode_int64(s.substr(enum_hex_len + tidp_hex_len));
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
    return encode_enum(row_type::extent) + encode_tidp(tidp)
           + encode_int64(base_offset());
}

std::optional<term_row_key> term_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + tidp_hex_len + int64_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }

    auto enum_opt = decode_enum(s.substr(0, enum_hex_len));
    if (!enum_opt || *enum_opt != row_type::term_start) {
        return std::nullopt;
    }

    auto tidp = decode_tidp(s.substr(enum_hex_len));
    if (!tidp) {
        return std::nullopt;
    }

    auto term_opt = decode_int64(s.substr(enum_hex_len + tidp_hex_len));
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
    return encode_enum(row_type::term_start) + encode_tidp(tidp)
           + encode_int64(term());
}

std::optional<compaction_row_key>
compaction_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + tidp_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }

    auto enum_opt = decode_enum(s.substr(0, enum_hex_len));
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
    return encode_enum(row_type::compaction) + encode_tidp(tidp);
}

std::optional<object_row_key> object_row_key::decode(std::string_view s) {
    constexpr size_t expected_len = enum_hex_len + uuid_hex_len;
    if (s.size() < expected_len) {
        return std::nullopt;
    }

    auto enum_opt = decode_enum(s.substr(0, enum_hex_len));
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
    return encode_enum(row_type::object) + encode_uuid(oid());
}

} // namespace cloud_topics::l1
