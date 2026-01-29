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

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "base/vassert.h"
#include "lsm/core/keys.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <string_view>

class iobuf;

namespace lsm::internal {

// The sequence number for a write into the database.
using sequence_number = named_type<uint64_t, struct seqno_tag>;

consteval sequence_number operator""_seqno(unsigned long long val) {
    return sequence_number{val};
}

// The type of the key
enum class value_type : uint8_t {
    // Value is a regular value.
    value = 0,
    // Value is a tombstone.
    tombstone = 1,
};

// And internal key is an encoded key for internal DB usage.
//
// It is made up of three parts:
//  1. The user key, which must NOT contain a `null` byte.
//  2. The sequence number, which is the offset in the log.
//  3. The value type, which is either a regular value or a tombstone.
//
// Internal keys are encoded in a way that allows them to be compared
// lexicographically, in the following manner: key ASC, seqno DESC, type DESC
class key {
    constexpr static size_t sso_size = 15;
    using underlying_t = ss::basic_sstring<char, uint32_t, sso_size, false>;

    explicit key(underlying_t v)
      : _value(std::move(v)) {}

public:
    struct parts {
        user_key_view key;
        sequence_number seqno = sequence_number(0);
        value_type type = value_type::value;

        // Create a value internal key
        static parts value(user_key_view key, sequence_number);
        // Create a tombstone internal key
        static parts tombstone(user_key_view key, sequence_number);
        bool operator==(const parts& other) const = default;
        fmt::iterator format_to(fmt::iterator) const;
    };

    key() = default;
    explicit key(const iobuf& encoded);

    // Encode a key into an internal key.
    static key encode(parts);
    // Decode a key into its parts.
    parts decode() const;
    // Returns this key's value type
    value_type type() const;
    bool is_tombstone() const { return type() == value_type::tombstone; }
    bool is_value() const { return type() == value_type::value; }
    sequence_number seqno() const;

    const char& operator[](size_t i) const { return _value[i]; }
    const char* data() const { return _value.data(); }
    size_t size() const { return _value.size(); }
    size_t memory_usage() const {
        auto usage = sizeof(*this);
        if (_value.size() > sso_size) {
            usage += _value.size();
        }
        return usage;
    }
    bool empty() const { return _value.empty(); }

    // Returns the user portion of the key.
    user_key_view user_key() const {
        dassert(
          _value.size() >= 5,
          "expected key size to be at least 5, got: {}",
          _value.size());
        return user_key_view{
          _value.data(), _value.size() - sizeof(uint64_t) - 1};
    }

    bool operator==(const key& other) const = default;
    auto operator<=>(const key&) const = default;
    bool operator<(const key&) const = default;
    explicit operator ss::sstring() const { return {data(), size()}; }
    explicit operator iobuf() const;

    fmt::iterator format_to(fmt::iterator) const;

private:
    friend class key_view;

    underlying_t _value;
};

// An internal key view is a lightweight view of an internal key that does not
// own the data.
class key_view {
public:
    struct parts {
        user_key_view key;
        sequence_number seqno = sequence_number(0);
        value_type type = value_type::value;

        bool operator==(const parts& other) const = default;
        fmt::iterator format_to(fmt::iterator) const;
        explicit operator key::parts() const;
    };
    key_view() = default;
    // Convert an owned key into a view.
    key_view(const key& k) // NOLINT(*explicit-conversions*)
      : _value(k._value.data(), k._value.size()) {}

    // Create a view from an already encoded string.
    static key_view from_encoded(std::string_view v) { return key_view{v}; }

    const char* data() const { return _value.data(); }
    size_t size() const { return _value.size(); }
    bool empty() const { return _value.empty(); }

    // The user portion of the key.
    user_key_view user_key() const {
        dassert(
          _value.size() >= 5,
          "expected key size to be at least 5, got: {}",
          _value.size());
        return user_key_view{
          _value.data(), _value.size() - sizeof(uint64_t) - 1};
    }
    // Returns this key's value type
    value_type type() const;
    bool is_tombstone() const { return type() == value_type::tombstone; }
    bool is_value() const { return type() == value_type::value; }
    sequence_number seqno() const { return decode().seqno; }
    // Decode a key into its parts.
    parts decode() const;

    // A key view without the tailing type marker so that the value sorts before
    // all types. This can be used to get the lexicographically first key at or
    // after a specific seqno.
    //
    // DO NOT TRY AND DECODE THIS KEY OR DO ANYTHING BUT USE IT FOR COMPARISON.
    internal::key_view without_type() const;

    // Make a copy of the view as an internal_key.
    explicit operator key() const {
        key k;
        k._value = key::underlying_t(_value);
        return k;
    }
    // This internal key as a string view.
    explicit operator std::string_view() const { return _value; }

    bool operator==(const key_view& other) const = default;
    auto operator<=>(const key_view&) const = default;
    bool operator<(const key_view&) const = default;
    fmt::iterator format_to(fmt::iterator) const;

private:
    explicit key_view(std::string_view v)
      : _value(v) {}

    std::string_view _value;
};

// Create a key from a static string. Used for testing.
//
// The grammar is: "<userkey>@<seqno>?"
// Where:
//  userkey: is the user key. Cannot contain a @
//  seqno: the absolute value is the seqno for the key
//  value_type: if seqno is negative, then it's a tombstone type.
//              Otherwise it's a value type.
key operator""_key(const char* s, size_t len);

// Create a key for seeks.
key operator""_seek_key(const char* s, size_t len);

} // namespace lsm::internal
