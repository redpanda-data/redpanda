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

#include "lsm/core/internal/keys.h"

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "base/vassert.h"
#include "bytes/iobuf.h"

#include <seastar/core/byteorder.hh>

#include <algorithm>
#include <limits>
#include <type_traits>
#include <utility>

namespace lsm::internal {

key key::encode(parts p) {
    internal::key::underlying_t v(
      underlying_t::initialized_later{}, p.key().size() + 1 + sizeof(uint64_t));
    dassert(
      std::ranges::find(p.key(), '\0') == p.key().end(),
      "key must not contain null characters");
    // First we append the user key.
    std::ranges::copy(p.key(), v.data());
    // Then we null terminate, so that keys of different lengths compare
    // lexicographically.
    v[p.key().size()] = '\0';
    // Next we want to encode the sequence number and type, and have them sort
    // in descending order for the same key. Use 7 bytes for the offset, since
    // that should give us plenty of values before overflow.
    // Encode in BE form so the values sort lexicographically, then invert the
    // bits so they sort in descending order.
    uint64_t encoded = (p.seqno() << CHAR_WIDTH)
                       | static_cast<uint64_t>(p.type);
    encoded = ss::cpu_to_be(~encoded);
    std::memcpy(&v[p.key().size() + 1], &encoded, sizeof(encoded));
    return key{v};
}

key::parts key::decode() const {
    key_view view{*this};
    return key::parts(view.decode());
}
value_type key::type() const { return key_view{*this}.type(); }

fmt::iterator key::parts::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "internal_key_parts={{key={},seqno={},type={}}}",
      key,
      seqno,
      std::to_underlying(type));
}

fmt::iterator key_view::parts::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "internal_key_parts={{key={},seqno={},type={}}}",
      key,
      seqno,
      std::to_underlying(type));
}

fmt::iterator key::format_to(fmt::iterator it) const {
    uint64_t encoded; // NOLINT
    std::memcpy(
      &encoded, &_value[_value.size() - sizeof(encoded)], sizeof(encoded));
    encoded = ~ss::be_to_cpu(encoded);
    return fmt::format_to(
      it, "internal_key={{user={},suffix=o{:08o}}}", user_key(), encoded);
}

fmt::iterator key_view::format_to(fmt::iterator it) const {
    uint64_t encoded; // NOLINT
    std::memcpy(
      &encoded, &_value[_value.size() - sizeof(encoded)], sizeof(encoded));
    encoded = ~ss::be_to_cpu(encoded);
    return fmt::format_to(
      it, "internal_key_view={{user={},suffix=o{:08o}}}", user_key(), encoded);
}

key::parts key::parts::value(user_key_view key, sequence_number seq_num) {
    return {.key = key, .seqno = seq_num, .type = value_type::value};
}
key::parts key::parts::tombstone(user_key_view key, sequence_number seq_num) {
    return {.key = key, .seqno = seq_num, .type = value_type::tombstone};
}

key_view::parts key_view::decode() const {
    key_view::parts p;
    // The user key is everything up to the last byte, which is the sequence
    // number and type.
    p.key = user_key();
    // The sequence number is the rest of the bytes, which we decode from BE
    // form and invert the bits.
    uint64_t encoded; // NOLINT
    std::memcpy(&encoded, &_value[p.key().size() + 1], sizeof(encoded));
    encoded = ~ss::be_to_cpu(encoded);
    // The last byte is the type.
    p.type = static_cast<value_type>(
      encoded & std::numeric_limits<std::underlying_type_t<value_type>>::max());
    // Shift to get back the seqno, which is the rest of the bits.
    p.seqno = sequence_number(encoded >> CHAR_WIDTH);
    return p;
}

value_type key_view::type() const {
    auto v = static_cast<std::underlying_type_t<value_type>>(_value.back());
    v = ~v;
    return value_type{v};
}

key_view::parts::operator key::parts() const {
    return {.key = key, .seqno = seqno, .type = type};
}

internal::key_view key_view::without_type() const {
    auto str = std::string_view(*this);
    str.remove_suffix(1);
    return internal::key_view{str};
}

key::key(const iobuf& encoded)
  : _value(underlying_t::initialized_later{}, encoded.size_bytes()) {
    auto it = _value.begin();
    for (const auto& frag : encoded) {
        it = std::copy_n(frag.get(), frag.size(), it);
    }
}

key::operator iobuf() const { return iobuf::from(_value); }

key operator""_seek_key(const char* s, size_t len) {
    return key::encode({
      .key = user_key_view(std::string_view(s, len)),
      .seqno = sequence_number::max(),
      .type = value_type::value,
    });
}

key operator""_key(const char* s, size_t len) {
    auto [k, seq_str] = std::pair<std::string_view, std::string_view>(
      absl::StrSplit(std::string_view{s, len}, "@"));
    int64_t seq_num = 0;
    using namespace lsm::internal;
    if (!absl::SimpleAtoi(seq_str, &seq_num)) {
        seq_num = 0; // Default seqno
    }
    return key::encode({
      .key = user_key_view(k),
      .seqno = sequence_number(std::abs(seq_num)),
      .type = seq_num < 0 ? value_type::tombstone : value_type::value,
    });
}

sequence_number key::seqno() const { return key_view{*this}.decode().seqno; }

} // namespace lsm::internal
