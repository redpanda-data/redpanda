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
#include "model/fundamental.h"
#include "model/record_batch_types.h"

#include <cstddef>
#include <cstdint>
#include <ostream>

namespace storage {
// simple types shared among readers and writers

/**
 * Type representing a record key prefixed with batch_type
 */
struct compaction_key : bytes {
    explicit compaction_key(bytes b)
      : bytes(std::move(b)) {}
};

inline compaction_key
prefix_with_batch_type(model::record_batch_type type, bytes_view key) {
    auto bt_le = ss::cpu_to_le(
      static_cast<std::underlying_type<model::record_batch_type>::type>(type));
    auto enriched_key = ss::uninitialized_string<bytes>(
      sizeof(bt_le) + key.size());
    auto out = enriched_key.begin();
    out = std::copy_n(
      reinterpret_cast<const char*>(&bt_le), sizeof(bt_le), out);
    std::copy_n(key.begin(), key.size(), out);

    return compaction_key(std::move(enriched_key));
}

struct compacted_index {
    static constexpr const size_t max_entry_size = size_t(
      std::numeric_limits<uint16_t>::max());

    enum class entry_type : uint8_t {
        none, // error detection
        key,  // most common - just keys
        /// \brief because of raft truncations, we write a truncation, for
        /// the recovery thread to compact up to key-point on the index.
        truncation,
    };
    // bitflags for index
    enum class footer_flags : uint32_t {
        none = 0,
        /// needed for truncation events in the same raft-term
        truncation = 1U,
        /// needed to determine if we should self compact first
        self_compaction = 1U << 1U,
        /// index writer was forced to stop without writing all the entries.
        incomplete = 1U << 2U,
    };
    struct footer {
        // footer versions:
        // 0 - initial version
        // 1 - introduced a key being a tuple of batch_type and the key content
        //  of footer
        // 2 - 64-bit size and keys fields
        static constexpr int8_t current_version = 2;

        uint64_t size{0};
        uint64_t keys{0};
        // must be kept for backwards compatibility with pre-version 2 code
        // (that allows using an index with a version greater than current).
        uint32_t size_deprecated{0};
        uint32_t keys_deprecated{0};
        footer_flags flags{0};
        uint32_t crc{0}; // crc32
        // version *must* be the last field
        int8_t version{current_version};

        static constexpr size_t footer_size = sizeof(size) + sizeof(keys)
                                              + sizeof(size_deprecated)
                                              + sizeof(keys_deprecated)
                                              + sizeof(flags) + sizeof(crc)
                                              + sizeof(version);

        friend std::ostream&
        operator<<(std::ostream& o, const compacted_index::footer& f) {
            return o << "{size:" << f.size << ", keys:" << f.keys
                     << ", flags:" << (uint32_t)f.flags << ", crc:" << f.crc
                     << ", version: " << (int)f.version << "}";
        }
    };

    struct footer_v1 {
        uint32_t size{0};
        uint32_t keys{0};
        footer_flags flags{0};
        uint32_t crc{0}; // crc32
        int8_t version{1};

        static constexpr size_t footer_size = sizeof(size) + sizeof(keys)
                                              + sizeof(flags) + sizeof(crc)
                                              + sizeof(version);
    };

    enum class recovery_state {
        /**
         * Index may be missing when either was deleted or not stored when
         * redpanda crashed
         */
        index_missing,
        /**
         * Index may needs a rebuild when it is corrupted
         */
        index_needs_rebuild,
        /**
         * Segment is already compacted
         */
        already_compacted,
        /**
         * Compaction index is recovered, ready to compaction
         */
        index_recovered
    };

    struct needs_rebuild_error final : public std::runtime_error {
    public:
        explicit needs_rebuild_error(std::string_view msg)
          : std::runtime_error(msg.data()) {}
    };

    // for the readers and friends
    struct entry {
        entry(
          entry_type t, compaction_key k, model::offset o, int32_t d) noexcept
          : type(t)
          , key(std::move(k))
          , offset(o)
          , delta(d) {}

        entry_type type;
        compaction_key key;
        model::offset offset;
        int32_t delta;
    };
};

std::ostream& operator<<(std::ostream&, compacted_index::recovery_state);

[[gnu::always_inline]] inline compacted_index::footer_flags
operator|(compacted_index::footer_flags a, compacted_index::footer_flags b) {
    return compacted_index::footer_flags(
      std::underlying_type_t<compacted_index::footer_flags>(a)
      | std::underlying_type_t<compacted_index::footer_flags>(b));
}

[[gnu::always_inline]] inline void
operator|=(compacted_index::footer_flags& a, compacted_index::footer_flags b) {
    a = (a | b);
}

[[gnu::always_inline]] inline compacted_index::footer_flags
operator~(compacted_index::footer_flags a) {
    return compacted_index::footer_flags(
      ~std::underlying_type_t<compacted_index::footer_flags>(a));
}

[[gnu::always_inline]] inline compacted_index::footer_flags
operator&(compacted_index::footer_flags a, compacted_index::footer_flags b) {
    return compacted_index::footer_flags(
      std::underlying_type_t<compacted_index::footer_flags>(a)
      & std::underlying_type_t<compacted_index::footer_flags>(b));
}

[[gnu::always_inline]] inline void
operator&=(compacted_index::footer_flags& a, compacted_index::footer_flags b) {
    a = (a & b);
}

} // namespace storage
