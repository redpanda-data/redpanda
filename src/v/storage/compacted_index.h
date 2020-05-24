#pragma once
#include "bytes/bytes.h"
#include "model/fundamental.h"

#include <cstddef>
#include <cstdint>
#include <ostream>

namespace storage {
// simple types shared among readers and writers

struct compacted_index {
    static constexpr size_t footer_size = 13;
    enum class entry_type : uint8_t {
        key,
        /// \brief because of raft truncations, we write a truncation, for
        /// the recovery thread to compact up to key-point on the index.
        truncation,
    };
    // bitflags for index
    enum class footer_flags : uint32_t {
        none = 0,
        /// needed for truncation events in the same raft-term
        truncation = 1U,
    };
    struct footer {
        uint32_t size{0};
        uint32_t keys{0};
        footer_flags flags{0};
        uint32_t crc{0}; // crc32
        // version *must* be the last value
        int8_t version{0};
    };

    // for the readers and friends
    struct entry {
        entry(entry_type t, bytes k, model::offset o, int32_t d) noexcept
          : type(t)
          , key(std::move(k))
          , offset(o)
          , delta(d) {}

        entry_type type;
        bytes key;
        model::offset offset;
        int32_t delta;
    };
};
inline compacted_index::footer_flags
operator|(compacted_index::footer_flags a, compacted_index::footer_flags b) {
    return compacted_index::footer_flags(
      std::underlying_type_t<compacted_index::footer_flags>(a)
      | std::underlying_type_t<compacted_index::footer_flags>(b));
}

inline void
operator|=(compacted_index::footer_flags& a, compacted_index::footer_flags b) {
    a = (a | b);
}

inline compacted_index::footer_flags
operator~(compacted_index::footer_flags a) {
    return compacted_index::footer_flags(
      ~std::underlying_type_t<compacted_index::footer_flags>(a));
}

inline compacted_index::footer_flags
operator&(compacted_index::footer_flags a, compacted_index::footer_flags b) {
    return compacted_index::footer_flags(
      std::underlying_type_t<compacted_index::footer_flags>(a)
      & std::underlying_type_t<compacted_index::footer_flags>(b));
}

inline void
operator&=(compacted_index::footer_flags& a, compacted_index::footer_flags b) {
    a = (a & b);
}

inline std::ostream&
operator<<(std::ostream& o, const compacted_index::footer& f) {
    return o << "{size:" << f.size << ", keys:" << f.keys
             << ", flags:" << (uint32_t)f.flags << ", crc:" << f.crc
             << ", version: " << (int)f.version << "}";
}

} // namespace storage
