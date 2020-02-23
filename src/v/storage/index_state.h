#pragma once

#include "model/fundamental.h"
#include "model/timestamp.h"
#include "reflection/adl.h"

#include <cstdint>

namespace storage {
/* Fileformat:
   4 bytes - size
   8 bytes - checksum - xxhash32 -- we checksum everything below the checksum
   4 bytes - bitflags - unused
   8 bytes - based_offset
   8 bytes - base_time
   4 bytes - index.size()
   []index_state::entry - array of 3 32-bit integers per item
 */
struct index_state {
    struct entry {
        entry(uint32_t relof, uint32_t reltim, uint32_t pos) noexcept
          : relative_offset(relof)
          , relative_time(reltim)
          , filepos(pos) {}
        /// \brief relative to the base offset of the segment
        uint32_t relative_offset;
        /// \brief relative to base_time
        uint32_t relative_time;
        /// \brief absolute physical offset from 0 to the start of the batch
        /// i.e.: the batch starts exactly at this position
        uint32_t filepos;
    };
    /// \brief sizeof the index in bytes
    uint32_t size{0};
    /// \brief currently xxhash64
    uint64_t checksum{0};
    /// \brief unused
    uint32_t bitflags{0};
    model::offset base_offset{0};
    model::timestamp base_timestamp{0};
    model::timestamp max_timestamp{0};
    std::vector<entry> index;

    static uint64_t checksum_state(const index_state&);
    friend std::ostream& operator<<(std::ostream&, const index_state&);
};

struct index_state_offset_comparator {
    // lower bound bool
    bool operator()(const index_state::entry& p, uint32_t needle) const {
        return p.relative_offset < needle;
    }
    // upper bound bool
    bool operator()(uint32_t needle, const index_state::entry& p) const {
        return needle < p.relative_offset;
    }
};
struct index_state_timestamp_comparator {
    // lower bound bool
    bool operator()(const index_state::entry& p, uint32_t needle) const {
        return p.relative_time < needle;
    }
    // upper bound bool
    bool operator()(uint32_t needle, const index_state::entry& p) const {
        return needle < p.relative_time;
    }
};

} // namespace storage
namespace reflection {
template<>
struct adl<storage::index_state> {
    void to(iobuf&, storage::index_state&&);
    storage::index_state from(iobuf_parser&);
};
template<>
struct adl<storage::index_state::entry> {
    void to(iobuf&, storage::index_state::entry&&);
    storage::index_state::entry from(iobuf_parser&);
};
} // namespace reflection
