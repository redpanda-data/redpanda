#pragma once
#include "bytes/bytes.h"
#include "model/fundamental.h"
#include "storage/compacted_topic_index.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

namespace storage {
class spill_key_index {
public:
    struct key_type_hash {
        using is_transparent = std::true_type;
        size_t operator()(const iobuf& k) const;
        size_t operator()(const bytes& k) const;
    };
    struct key_type_eq {
        using is_transparent = std::true_type;
        bool operator()(const bytes& lhs, const bytes& rhs) const;
        bool operator()(const bytes& lhs, const iobuf& rhs) const;
    };
    using underlying_t
      = absl::flat_hash_map<bytes, model::offset, key_type_hash, key_type_eq>;

    spill_key_index(
      ss::file index_file, ss::io_priority_class, size_t max_memory);

    ss::future<> index(const iobuf& key, model::offset);
    ss::future<> close();

private:
    compacted_topic_index _index;
    size_t _max_mem;
    underlying_t _midx;
    size_t _mem_usage{0};
};

inline bool spill_key_index::key_type_eq::operator()(
  const bytes& lhs, const bytes& rhs) const {
    return lhs < rhs;
}

inline bool spill_key_index::key_type_eq::operator()(
  const bytes& lhs, const iobuf& rhs) const {
    if (lhs.size() != rhs.size_bytes()) {
        return false;
    }
    auto iobuf_end = iobuf::byte_iterator(rhs.cend(), rhs.cend());
    auto iobuf_it = iobuf::byte_iterator(rhs.cbegin(), rhs.cend());
    size_t bytes_idx = 0;
    const size_t max = lhs.size();
    while (iobuf_it != iobuf_end && bytes_idx < max) {
        const char r_c = *iobuf_it;
        const char l_c = lhs[bytes_idx];
        if (l_c < r_c) {
            return true;
        }
        if (l_c > r_c) {
            return false;
        }
        // the equals case
        ++bytes_idx;
        ++iobuf_it;
    }
    return false;
}

inline size_t spill_key_index::key_type_hash::operator()(const iobuf& k) const {
    return absl::Hash<iobuf>{}(k);
}
inline size_t spill_key_index::key_type_hash::operator()(const bytes& k) const {
    return absl::Hash<bytes>{}(k);
}

} // namespace storage
