#pragma once
#include "bytes/bytes.h"
#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/segment_appender.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

namespace storage::internal {
using namespace storage; // NOLINT
class spill_key_index final : public compacted_index_writer::impl {
public:
    struct key_type_hash {
        using is_transparent = std::true_type;
        size_t operator()(const iobuf& k) const;
        size_t operator()(const bytes& k) const;
        size_t operator()(const bytes_view&) const;
    };
    struct key_type_eq {
        using is_transparent = std::true_type;
        bool operator()(const bytes& lhs, const bytes_view& rhs) const;
        bool operator()(const bytes& lhs, const bytes& rhs) const;
        bool operator()(const bytes& lhs, const iobuf& rhs) const;
    };
    struct value_type {
        model::offset base_offset;
        int32_t delta{0};
    };
    using underlying_t
      = absl::flat_hash_map<bytes, value_type, key_type_hash, key_type_eq>;

    spill_key_index(
      ss::sstring filename,
      ss::file index_file,
      ss::io_priority_class,
      size_t max_memory);
    spill_key_index(const spill_key_index&) = delete;
    spill_key_index& operator=(const spill_key_index&) = delete;
    spill_key_index(spill_key_index&&) noexcept = default;
    spill_key_index& operator=(spill_key_index&&) noexcept = delete;
    ~spill_key_index() override = default;

    // public

    ss::future<> index(const iobuf& key, model::offset, int32_t) final;
    ss::future<> index(bytes_view, model::offset, int32_t) final;
    ss::future<> truncate(model::offset) final;
    ss::future<> close() final;
    void print(std::ostream&) const final;

private:
    ss::future<> drain_all_keys();
    ss::future<> add_key(bytes b, value_type);
    ss::future<> spill(compacted_index::entry_type, bytes_view, value_type);

    segment_appender _appender;
    underlying_t _midx;
    size_t _max_mem;
    size_t _mem_usage{0};
    compacted_index::footer _footer;
    crc32 _crc;

    friend std::ostream& operator<<(std::ostream&, const spill_key_index&);
};

inline bool spill_key_index::key_type_eq::operator()(
  const bytes& lhs, const bytes& rhs) const {
    return lhs == rhs;
}
inline bool spill_key_index::key_type_eq::operator()(
  const bytes& lhs, const bytes_view& rhs) const {
    return bytes_view(lhs) == rhs;
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
        if (l_c != r_c) {
            return false;
        }
        // the equals case
        ++bytes_idx;
        ++iobuf_it;
    }
    return false;
}
inline size_t
spill_key_index::key_type_hash::operator()(const bytes_view& k) const {
    return absl::Hash<bytes_view>{}(k);
}
inline size_t spill_key_index::key_type_hash::operator()(const iobuf& k) const {
    return absl::Hash<iobuf>{}(k);
}
inline size_t spill_key_index::key_type_hash::operator()(const bytes& k) const {
    return absl::Hash<bytes>{}(k);
}

} // namespace storage::internal
