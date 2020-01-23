#pragma once
#include "model/fundamental.h"

#include <seastar/core/file.hh>
#include <seastar/core/unaligned.hh>

#include <memory>
#include <optional>
#include <vector>

namespace storage {

/**
 * file file format is: [ header ] [ payload ]
 * header  == segment_offset_index::header
 * payload == std::vector<pair<uint32_t,uint32_t>>;
 *
 * Assume an ntp("default", "test", 0);
 *     default/test/0/1-1-v1.log
 *
 * The name of this index _must_ be then:
 *     default/test/0/1-1-v1.log.offset_index
 */
class segment_offset_index {
public:
    struct [[gnu::packed]] header {
        /// xxhash of the payload only. does not include header in payload
        ss::unaligned<uint32_t> checksum{0};
        /// payload size
        ss::unaligned<uint32_t> size{0};
        /// reserved for future extensions, such as version and compression
        ss::unaligned<uint32_t> unused_flags{0};
    };
    // 32KB - a well known number as a sweet spot for fetching data from disk
    static constexpr size_t default_data_buffer_step = 4096 * 8;

    segment_offset_index(
      ss::sstring filename, ss::file, model::offset base, size_t step);
    segment_offset_index(segment_offset_index&&) noexcept = default;
    segment_offset_index& operator=(segment_offset_index&&) noexcept = default;
    segment_offset_index(const segment_offset_index&) = delete;
    segment_offset_index& operator=(const segment_offset_index&) = delete;

    void maybe_track(model::offset o, size_t pos, size_t data_size);
    std::optional<size_t> lower_bound(model::offset o);

    model::offset base_offset() const { return _base; }
    model::offset last_indexed_offset() const {
        if (_positions.empty()) {
            return _base;
        }
        return _base + model::offset(_positions.back().first);
    }
    model::offset last_seen_offset() const {
        return _last_seen_offset;
    }
    bool needs_persistence() const { return _needs_persistence; }
    size_t indexed_offsets() const { return _positions.size(); }
    size_t step() const { return _step; }
    const ss::sstring& filename() const { return _name; }

    ss::future<bool> materialize_index();
    ss::future<> close();
    ss::future<> flush();

private:
    ss::sstring _name;
    ss::file _out;
    model::offset _base;
    model::offset _last_seen_offset;
    size_t _step;
    size_t _acc{0};
    bool _needs_persistence{false};
    std::vector<std::pair<uint32_t, uint32_t>> _positions;
};

using segment_offset_index_ptr = std::unique_ptr<segment_offset_index>;
std::ostream& operator<<(std::ostream&, const segment_offset_index&);
std::ostream& operator<<(std::ostream&, const segment_offset_index_ptr&);
} // namespace storage
