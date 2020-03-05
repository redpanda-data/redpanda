#pragma once
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/index_state.h"

#include <seastar/core/file.hh>
#include <seastar/core/unaligned.hh>

#include <memory>
#include <optional>
#include <vector>

namespace storage {

/**
 * file file format is: [ header ] [ payload ]
 * header  == segment_index::header
 * payload == std::vector<pair<uint32_t,uint32_t>>;
 *
 * Assume an ntp("default", "test", 0);
 *     default/test/0/1-1-v1.log
 *
 * The name of this index _must_ be then:
 *     default/test/0/1-1-v1.log.offset_index
 */
class segment_index {
public:
    /// brief hydrated entry
    struct entry {
        model::offset offset;
        model::timestamp timestamp;
        size_t filepos;
    };

    // 32KB - a well known number as a sweet spot for fetching data from disk
    static constexpr size_t default_data_buffer_step = 4096 * 8;

    segment_index(
      ss::sstring filename, ss::file, model::offset base, size_t step);
    ~segment_index() noexcept = default;
    segment_index(segment_index&&) noexcept = default;
    segment_index& operator=(segment_index&&) noexcept = default;
    segment_index(const segment_index&) = delete;
    segment_index& operator=(const segment_index&) = delete;

    void maybe_track(const model::record_batch_header&, size_t filepos);
    std::optional<entry> find_nearest(model::offset);
    std::optional<entry> find_nearest(model::timestamp);

    model::offset base_offset() const { return _state.base_offset; }
    model::timestamp max_timestamp() const { return _state.max_timestamp; }
    const ss::sstring& filename() const { return _name; }

    ss::future<bool> materialize_index();
    ss::future<> close();
    ss::future<> flush();
    ss::future<> truncate(model::offset);

private:
    ss::sstring _name;
    ss::file _out;
    size_t _step;
    size_t _acc{0};
    bool _needs_persistence{false};
    index_state _state;

    friend std::ostream& operator<<(std::ostream&, const segment_index&);
};

using segment_index_ptr = std::unique_ptr<segment_index>;
std::ostream& operator<<(std::ostream&, const segment_index_ptr&);
} // namespace storage
