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
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/index_state.h"
#include "storage/types.h"

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
 *     default/test/0/1-1-v1.base_index
 */
class segment_index {
public:
    /// brief hydrated entry
    struct entry {
        model::offset offset;
        model::timestamp timestamp;
        size_t filepos;
        friend std::ostream& operator<<(std::ostream&, const entry&);
    };

    // 32KB - a well known number as a sweet spot for fetching data from disk
    static constexpr size_t default_data_buffer_step = 4096 * 8;

    segment_index(
      ss::sstring filename,
      model::offset base,
      size_t step,
      debug_sanitize_files);

    /** Constructor with mock file content for unit testing */
    segment_index(
      ss::sstring filename,
      ss::file mock_file,
      model::offset base,
      size_t step);

    ~segment_index() noexcept = default;
    segment_index(segment_index&&) noexcept = default;
    segment_index& operator=(segment_index&&) noexcept = default;
    segment_index(const segment_index&) = delete;
    segment_index& operator=(const segment_index&) = delete;

    void maybe_track(const model::record_batch_header&, size_t filepos);
    std::optional<entry> find_nearest(model::offset);
    std::optional<entry> find_nearest(model::timestamp);

    model::offset base_offset() const { return _state.base_offset; }
    model::offset max_offset() const { return _state.max_offset; }
    model::timestamp max_timestamp() const { return _state.max_timestamp; }
    model::timestamp base_timestamp() const { return _state.base_timestamp; }
    const ss::sstring& filename() const { return _name; }

    ss::future<bool> materialize_index();
    ss::future<> close();
    ss::future<> flush();
    ss::future<> truncate(model::offset);

    ss::future<ss::file> open();

    /// \brief erases the underlying file and resets the index
    /// this is used during compacted index recovery, as we must first
    /// invalidate all indices, before we swap the data file
    ss::future<> drop_all_data();

    /// \brief resets the state to 0, except for base_offset
    /// a destructive operation. Needed for node bootstrap
    void reset();
    void swap_index_state(index_state&&);
    bool needs_persistence() const { return _needs_persistence; }
    index_state release_index_state() && { return std::move(_state); }

private:
    ss::sstring _name;
    size_t _step;
    size_t _acc{0};
    bool _needs_persistence{false};
    index_state _state;
    debug_sanitize_files _sanitize;

    // For unit testing only.  If this is set, then open() returns
    // the contents of mock_file instead of opening the path in _name.
    std::optional<ss::file> _mock_file;

    friend std::ostream& operator<<(std::ostream&, const segment_index&);
};

using segment_index_ptr = std::unique_ptr<segment_index>;
std::ostream& operator<<(std::ostream&, const segment_index_ptr&);
std::ostream&
operator<<(std::ostream&, const std::optional<segment_index::entry>&);

} // namespace storage
