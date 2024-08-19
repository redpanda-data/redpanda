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

#include "storage/compacted_index_reader.h"

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

#include <optional>
namespace storage::internal {
using namespace storage; // NOLINT

/// \brief this class loads up slices in chunks tracking memory usage
///
class compacted_index_chunk_reader final : public compacted_index_reader::impl {
public:
    compacted_index_chunk_reader(
      segment_full_path,
      ss::file,
      ss::io_priority_class,
      size_t max_chunk_memory,
      ss::abort_source* _as) noexcept;

    ss::future<> close() final;

    ss::future<compacted_index::footer> load_footer() final;

    ss::future<> verify_integrity() final;

    void reset() final;

    void print(std::ostream&) const final;

    bool is_end_of_stream() const final;

    ss::future<ss::circular_buffer<compacted_index::entry>>
      load_slice(model::timeout_clock::time_point) final;

private:
    bool is_footer_loaded() const;

    ss::file _handle;
    ss::io_priority_class _iopc;
    size_t _max_chunk_memory{0};
    size_t _byte_index{0};
    // file size minus footer size
    std::optional<size_t> _data_size;
    std::optional<compacted_index::footer> _footer;
    bool _end_of_stream{false};
    std::optional<ss::input_stream<char>> _cursor;
    ss::abort_source* _as;

    friend std::ostream&
    operator<<(std::ostream&, const compacted_index_chunk_reader&);
};

} // namespace storage::internal
