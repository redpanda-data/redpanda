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
#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/log.hh>

#include <optional>
#include <type_traits>
#include <vector>

namespace storage {

class segment_reader {
public:
    segment_reader(
      ss::sstring filename,
      ss::file,
      size_t file_size,
      size_t buffer_size,
      unsigned read_ahead) noexcept;
    ~segment_reader() noexcept = default;
    segment_reader(segment_reader&&) noexcept = default;
    segment_reader& operator=(segment_reader&&) noexcept = default;
    segment_reader(const segment_reader&) = delete;
    segment_reader& operator=(const segment_reader&) = delete;

    /// max physical byte that this reader is allowed to fetch
    void set_file_size(size_t o) { _file_size = o; }
    size_t file_size() const { return _file_size; }

    /// file name
    const ss::sstring& filename() const { return _filename; }

    bool empty() const { return _file_size == 0; }

    /// close the underlying file handle
    ss::future<> close() { return _data_file.close(); }

    /// perform syscall stat
    ss::future<struct stat> stat() { return _data_file.stat(); }

    /// truncates file starting at this phyiscal offset
    ss::future<> truncate(size_t sz);

    /// create an input stream _sharing_ the underlying file handle
    /// starting at position @pos
    ss::input_stream<char>
    data_stream(size_t pos, const ss::io_priority_class&);
    ss::input_stream<char>
    data_stream(size_t pos_begin, size_t pos_end, const ss::io_priority_class&);

private:
    ss::sstring _filename;
    ss::file _data_file;
    size_t _file_size{0};
    size_t _buffer_size{0};
    unsigned _read_ahead{0};

    friend std::ostream& operator<<(std::ostream&, const segment_reader&);
};

using segment_reader_ptr = ss::lw_shared_ptr<segment_reader>;

std::ostream& operator<<(std::ostream&, segment_reader_ptr);
} // namespace storage
