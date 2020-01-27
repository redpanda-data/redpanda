#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/log_segment_appender.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/log.hh>

#include <optional>
#include <type_traits>
#include <vector>

namespace storage {

class log_segment_reader {
public:
    log_segment_reader(
      ss::sstring filename,
      ss::file,
      model::term_id term,
      model::offset base_offset,
      uint64_t file_size,
      size_t buffer_size) noexcept;
    log_segment_reader(log_segment_reader&&) noexcept = default;
    log_segment_reader(const log_segment_reader&) = delete;
    log_segment_reader& operator=(const log_segment_reader&) = delete;

    /// mutating method for keeping track of the last offset from
    /// the active log_segment_appender
    void set_last_written_offset(model::offset o) { _max_offset = o; }

    /// max physical byte that this reader is allowed to fetch
    void set_last_visible_byte_offset(uint64_t o) { _file_size = o; }

    /// file name
    const ss::sstring& filename() const { return _filename; }

    /// current term
    model::term_id term() const { return _term; }

    // Inclusive lower bound offset.
    model::offset base_offset() const { return _base_offset; }

    // Inclusive upper bound offset.
    model::offset max_offset() const { return _max_offset; }

    uint64_t file_size() const { return _file_size; }

    bool empty() const {
        // Note cannot be _max_offset() < 0 because
        // on truncation we set it to one past the base
        // which needs to invalidate the file
        return _max_offset() < _base_offset;
    }

    /// close the underlying file handle
    ss::future<> close() { return _data_file.close(); }

    /// perform syscall stat
    ss::future<struct stat> stat() { return _data_file.stat(); }

    /// truncates file starting at this phyiscal offset
    ss::future<> truncate(size_t sz);

    /// flushes the file metadata
    ss::future<> flush() { return _data_file.flush(); }

    /// create an input stream _sharing_ the underlying file handle
    /// starting at position @pos
    ss::input_stream<char>
    data_stream(uint64_t pos, const ss::io_priority_class&);

private:
    ss::sstring _filename;
    ss::file _data_file;
    model::offset _base_offset;
    model::term_id _term;
    uint64_t _file_size;
    size_t _buffer_size;
    model::offset _max_offset;
    ss::lw_shared_ptr<ss::file_input_stream_history> _history
      = ss::make_lw_shared<ss::file_input_stream_history>();
};

using segment_reader_ptr = ss::lw_shared_ptr<log_segment_reader>;

std::ostream& operator<<(std::ostream&, const log_segment_reader&);
std::ostream& operator<<(std::ostream&, segment_reader_ptr);
} // namespace storage
