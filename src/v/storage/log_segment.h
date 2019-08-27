#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/log_segment_appender.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/log.hh>

namespace storage {

class log_segment {
public:
    log_segment(
      sstring filename,
      file,
      int64_t term,
      model::offset base_offset,
      size_t buffer_size) noexcept;

    sstring get_filename() const {
        return _filename;
    }

    int64_t term() const {
        return _term;
    }

    // Inclusive lower bound offset.
    model::offset base_offset() const {
        return _base_offset;
    }

    // Exclusive upper bound offset.
    model::offset max_offset() const {
        return _max_offset;
    }

    void set_last_written_offset(model::offset max_offset) {
        _max_offset = std::max(_max_offset, max_offset);
    }

    future<> close() {
        return _data_file.close();
    }

    future<> flush() {
        return _data_file.flush();
    }

    future<struct stat> stat() {
        return _data_file.stat();
    }

    future<> truncate(size_t size) {
        return _data_file.truncate(size);
    }

    input_stream<char> data_stream(uint64_t pos, const io_priority_class&);

    log_segment_appender data_appender(const io_priority_class&);

private:
    sstring _filename;
    file _data_file;
    model::offset _base_offset;
    int64_t _term;
    size_t _buffer_size;
    model::offset _max_offset;

    lw_shared_ptr<file_input_stream_history> _history
      = make_lw_shared<file_input_stream_history>();
};

using log_segment_ptr = lw_shared_ptr<log_segment>;

} // namespace storage