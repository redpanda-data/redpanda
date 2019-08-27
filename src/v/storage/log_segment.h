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

/*
 * A container for log segments. Usage:
 *
 * log_set l;
 * l.add(some_log_segment);
 * ...
 * l.add(another_log_segment);
 * ...
 * for (auto seg : l) {
 *   // Do something with the segment
 * }
 */
class log_set {
public:
    using const_iterator_type = std::vector<log_segment_ptr>::const_iterator;
    using iterator_type = std::vector<log_segment_ptr>::iterator;

    using iter_gen_type = uint64_t;

    static constexpr iter_gen_type invalid_iter_gen = 0;

    using is_nothrow
      = std::is_nothrow_move_constructible<std::vector<log_segment_ptr>>;

    explicit log_set(std::vector<log_segment_ptr>) noexcept(is_nothrow::value);

    size_t size() {
        return _segments.size();
    }

    /// New segments must be monotonically increasing in base offset
    void add(log_segment_ptr);

    void pop_last();

    log_segment_ptr last() const {
        return _segments.back();
    }

    const_iterator_type begin() const {
        return _segments.begin();
    }

    const_iterator_type end() const {
        return _segments.end();
    }

    iterator_type begin() {
        return _segments.begin();
    }

    iterator_type end() {
        return _segments.end();
    }

    // On generation changes, the iterators are invalidated.
    iter_gen_type iter_gen() const {
        return _generation;
    }

private:
    class generation_advancer {
    public:
        explicit generation_advancer(log_set&) noexcept;
        ~generation_advancer();

    private:
        log_set& _log_set;
    };

    friend class generation_advancer;

private:
    std::vector<log_segment_ptr> _segments;
    iter_gen_type _generation = 1;
};

} // namespace storage