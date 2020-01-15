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
    ss::sstring get_filename() const { return _filename; }

    /// current term
    model::term_id term() const { return _term; }

    // Inclusive lower bound offset.
    model::offset base_offset() const { return _base_offset; }

    // Inclusive upper bound offset.
    model::offset max_offset() const { return _max_offset; }

    uint64_t file_size() const { return _file_size; }

    /// close the underlying file handle
    ss::future<> close() { return _data_file.close(); }

    /// perform syscall stat
    ss::future<struct stat> stat() { return _data_file.stat(); }

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

/*
 * A container for log_segment_reader's. Usage:
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
    using underlying_t = std::vector<segment_reader_ptr>;
    using const_iterator = underlying_t::const_iterator;
    using const_reverse_iterator = underlying_t::const_reverse_iterator;
    using iterator = underlying_t::iterator;

    using iter_gen_type = uint64_t;

    static constexpr iter_gen_type invalid_iter_gen = 0;

    using is_nothrow
      = std::is_nothrow_move_constructible<std::vector<segment_reader_ptr>>;

    explicit log_set(std::vector<segment_reader_ptr>) noexcept(
      is_nothrow::value);

    size_t size() const { return _segments.size(); }

    bool empty() const { return _segments.empty(); }

    /// New segments must be monotonically increasing in base offset
    void add(segment_reader_ptr);

    void pop_last();

    segment_reader_ptr last() const { return _segments.back(); }

    const_iterator begin() const { return _segments.begin(); }

    const_iterator end() const { return _segments.end(); }
    const_reverse_iterator rbegin() const { return _segments.rbegin(); }
    const_reverse_iterator rend() const { return _segments.rend(); }

    iterator begin() { return _segments.begin(); }

    iterator end() { return _segments.end(); }

    const_iterator lower_bound(model::offset) const;
    /// very rare. needed when raft needs to truncate un-acknowledged data
    /// it is also very likely that they are the back sements, and so
    /// the overhead is likely to be small. This is why our std::find
    /// starts from the back
    std::vector<segment_reader_ptr>
    remove(std::vector<ss::sstring> segment_filenames) {
        std::vector<segment_reader_ptr> retval;
        for (auto& s : segment_filenames) {
            auto b = _segments.rbegin();
            auto e = _segments.rend();
            auto it = std::find_if(b, e, [&s](segment_reader_ptr& p) {
                return p->get_filename() == s;
            });
            if (it != e) {
                retval.push_back(*it);
                std::iter_swap(it, _segments.end() - 1);
                _segments.pop_back();
            }
        }
        if (!retval.empty()) {
            std::stable_sort(
              _segments.begin(),
              _segments.end(),
              [](const segment_reader_ptr& a, const segment_reader_ptr& b) {
                  return a->base_offset() < b->base_offset();
              });
        }
        return retval;
    }
    // On generation changes, the iterators are invalidated.
    iter_gen_type iter_gen() const { return _generation; }

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
    std::vector<segment_reader_ptr> _segments;
    iter_gen_type _generation = 1;
};

class log_segment_selector {
public:
    explicit log_segment_selector(const log_set&) noexcept;

    // Successive calls to `select()' have to pass weakly
    // monotonic offsets.
    segment_reader_ptr select(model::offset) const;

private:
    const log_set& _set;
    // Always valid within an epoch.
    mutable log_set::const_iterator _current_segment;
    // Detects compactions and iterator invalidations.
    mutable log_set::iter_gen_type _iter_gen = log_set::invalid_iter_gen;
};

} // namespace storage
