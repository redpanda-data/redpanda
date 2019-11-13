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
      model::term_id term,
      model::offset base_offset,
      size_t buffer_size) noexcept;
    log_segment(log_segment&&) noexcept = default;
    log_segment(const log_segment&) = delete;
    log_segment& operator=(const log_segment&) = delete;
    sstring get_filename() const {
        return _filename;
    }

    model::term_id term() const {
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
    model::term_id _term;
    size_t _buffer_size;
    model::offset _max_offset;

    lw_shared_ptr<file_input_stream_history> _history
      = make_lw_shared<file_input_stream_history>();
};

using log_segment_ptr = lw_shared_ptr<log_segment>;

std::ostream& operator<<(std::ostream&, const log_segment&);
std::ostream& operator<<(std::ostream&, log_segment_ptr);

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
    using underlying_t = std::vector<log_segment_ptr>;
    using const_iterator = underlying_t::const_iterator;
    using const_reverse_iterator = underlying_t::const_reverse_iterator;
    using iterator = underlying_t::iterator;

    using iter_gen_type = uint64_t;

    static constexpr iter_gen_type invalid_iter_gen = 0;

    using is_nothrow
      = std::is_nothrow_move_constructible<std::vector<log_segment_ptr>>;

    explicit log_set(std::vector<log_segment_ptr>) noexcept(is_nothrow::value);

    size_t size() const {
        return _segments.size();
    }

    bool empty() const {
        return _segments.empty();
    }

    /// New segments must be monotonically increasing in base offset
    void add(log_segment_ptr);

    void pop_last();

    log_segment_ptr last() const {
        return _segments.back();
    }

    const_iterator begin() const {
        return _segments.begin();
    }

    const_iterator end() const {
        return _segments.end();
    }
    const_reverse_iterator rbegin() const {
        return _segments.rbegin();
    }
    const_reverse_iterator rend() const {
        return _segments.rend();
    }

    iterator begin() {
        return _segments.begin();
    }

    iterator end() {
        return _segments.end();
    }

    /// very rare. needed when raft needs to truncate un-acknowledged data
    /// it is also very likely that they are the back sements, and so
    /// the overhead is likely to be small. This is why our std::find
    /// starts from the back
    std::vector<log_segment_ptr>
    remove(std::vector<sstring> segment_filenames) {
        std::vector<log_segment_ptr> retval;
        for (auto& s : segment_filenames) {
            auto b = _segments.rbegin();
            auto e = _segments.rend();
            auto it = std::find_if(b, e, [&s](log_segment_ptr& p) {
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
              [](const log_segment_ptr& a, const log_segment_ptr& b) {
                  return a->base_offset() < b->base_offset();
              });
        }
        return retval;
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

class log_segment_selector {
public:
    explicit log_segment_selector(const log_set&) noexcept;

    // Successive calls to `select()' have to pass weakly
    // monotonic offsets.
    log_segment_ptr select(model::offset) const;

private:
    const log_set& _set;
    // Always valid within an epoch.
    mutable log_set::const_iterator _current_segment;
    // Detects compactions and iterator invalidations.
    mutable log_set::iter_gen_type _iter_gen = log_set::invalid_iter_gen;
};

} // namespace storage
