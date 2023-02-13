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
#include "storage/fs_utils.h"
#include "storage/types.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/mutex.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/log.hh>

#include <optional>
#include <type_traits>
#include <vector>

namespace storage {

class segment_reader;

struct stream_provider {
    virtual ss::input_stream<char> take_stream() = 0;
    virtual ss::future<> close() = 0;
    virtual ~stream_provider() = default;
};

/**
 * A segment reader handle confers the right to use a file descriptor
 * opened by segment_reader, a clone of which is encapsulated in
 * the handler's input_stream (if stream is set).
 *
 * We need this handle & associated reference counting to enable
 * opening segment files on demand, and keeping them open as long
 * as input_streams using the file are alive.
 */
class segment_reader_handle final : public stream_provider {
private:
    intrusive_list_hook _hook;
    segment_reader* _parent{nullptr};

    friend class segment_reader;

    // A handle does not have to have a stream: it might have been
    // created to just stat() a file for example.
    std::optional<ss::input_stream<char>> _stream;

public:
    explicit segment_reader_handle(segment_reader* parent);

    segment_reader_handle(segment_reader_handle&& rhs) noexcept {
        _stream = std::exchange(rhs._stream, std::nullopt);
        _parent = std::exchange(rhs._parent, nullptr);
        _hook.swap_nodes(rhs._hook);
    }

    /**
     * Special constructor for use in remote_segment, which doesn't
     * implement open-on-demand behaviour.
     */
    explicit segment_reader_handle(ss::input_stream<char>&& s)
      : _stream(std::move(s)) {}

    void operator=(segment_reader_handle&& rhs) noexcept;

    /**
     * May only be called once per lifetime.  Use this immediately after
     * construction.
     */
    void set_stream(ss::input_stream<char> s) {
        vassert(!_stream.has_value(), "Called set_stream twice!");
        _stream = std::move(s);
    }

    /**
     * Move the input_stream out of this class: use this method
     * if you are about to consume the stream through close and destruction.
     *
     * You must still call close on this handle before destroying it, even if
     * you have closed the stream.
     */
    ss::input_stream<char> take_stream() override {
        auto r = std::move(_stream.value());
        _stream.reset();
        return r;
    }

    ss::input_stream<char>& stream() { return _stream.value(); }

    ss::future<> close() override;

    ~segment_reader_handle() override;
};

class segment_reader {
public:
    segment_reader(
      segment_full_path filename,
      size_t buffer_size,
      unsigned read_ahead,
      debug_sanitize_files) noexcept;
    ~segment_reader() noexcept;
    segment_reader(segment_reader&&) noexcept;
    segment_reader& operator=(segment_reader&&) noexcept;
    segment_reader(const segment_reader&) = delete;
    segment_reader& operator=(const segment_reader&) = delete;

    ss::future<> load_size();

    /// max physical byte that this reader is allowed to fetch
    void set_file_size(size_t o) { _file_size = o; }
    size_t file_size() const { return _file_size; }

    /// file name
    const ss::sstring filename() const { return path(); }
    const segment_full_path& path() const { return _path; }

    bool empty() const { return _file_size == 0; }

    /// close the underlying file handle
    ss::future<> close();

    /// perform syscall stat
    ss::future<struct stat> stat();

    /// truncates file starting at this phyiscal offset
    ss::future<> truncate(size_t sz);

    /// create an input stream _sharing_ the underlying file handle
    /// starting at position @pos
    ss::future<segment_reader_handle>
    data_stream(size_t pos, const ss::io_priority_class);
    ss::future<segment_reader_handle>
    data_stream(size_t pos_begin, size_t pos_end, const ss::io_priority_class);

private:
    segment_full_path _path;

    // Protects open/close of _data_file, to avoid double-opening on
    // concurrent calls to get()
    mutex _open_lock;

    // This is only initialized if _data_file_refcount is greater than zero
    ss::file _data_file;

    uint32_t _data_file_refcount{0};

    intrusive_list<segment_reader_handle, &segment_reader_handle::_hook>
      _streams;

    size_t _file_size{0};
    size_t _buffer_size{0};
    unsigned _read_ahead{0};
    debug_sanitize_files _sanitize;

    // Keeps track of operations that cannot be pre-empted by close()
    ss::gate _gate;
    // Acquire a handle to use the underlying file handle
    ss::future<segment_reader_handle> get();

    // Signal destruction of a segment_reader_handle
    ss::future<> put();

    friend class segment_reader_handle;
    friend std::ostream& operator<<(std::ostream&, const segment_reader&);
};

using segment_reader_ptr = ss::lw_shared_ptr<segment_reader>;

std::ostream& operator<<(std::ostream&, segment_reader_ptr);

/**
 * Enables reading from a series of segments sequentially using a single data
 * source. The first segment in the list is read starting from the start file
 * position. The last segment in the list is read upto the end file position.
 */
class concat_segment_data_source_impl final : public ss::data_source_impl {
public:
    concat_segment_data_source_impl(
      std::vector<ss::lw_shared_ptr<segment>> segments,
      size_t start_pos,
      size_t end_pos,
      ss::io_priority_class priority_class);

    /// Reads a buffer from the current underlying segment handle. Once the
    /// segment is depleted, moves over to the next segment, until all segments
    /// are finished.
    ss::future<ss::temporary_buffer<char>> get() override;

    /// Closing the data source closes the currently open handle, and by
    /// extension the input stream. Only one handle is open at a time.
    ss::future<> close() override;

private:
    /**
     * Switches over to the next segment, if the current segment is depleted.
     */
    ss::future<> next_stream();

private:
    using segment_seq = std::vector<ss::lw_shared_ptr<segment>>;
    using segment_iter = segment_seq::iterator;
    segment_seq _segments;
    std::optional<ss::input_stream<char>> _current_stream;
    std::optional<segment_reader_handle> _current_handle;
    segment_iter _current_pos;

    // The file position from where the first segment will be read.
    size_t _start_pos;

    // The file position upto which the last segment will be read.
    size_t _end_pos;
    ss::io_priority_class _priority_class;

    ss::gate _gate;
    ss::sstring _name;
};

/**
 * Provides a stream_provider interface for reading from underlying segments in
 * sequence.
 */
class concat_segment_reader_view final : public stream_provider {
public:
    concat_segment_reader_view(
      std::vector<ss::lw_shared_ptr<segment>> segments,
      size_t start_pos,
      size_t end_pos,
      ss::io_priority_class priority_class);

    /// Moves the composite input stream out of the view.
    ss::input_stream<char> take_stream() override;

    /// Closing the view is a no-op if the stream has been moved out earlier
    /// with `take_stream`
    ss::future<> close() override;

private:
    std::optional<ss::input_stream<char>> _stream;
};

} // namespace storage
