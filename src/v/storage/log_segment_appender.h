#pragma once

#include "utils/fragmented_temporary_buffer.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

namespace storage {

// FIXME: Adaptive fallocate here.

/// Appends data to a log segment. It can be subclassed so
/// other classes can add behavior and still be treated as
/// an appender.
/// Note: The functions in this call cannot be called concurrently.
class log_segment_appender {
public:
    log_segment_appender(file f, file_output_stream_options options)
      : _prio(options.io_priority_class)
      , _out(make_file_output_stream(std::move(f), std::move(options))) {
    }

    future<> append(const char* buf, size_t n) {
        _offset += n;
        return _out.write(buf, n);
    }

    future<> append(bytes_view s) {
        return append(reinterpret_cast<const char*>(s.begin()), s.size());
    }

    future<> append(const fragmented_temporary_buffer& fragmented_buffer) {
        auto istream = fragmented_buffer.get_istream();
        auto f = make_ready_future<>();
        istream.consume([this, &f](bytes_view bv) {
            f = f.then(
              [this, bv = std::move(bv)] { return append(std::move(bv)); });
        });
        return f;
    }

    future<> flush() {
        return _out.flush();
    }

    future<> close() {
        return _out.close();
    }

    uint64_t offset() const {
        return _offset;
    }

    const io_priority_class& priority_class() const {
        return _prio;
    }

private:
    output_stream<char> _out;
    size_t _offset = 0;
    io_priority_class _prio;
};

} // namespace storage