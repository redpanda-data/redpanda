#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "likely.h"
#include "seastarx.h"
#include "storage/segment_appender_chunk.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>

#include <iostream>

namespace storage {

/// Appends data to a log segment. It can be subclassed so
/// other classes can add behavior and still be treated as
/// an appender.
/// Note: The functions in this call cannot be called concurrently.
class segment_appender {
public:
    using chunk = segment_appender_chunk;
    using underlying_t = intrusive_list<chunk, &chunk::hook>;
    using iterator = typename underlying_t::iterator;

    static constexpr const size_t chunks_no_buffer = 8;
    static constexpr const size_t chunk_size = chunk::chunk_size;

    struct options {
        options(ss::io_priority_class p, size_t chunks_no)
          : priority(p)
          , number_of_chunks(chunks_no) {}
        ss::io_priority_class priority;
        size_t number_of_chunks{chunks_no_buffer};
    };

    segment_appender(ss::file f, options opts);
    ~segment_appender() noexcept;
    segment_appender(segment_appender&&) noexcept;
    // semaphores cannot be assigned
    segment_appender& operator=(segment_appender&& o) noexcept = delete;
    segment_appender(const segment_appender&) = delete;
    segment_appender& operator=(const segment_appender&) = delete;

    uint64_t file_byte_offset() const {
        return _committed_offset + _bytes_flush_pending;
    }

    ss::future<> append(const char* buf, const size_t n);
    ss::future<> append(bytes_view s);
    ss::future<> append(const iobuf& io);
    ss::future<> truncate(size_t n);
    ss::future<> close();
    ss::future<> flush();

private:
    void clear();
    chunk& head() { return _free_chunks.front(); }
    void dispatch_background_head_write();

    ss::file _out;
    options _opts;
    bool _closed{false};
    size_t _committed_offset{0};
    size_t _bytes_flush_pending{0};
    ss::semaphore _concurrent_flushes{1};
    underlying_t _free_chunks;
    underlying_t _full_chunks;

    friend std::ostream& operator<<(std::ostream&, const segment_appender&);
};

using segment_appender_ptr = std::unique_ptr<segment_appender>;

} // namespace storage
