/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

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

#include <iosfwd>

namespace storage {

/// Appends data to a log segment. It can be subclassed so
/// other classes can add behavior and still be treated as
/// an appender.
/// Note: The functions in this call cannot be called concurrently.
class segment_appender {
public:
    using chunk = segment_appender_chunk;

    static constexpr const size_t write_behind_memory = 1_MiB;
    static constexpr const size_t chunks_no_buffer = write_behind_memory
                                                     / chunk::chunk_size;
    static constexpr const size_t chunk_size = chunk::chunk_size;
    static constexpr const size_t fallocation_step = 32_MiB;

    struct options {
        options(ss::io_priority_class p, size_t chunks_no)
          : options(p, chunks_no, fallocation_step) {}

        options(ss::io_priority_class p, size_t chunks_no, size_t step)
          : priority(p)
          , number_of_chunks(chunks_no)
          , falloc_step(step) {}

        ss::io_priority_class priority;
        size_t number_of_chunks{chunks_no_buffer};
        size_t falloc_step{fallocation_step};
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

    struct callbacks {
        virtual void committed_physical_offset(size_t) = 0;
    };

    void set_callbacks(callbacks* callbacks) { _callbacks = callbacks; }

private:
    void dispatch_background_head_write();
    ss::future<> do_next_adaptive_fallocation();
    ss::future<> hydrate_last_half_page();
    ss::future<> do_truncation(size_t);
    ss::future<> do_append(const char* buf, const size_t n);

    /*
     * committed offset isn't updated until the background write is dispatched.
     * however, we must ensure that an fallocation never occurs at an offset
     * below the committed offset. because truncation can occur at an unaligned
     * offset, its possible that a chunk offset range overlaps fallocation
     * offset. if that happens and the chunk fills up and is dispatched before
     * the next fallocation then fallocation will write zeros to a lower offset
     * than the commit index. thus, here we must compare fallocation offset to
     * the eventual committed offset taking into account pending bytes.
     */
    size_t next_committed_offset() const {
        return _committed_offset + (_head ? _head->bytes_pending() : 0);
    }

    ss::file _out;
    options _opts;
    bool _closed{false};
    size_t _committed_offset{0};
    size_t _fallocation_offset{0};
    size_t _bytes_flush_pending{0};
    ss::semaphore _concurrent_flushes;
    ss::lw_shared_ptr<chunk> _head;

    struct inflight_write {
        bool done;
        size_t offset;

        explicit inflight_write(size_t offset)
          : done(false)
          , offset(offset) {}
    };

    ss::chunked_fifo<ss::lw_shared_ptr<inflight_write>> _inflight;
    callbacks* _callbacks = nullptr;
    void maybe_advance_stable_offset(const ss::lw_shared_ptr<inflight_write>&);

    ss::timer<ss::lowres_clock> _inactive_timer;
    void handle_inactive_timer();
    bool _previously_inactive = false;

    friend std::ostream& operator<<(std::ostream&, const segment_appender&);
};

using segment_appender_ptr = std::unique_ptr<segment_appender>;

} // namespace storage
