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

    static constexpr const size_t fallocation_alignment = 4_KiB;
    static constexpr const size_t write_behind_memory = 1_MiB;

    struct options {
        options(
          ss::io_priority_class p,
          size_t chunks_no,
          config::binding<size_t> falloc_step)
          : priority(p)
          , number_of_chunks(chunks_no)
          , falloc_step(falloc_step) {}

        ss::io_priority_class priority;
        size_t number_of_chunks;
        config::binding<size_t> falloc_step;
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

    /** Validator for fallocation step configuration setting */
    static std::optional<ss::sstring>
    validate_fallocation_step(const size_t& value) {
        if (value % segment_appender::fallocation_alignment != 0) {
            return "Fallocation step must be multiple of 4096";
        } else if (value < segment_appender::fallocation_alignment) {
            return "Fallocation step must be at least 4 KiB (4096)";
        } else if (value > 1_GiB) {
            return "Fallocation step can't be larger than 1 GiB (1073741824)";
        } else {
            return std::nullopt;
        }
    }

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
    ss::lw_shared_ptr<ss::semaphore> _prev_head_write;

    struct flush_op {
        explicit flush_op(size_t offset)
          : offset(offset) {}
        size_t offset;
        ss::promise<> p;
    };

    std::vector<flush_op> _flush_ops;
    size_t _flushed_offset{0};
    size_t _stable_offset{0};

    // like flush, but wait on fibers. used by truncate() and close() which are
    // still heavy weight operations compared to regular flush()
    ss::future<> hard_flush();

    struct inflight_write {
        bool done;
        size_t offset;

        explicit inflight_write(size_t offset)
          : done(false)
          , offset(offset) {}
    };

    ss::chunked_fifo<ss::lw_shared_ptr<inflight_write>> _inflight;
    callbacks* _callbacks = nullptr;
    ss::future<>
    maybe_advance_stable_offset(const ss::lw_shared_ptr<inflight_write>&);
    ss::future<> process_flush_ops(size_t);

    ss::timer<ss::lowres_clock> _inactive_timer;
    void handle_inactive_timer();

    size_t _chunk_size{0};

    friend std::ostream& operator<<(std::ostream&, const segment_appender&);
};

using segment_appender_ptr = std::unique_ptr<segment_appender>;

} // namespace storage
