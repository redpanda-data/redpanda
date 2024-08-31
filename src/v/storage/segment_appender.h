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

#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "container/fragmented_vector.h"
#include "model/record.h"
#include "ssx/semaphore.h"
#include "storage/config.h"
#include "storage/segment_appender_chunk.h"
#include "storage/storage_resources.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>

#include <iosfwd>

class iobuf;

namespace storage {

/// Appends data to a log segment. It can be subclassed so
/// other classes can add behavior and still be treated as
/// an appender.
///
/// The append() functions in this class take different input types to
/// append but all return future<> and have the same general semantics:
/// After the future<> for an append() call returns, the data has been
/// logically appended to the segment in memory, but may not be, or not
/// fully be flushed to disk and in general hasn't been fsynced. After
/// the future from append() results, a subject flush() returns a future
/// whose resolution indicates that all prior appends have been flushed
/// and fsync'd on disk.
///
/// NOTE: Only one append() may be progress at one time. I.e., it is not
/// safe to call append() before the prior append() call has resolved.
/// However, there are no requirements around concurrent flushing: flush
/// may be called even if other flushes or appends are in progress.
class segment_appender {
public:
    using chunk = segment_appender_chunk;

    static constexpr const size_t fallocation_alignment
      = segment_appender_fallocation_alignment;
    static constexpr const size_t write_behind_memory = 1_MiB;

    struct options {
        options(
          ss::io_priority_class p,
          size_t chunks_no,
          std::optional<uint64_t> s,
          storage_resources& r)
          : priority(p)
          , number_of_chunks(chunks_no)
          , segment_size(s)
          , resources(r) {}

        ss::io_priority_class priority;
        size_t number_of_chunks;
        // Generally a segment appender doesn't need to know the target size
        // of the segment it's appending to, but this is used as an input
        // to the dynamic fallocation size algorithm, to avoid falloc'ing
        // more space than a segment would ever need.
        std::optional<uint64_t> segment_size;
        storage_resources& resources;
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

    /**
     * @brief Appends a batch.
     *
     * See the class doc for the general semantics of append() and the
     * return value.
     */
    ss::future<> append(const model::record_batch& batch);

    /**
     * @brief Appends a buffer of size n starting at buf.
     *
     * See the class doc for the general semantics of append() and the
     * return value.
     */
    ss::future<> append(const char* buf, const size_t n);

    /**
     * @brief Appends a byte view.
     *
     * See the class doc for the general semantics of append() and the
     * return value.
     */
    ss::future<> append(bytes_view s);

    /**
     * @brief Appends the contents of an iobuf.
     *
     * See the class doc for the general semantics of append() and the
     * return value.
     */
    ss::future<> append(const iobuf& io);
    ss::future<> truncate(size_t n);
    ss::future<> close();
    ss::future<> flush();

    struct callbacks {
        virtual ~callbacks() = default;
        virtual void committed_physical_offset(size_t) = 0;
    };

    void set_callbacks(callbacks* callbacks) { _callbacks = callbacks; }

    constexpr ss::io_priority_class get_priority_class() const {
        return _opts.priority;
    }

private:
    using chunk_ptr = ss::lw_shared_ptr<chunk>;

    void dispatch_background_head_write();
    ss::future<> do_next_adaptive_fallocation();
    ss::future<> hydrate_last_half_page();
    ss::future<> do_truncation(size_t);
    ss::future<> do_append(const char* buf, size_t n);

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

    // Reset the bit-map tracking unwritten batch types in the `_head` chunk.
    void reset_batch_types_to_write() { _batch_types_to_write = 0; }

    uint64_t batch_types_to_write() const { return _batch_types_to_write; }

    // called to assert that no writes are currently in progress, dying if
    // there are
    void check_no_dispatched_writes();

    ss::file _out;
    options _opts;
    bool _closed{false};
    size_t _committed_offset{0};
    size_t _fallocation_offset{0};
    size_t _bytes_flush_pending{0};
    ssx::semaphore _concurrent_flushes;
    chunk_ptr _head;
    // ensures that writes to the *same* head are sequenced in order, each write
    ss::lw_shared_ptr<ssx::semaphore> _prev_head_write;

    struct flush_op {
        explicit flush_op(size_t offset)
          : offset(offset) {}
        size_t offset;
        ss::promise<> p;

        friend std::ostream& operator<<(std::ostream& s, const flush_op& op) {
            fmt::print(s, "{{offest: {}}}", op.offset);
            return s;
        }
    };

    // There is one segment_appender per partition replica so we don't want to
    // allocate too many elements by default and hence limit.
    // Limit to 16 elements which is about 640 bytes per chunk.
    using flush_ops_container
      = fragmented_vector<flush_op, sizeof(flush_op) * 16>;
    flush_ops_container _flush_ops;
    size_t _flushed_offset{0};
    size_t _stable_offset{0};

    // like flush, but wait on fibers. used by truncate() and close() which are
    // still heavy weight operations compared to regular flush()
    ss::future<> hard_flush();

    enum class write_state : char { QUEUED = 1, DISPATCHED, DONE };

    struct inflight_write {
        using enum write_state;

        // true if the write extends to the end of the chunk, i.e., this
        // is the last write that will use the current chunk before it
        // is recycled
        bool full;

        // the current state of the write
        write_state state = QUEUED;

        // the chunk containing the data for this write, may be nulled out
        // when the chunk moves to DONE state
        chunk_ptr chunk;

        // the aligned begin and end offsets into the chunk covering the
        // region to write
        size_t chunk_begin, chunk_end;

        // The aligned file offset where this write begins. That is, the bytes
        // at chunk.data() + chunk_begin are written to file_start_offset in the
        // file.
        size_t file_start_offset;

        // The committed file offset after this this write, i.e., the offset
        // one beyond the last byte logically written by this write.
        size_t committed_offset;

        /**
         * @brief Set the state of the write
         *
         * I'm just here as a handy place to hide an assert.
         */
        void set_state(write_state new_state) {
            // the only allowed transitions are QUEUED -> DISPATCHED -> DONE
            vassert(
              state < DONE && (int)new_state == (int)state + 1,
              "bad transition {} -> {}",
              (int)state,
              (int)new_state);
            state = new_state;
        }

        /**
         * @brief Try to merge the given write with this one.
         *
         * If the passed write is "compatible" with this write, merge
         * it with this one so both writes are covered by "this" object.
         *
         * This is not a general purpose method: it expects to only be passed
         * the immediately subsequent write as "other" and so asserts on
         * conditions that must be true in this case rather than simply checking
         * them as part of the merge check.
         *
         * @param other the write to try to merge in to this one
         * @param prior_committed_offset the offset prior to updating the offset
         * for the 'other' write, used only as a sanity check that the writes
         * are adjacent.
         * @return true iff the merge succeeded and this object was updated
         */
        bool
        try_merge(const inflight_write& other, size_t prior_committed_offset);

        friend std::ostream&
        operator<<(std::ostream& s, const inflight_write& op);
    };

    friend std::ostream& operator<<(std::ostream& s, const inflight_write& op);

    ss::chunked_fifo<ss::lw_shared_ptr<inflight_write>> _inflight;
    // A gauge of the current number of oustanding dispatched writes, equal to
    // the count of elements in the _inflight container which have state ==
    // DISPATCHED
    size_t _inflight_dispatched{0};
    // A counter of the number of dispatched writes (i.e., dma_write calls) over
    // the lifetime of the appender
    size_t _dispatched_writes{0};
    // A counter of the number of writes that were succesfully merged into a
    // queued write prior to dispatch, hence don't need to be separately
    // dispatched
    size_t _merged_writes{0};
    callbacks* _callbacks = nullptr;
    ss::future<>
    maybe_advance_stable_offset(const ss::lw_shared_ptr<inflight_write>&);
    ss::future<> process_flush_ops(size_t);

    ss::timer<ss::lowres_clock> _inactive_timer;
    void handle_inactive_timer();

    size_t _chunk_size{0};

    // Bit-map tracking the types of batches in the `_head` chunk that have
    // not been written to disk yet.
    static_assert(static_cast<uint8_t>(model::record_batch_type::MAX) <= 63);
    uint64_t _batch_types_to_write{0};

    friend std::ostream& operator<<(std::ostream&, const segment_appender&);
    friend class file_io_sanitizer;
    friend struct segment_appender_test_accessor;
};

using segment_appender_ptr = std::unique_ptr<segment_appender>;

} // namespace storage
