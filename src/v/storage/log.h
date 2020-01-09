#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/failure_probes.h"
#include "storage/log_reader.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/offset_tracker.h"
#include "storage/probe.h"

#include <optional>

namespace storage {

class log_manager;

using log_clock = ss::lowres_clock;

struct log_append_config {
    using fsync = ss::bool_class<class skip_tag>;
    fsync should_fsync;
    ss::io_priority_class io_priority;
    model::timeout_clock::time_point timeout;
};

/// \brief Non-synchronized log management class.
///
/// Offset management
///
/// Kafka records has the following offset-related fields
/// Batch level:
///
/// FirstOffset [int64] - base offset of the batch equal to  offset of the
///                       first record
/// LastOffsetDelta [int32] - offset delta for last record in batch equals
///                           FirstOffset + NumberOfMessages  + 1
/// Record level:
///
/// OffsetDelta [varint] - record position in the batch starting from 0
///
/// For the batch with base offset 10 and 4 records the offsets are calculated
/// as follows:
///
/// Batch header:
///   FirstOffset: 10
///   LastOffsetDelta: 3
///
///   Record #1:
///     OffsetDelta: 0
///   Record #2
///     OffsetDelta: 1
///   Record #3
///     OffsetDelta: 2
///   Record #4
///     OffsetDelta: 3
/// Subsequent batch will have offset 14.

class log {
    using failure_probes = storage::log_failure_probes;

public:
    struct append_result {
        log_clock::time_point append_time;
        model::offset base_offset;
        model::offset last_offset;
    };

    log(model::ntp, log_manager&, log_set) noexcept;

    ss::future<> close();

    const log_set& segments() const { return _segs; }

    model::record_batch_reader make_reader(log_reader_config);

    // External synchronization: only one append can be performed at a time.
    [[gnu::always_inline]] ss::future<append_result>
    append(model::record_batch_reader&& r, log_append_config cfg) {
        return _failure_probes.append().then(
          [this, r = std::move(r), cfg = std::move(cfg)]() mutable {
              return do_append(std::move(r), std::move(cfg));
          });
    }

    // Can only be called after append().
    log_segment_appender& appender() { return *_appender; }

    /// flushes the _tracker.dirty_offset into _tracker.committed_offset
    ss::future<> flush();

    ss::future<> maybe_roll(model::offset);

    [[gnu::always_inline]] ss::future<>
    truncate(model::offset offset, model::term_id term) {
        return _failure_probes.truncate().then(
          [this, offset, term]() mutable { return do_truncate(offset, term); });
    }

    ss::sstring base_directory() const;

    const model::ntp& ntp() const { return _ntp; }

    probe& get_probe() { return _probe; }

    model::offset max_offset() const { return _tracker.dirty_offset(); }

    model::offset committed_offset() const {
        return _tracker.committed_offset();
    }

private:
    friend class log_builder;

    ss::future<>
    new_segment(model::offset, model::term_id, const ss::io_priority_class&);

    /// \brief forces a flush() on the last segment & rotates given the current
    /// _term && (tracker.committed_offset+1)
    ss::future<> do_roll();

    ss::future<append_result>
    do_append(model::record_batch_reader&&, log_append_config);

    ss::future<> do_truncate(model::offset, model::term_id);

private:
    model::term_id _term;
    model::ntp _ntp;
    log_manager& _manager;
    log_set _segs;
    segment_reader_ptr _active_segment;
    segment_appender_ptr _appender;
    offset_tracker _tracker;
    storage::probe _probe;
    failure_probes _failure_probes;
};

using log_ptr = ss::lw_shared_ptr<log>;

} // namespace storage
