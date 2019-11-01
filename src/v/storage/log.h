#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/failure_probes.h"
#include "storage/log_reader.h"
#include "storage/log_segment.h"
#include "storage/log_segment_appender.h"
#include "storage/offset_tracker.h"
#include "storage/probe.h"

#include <optional>

namespace storage {

class log_manager;

using log_clock = lowres_clock;

struct log_append_config {
    using fsync = bool_class<class skip_tag>;
    fsync should_fsync;
    io_priority_class io_priority;
    model::timeout_clock::time_point timeout;
};

/// \brief a non-synchronized log management class.
class log {
    using failure_probes = storage::log_failure_probes;

public:
    struct append_result {
        log_clock::time_point append_time;
        model::offset base_offset;
        model::offset last_offset;
    };

    log(model::ntp, log_manager&, log_set) noexcept;

    future<> close();

    const log_set& segments() const {
        return _segs;
    }

    model::record_batch_reader make_reader(log_reader_config);

    // External synchronization: only one append can be performed at a time.
    [[gnu::always_inline]] future<append_result>
    append(model::record_batch_reader&& r, log_append_config cfg) {
        return _failure_probes.append().then(
          [this, r = std::move(r), cfg = std::move(cfg)]() mutable {
              return do_append(std::move(r), std::move(cfg));
          });
    }

    // Can only be called after append().
    log_segment_appender& appender();

    /// flushes the _tracker.dirty_offset into _tracker.committed_offset
    future<> flush();

    future<> maybe_roll();

    [[gnu::always_inline]] future<>
    truncate(model::offset offset, model::term_id term) {
        return _failure_probes.truncate().then(
          [this, offset, term]() mutable { return do_truncate(offset, term); });
    }

    sstring base_directory() const;
    const model::ntp& ntp() const {
        return _ntp;
    }

    probe& get_probe() {
        return _probe;
    }
    model::offset max_offset() const {
        return _tracker.dirty_offset();
    }

private:
    future<>
    new_segment(model::offset, model::term_id, const io_priority_class&);

    /// \brief forces a flush() on the last segment & rotates given the current
    /// _term && (tracker.committed_offset+1)
    future<> do_roll();

    future<> do_roll(model::offset, model::term_id);

    future<append_result>
    do_append(model::record_batch_reader&&, log_append_config);

    future<> do_truncate(model::offset, model::term_id) {
        return make_ready_future<>();
    }

private:
    model::term_id _term;
    model::ntp _ntp;
    log_manager& _manager;
    log_set _segs;
    log_segment_ptr _active_segment;
    std::optional<log_segment_appender> _appender;
    offset_tracker _tracker;
    storage::probe _probe;
    failure_probes _failure_probes;
};

using log_ptr = lw_shared_ptr<log>;

} // namespace storage
