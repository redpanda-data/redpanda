#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/log_reader.h"
#include "storage/log_segment.h"
#include "storage/log_segment_appender.h"
#include "storage/offset_tracker.h"

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

class log {
public:
    struct append_result {
        log_clock::time_point append_time;
        model::offset base_offset;
        model::offset last_offset;
    };

    log(model::namespaced_topic_partition, log_manager&, log_set) noexcept;

    future<> close();

    const log_set& segments() const {
        return _segs;
    }

    model::record_batch_reader make_reader(log_reader_config);

    // External synchronization: only one append can be performed at a time.
    future<append_result>
    append(model::record_batch_reader&&, log_append_config);

    // Can only be called after append().
    log_segment_appender& appender();

    future<> maybe_roll(model::offset);
    future<> roll(int64_t term) {
        return make_ready_future<>();
    }
    future<> truncate(model::offset, int64_t term) {
        return make_ready_future<>();
    }

private:
    future<> new_segment(model::offset, int64_t term, const io_priority_class&);

private:
    model::namespaced_topic_partition _ntp;
    log_manager& _manager;
    log_set _segs;
    log_segment_ptr _active_segment;
    std::optional<log_segment_appender> _appender;
    offset_tracker _tracker;
};

using log_ptr = lw_shared_ptr<log>;

} // namespace storage
