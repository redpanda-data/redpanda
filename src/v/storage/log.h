#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/log_segment.h"
#include "storage/log_segment_appender.h"
#include "storage/offset_tracker.h"

namespace storage {

class log_manager;

using log_clock = lowres_clock;

class log {
public:
    log(model::namespaced_topic_partition, log_manager&, log_set) noexcept;

    future<> close();

    const log_set& segments() const {
        return _segs;
    }

private:
    model::namespaced_topic_partition _ntp;
    log_manager& _manager;
    log_set _segs;
    offset_tracker _tracker;
};

using log_ptr = lw_shared_ptr<log>;

} // namespace storage
