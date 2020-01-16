#pragma once

#include "storage/failure_probes.h"
#include "storage/log.h"
#include "storage/log_reader.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/offset_tracker.h"
#include "storage/probe.h"

namespace storage {

class disk_log_impl final : public log::impl {
    using failure_probes = storage::log_failure_probes;

public:
    disk_log_impl(model::ntp, ss::sstring, log_manager&, log_set);
    ~disk_log_impl() override = default;
    ss::future<> close() final;

    const log_set& segments() const { return _segs; }

    model::record_batch_reader make_reader(log_reader_config) final;

    // External synchronization: only one append can be performed at a time.
    [[gnu::always_inline]] ss::future<append_result>
    append(model::record_batch_reader&& r, log_append_config cfg) final {
        return _failure_probes.append().then(
          [this, r = std::move(r), cfg = std::move(cfg)]() mutable {
              return do_append(std::move(r), std::move(cfg));
          });
    }

    // Can only be called after append().
    log_segment_appender& appender() { return *_appender; }

    /// flushes the _tracker.dirty_offset into _tracker.committed_offset
    ss::future<> flush() final;

    ss::future<> maybe_roll(model::offset);

    [[gnu::always_inline]] ss::future<> truncate(model::offset offset) final {
        return _failure_probes.truncate().then(
          [this, offset]() mutable { return do_truncate(offset); });
    }

    probe& get_probe() { return _probe; }

    size_t segment_count() const final { return _segs.size(); }

    model::offset start_offset() const final {
        if (_segs.empty()) {
            return model::offset{};
        }
        return (*_segs.begin())->base_offset();
    }
    model::offset max_offset() const final { return _tracker.dirty_offset(); }

    model::offset committed_offset() const final {
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

    ss::future<> do_truncate(model::offset);

    ss::future<> truncate_whole_segments(log_set::const_iterator);

private:
    model::term_id _term;
    log_manager& _manager;
    log_set _segs;
    segment_reader_ptr _active_segment;
    segment_appender_ptr _appender;
    offset_tracker _tracker;
    storage::probe _probe;
    failure_probes _failure_probes;
};

} // namespace storage
