#pragma once

#include "storage/disk_log_appender.h"
#include "storage/failure_probes.h"
#include "storage/log.h"
#include "storage/log_reader.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/probe.h"

namespace storage {

class disk_log_impl final : public log::impl {
    using failure_probes = storage::log_failure_probes;

public:
    disk_log_impl(model::ntp, ss::sstring, log_manager&, log_set);
    ~disk_log_impl() override;
    ss::future<> close() final;

    const log_set& segments() const { return _segs; }

    model::record_batch_reader make_reader(log_reader_config) final;

    // External synchronization: only one append can be performed at a time.
    log_appender make_appender(log_append_config cfg) final;

    ss::future<> flush() final;

    ss::future<> maybe_roll(
      model::term_id, model::offset next_offset, ss::io_priority_class);

    [[gnu::always_inline]] ss::future<> truncate(model::offset offset) final {
        return _failure_probes.truncate().then(
          [this, offset]() mutable { return do_truncate(offset); });
    }

    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) final;

    probe& get_probe() { return _probe; }

    size_t segment_count() const final { return _segs.size(); }

    model::offset start_offset() const final {
        if (_segs.empty()) {
            return model::offset{};
        }
        return _segs.front()->reader()->base_offset();
    }
    model::offset max_offset() const final {
        for (auto it = _segs.rbegin(); it != _segs.rend(); it++) {
            if (!(*it)->empty()) {
                return (*it)->dirty_offset();
            }
        }
        return model::offset{};
    }
    model::term_id term() const {
        if (_segs.empty()) {
            // does not make sense to return unitinialized term
            // if we have no term, default to the first term.
            // the next append() will truncate if greater
            return model::term_id{0};
        }
        return _segs.back()->term();
    }

    std::optional<model::term_id> get_term(model::offset) const final;

    model::offset committed_offset() const final {
        for (auto it = _segs.rbegin(); it != _segs.rend(); it++) {
            if (!(*it)->empty()) {
                return (*it)->committed_offset();
            }
        }
        return model::offset{};
    }
    std::ostream& print(std::ostream&) const final;

private:
    friend class log_builder;
    friend class disk_log_appender;
    friend class disk_log_builder;

    log_set& segments() { return _segs; }

    ss::future<> remove_empty_segments();

    ss::future<>
      new_segment(model::offset, model::term_id, ss::io_priority_class);

    ss::future<> do_truncate(model::offset);

private:
    bool _closed{false};
    log_manager& _manager;
    log_set _segs;
    storage::probe _probe;
    failure_probes _failure_probes;
};

} // namespace storage
