#pragma once

#include "storage/disk_log_appender.h"
#include "storage/failure_probes.h"
#include "storage/lock_manager.h"
#include "storage/log.h"
#include "storage/log_reader.h"
#include "storage/probe.h"
#include "storage/segment_appender.h"
#include "storage/segment_reader.h"

#include <seastar/core/gate.hh>

namespace storage {

class disk_log_impl final : public log::impl {
    using failure_probes = storage::log_failure_probes;

public:
    disk_log_impl(ntp_config, log_manager&, segment_set);
    ~disk_log_impl() override;
    disk_log_impl(disk_log_impl&&) noexcept = default;
    disk_log_impl& operator=(disk_log_impl&&) noexcept = delete;
    disk_log_impl(const disk_log_impl&) = delete;
    disk_log_impl& operator=(const disk_log_impl&) = delete;

    ss::future<> close() final;
    ss::future<> remove() final;
    ss::future<> flush() final;
    ss::future<> truncate(truncate_config) final;
    ss::future<> truncate_prefix(truncate_prefix_config) final;

    ss::future<> gc(
      model::timestamp collection_upper_bound,
      std::optional<size_t> max_partition_retention_size) final;

    ss::future<model::record_batch_reader> make_reader(log_reader_config) final;
    // External synchronization: only one append can be performed at a time.
    log_appender make_appender(log_append_config cfg) final;
    /// timequery
    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) final;
    size_t segment_count() const final { return _segs.size(); }
    model::offset start_offset() const final;
    model::offset dirty_offset() const final;
    std::optional<model::term_id> get_term(model::offset) const final;
    model::offset committed_offset() const final;
    std::ostream& print(std::ostream&) const final;

    ss::future<> maybe_roll(
      model::term_id, model::offset next_offset, ss::io_priority_class);

    probe& get_probe() { return _probe; }
    model::term_id term() const;
    segment_set& segments() { return _segs; }
    const segment_set& segments() const { return _segs; }
    size_t bytes_left_before_roll() const;

private:
    friend class disk_log_appender; // for multi-term appends
    friend class disk_log_builder;  // for tests
    friend std::ostream& operator<<(std::ostream& o, const disk_log_impl& d);

    ss::future<> remove_empty_segments();

    ss::future<> remove_segment_permanently(
      ss::lw_shared_ptr<segment> segment_to_tombsone,
      std::string_view logging_context_msg);

    ss::future<> new_segment(
      model::offset starting_offset,
      model::term_id term_for_this_segment,
      ss::io_priority_class prio);

    ss::future<> do_truncate(truncate_config);
    ss::future<> remove_full_segments(model::offset o);

    ss::future<> do_truncate_prefix(truncate_prefix_config);
    ss::future<> remove_prefix_full_segments(truncate_prefix_config);

    ss::future<> garbage_collect_max_partition_size(size_t max_bytes);
    ss::future<> garbage_collect_oldest_segments(model::timestamp);

private:
    bool _closed{false};
    log_manager& _manager;
    segment_set _segs;
    lock_manager _lock_mngr;
    storage::probe _probe;
    failure_probes _failure_probes;
};

} // namespace storage
