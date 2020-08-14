#pragma once

#include "model/fundamental.h"
#include "storage/disk_log_appender.h"
#include "storage/failure_probes.h"
#include "storage/kvstore.h"
#include "storage/lock_manager.h"
#include "storage/log.h"
#include "storage/log_reader.h"
#include "storage/probe.h"
#include "storage/segment_appender.h"
#include "storage/segment_reader.h"
#include "storage/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

namespace storage {

class disk_log_impl final : public log::impl {
public:
    using failure_probes = storage::log_failure_probes;

    disk_log_impl(ntp_config, log_manager&, segment_set, kvstore&);
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
    ss::future<> compact(compaction_config) final;

    ss::future<model::offset> monitor_eviction(ss::abort_source&) final;
    void set_collectible_offset(model::offset) final;

    ss::future<model::record_batch_reader> make_reader(log_reader_config) final;
    ss::future<model::record_batch_reader> make_reader(timequery_config);
    // External synchronization: only one append can be performed at a time.
    log_appender make_appender(log_append_config cfg) final;
    /// timequery
    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) final;
    size_t segment_count() const final { return _segs.size(); }
    offset_stats offsets() const final;
    std::optional<model::term_id> get_term(model::offset) const final;
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

    // key types used to store data in key-value store
    enum class kvstore_key_type : int8_t {
        start_offset = 0,
    };

    ss::future<model::record_batch_reader>
      make_unchecked_reader(log_reader_config);

    bytes start_offset_key() const;
    model::offset read_start_offset() const;

    ss::future<> do_compact(compaction_config);
    ss::future<> gc(compaction_config);

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

    ss::future<>
    garbage_collect_max_partition_size(size_t max_bytes, ss::abort_source*);
    ss::future<>
    garbage_collect_oldest_segments(model::timestamp, ss::abort_source*);
    ss::future<> garbage_collect_segments(
      model::offset, ss::abort_source*, std::string_view);
    model::offset size_based_gc_max_offset(size_t);
    model::offset time_based_gc_max_offset(model::timestamp);

private:
    struct eviction_monitor {
        ss::promise<model::offset> promise;
        ss::abort_source::subscription subscription;
    };
    bool _closed{false};
    log_manager& _manager;
    segment_set _segs;
    kvstore& _kvstore;
    model::offset _start_offset;
    lock_manager _lock_mngr;
    storage::probe _probe;
    failure_probes _failure_probes;
    std::optional<eviction_monitor> _eviction_monitor;
    model::offset _max_collectible_offset;
};

} // namespace storage
