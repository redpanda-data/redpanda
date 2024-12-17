// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once
#include "storage/log.h"
#include "storage/offset_translator_state.h"

namespace raft {

using append_delay_generator = std::function<std::chrono::milliseconds()>;

class failure_injectable_log final : public storage::log {
public:
    // sets the generator for the delay to be injected when appending a record
    // batch to the log
    void set_append_delay(std::optional<append_delay_generator> generator) {
        _append_delay_generator = std::move(generator);
    }

public:
    explicit failure_injectable_log(
      ss::shared_ptr<storage::log> underlying_log) noexcept;

    failure_injectable_log(failure_injectable_log&&) noexcept = delete;
    failure_injectable_log& operator=(failure_injectable_log&&) noexcept
      = delete;
    failure_injectable_log(const failure_injectable_log&) = delete;
    failure_injectable_log& operator=(const failure_injectable_log&) = delete;
    ~failure_injectable_log() noexcept final = default;

    ss::future<>
    start(std::optional<storage::truncate_prefix_config> cfg) final;
    ss::future<> housekeeping(storage::housekeeping_config cfg) final;

    ss::future<> truncate(storage::truncate_config) final;

    ss::future<> truncate_prefix(storage::truncate_prefix_config) final;
    ss::future<> gc(storage::gc_config) final;
    ss::future<> apply_segment_ms() final;

    ss::future<model::record_batch_reader>
      make_reader(storage::log_reader_config) final;

    storage::log_appender make_appender(storage::log_append_config) final;

    ss::future<std::optional<ss::sstring>> close() final;

    ss::future<> remove() final;

    ss::future<> flush() final;

    ss::future<std::optional<storage::timequery_result>>
      timequery(storage::timequery_config) final;

    ss::lw_shared_ptr<const storage::offset_translator_state>
    get_offset_translator_state() const final;

    model::offset_delta offset_delta(model::offset) const final;

    model::offset from_log_offset(model::offset) const final;

    model::offset to_log_offset(model::offset) const final;

    bool is_new_log() const final;

    size_t segment_count() const final;

    storage::offset_stats offsets() const final;

    size_t get_log_truncation_counter() const noexcept final;

    model::offset find_last_term_start_offset() const final;

    model::timestamp start_timestamp() const final;

    std::ostream& print(std::ostream& o) const final;

    std::optional<model::term_id> get_term(model::offset) const final;

    std::optional<model::offset>
      get_term_last_offset(model::term_id) const final;

    std::optional<model::offset> index_lower_bound(model::offset o) const final;

    ss::future<model::offset> monitor_eviction(ss::abort_source&) final;

    size_t size_bytes() const final;

    uint64_t size_bytes_after_offset(model::offset o) const final;

    ss::future<std::optional<storage::log::offset_range_size_result_t>>
    offset_range_size(
      model::offset first,
      model::offset last,
      ss::io_priority_class io_priority) final;

    ss::future<std::optional<offset_range_size_result_t>> offset_range_size(
      model::offset first,
      offset_range_size_requirements_t target,
      ss::io_priority_class io_priority) final;

    bool is_compacted(model::offset first, model::offset last) const final;

    void set_overrides(storage::ntp_config::default_overrides) final;

    bool notify_compaction_update() final;

    int64_t compaction_backlog() const final;

    ss::future<storage::usage_report> disk_usage(storage::gc_config) final;

    ss::future<storage::reclaimable_offsets>
    get_reclaimable_offsets(storage::gc_config cfg) final;

    void set_cloud_gc_offset(model::offset) final;

    const storage::segment_set& segments() const final;
    storage::segment_set& segments() final;

    ss::future<> force_roll(ss::io_priority_class) final;

    storage::probe& get_probe() final;

    size_t reclaimable_size_bytes() const final;

    std::optional<model::offset>
      retention_offset(storage::gc_config) const final;

private:
    ss::shared_ptr<storage::log> _underlying_log;
    std::optional<append_delay_generator> _append_delay_generator;
    friend std::ostream&
    operator<<(std::ostream& o, const failure_injectable_log& lg) {
        return lg.print(o);
    }
};
} // namespace raft
