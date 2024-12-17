// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "raft/tests/failure_injectable_log.h"
namespace raft {

failure_injectable_log::failure_injectable_log(
  ss::shared_ptr<storage::log> underlying_log) noexcept
  : storage::log(underlying_log->config().copy())
  , _underlying_log(std::move(underlying_log)) {}

ss::future<> failure_injectable_log::start(
  std::optional<storage::truncate_prefix_config> cfg) {
    return _underlying_log->start(cfg);
}

ss::future<>
failure_injectable_log::housekeeping(storage::housekeeping_config cfg) {
    return _underlying_log->housekeeping(cfg);
}

ss::future<> failure_injectable_log::truncate(storage::truncate_config cfg) {
    return _underlying_log->truncate(cfg);
}

ss::future<>
failure_injectable_log::truncate_prefix(storage::truncate_prefix_config tpc) {
    return _underlying_log->truncate_prefix(tpc);
}
ss::future<> failure_injectable_log::gc(storage::gc_config cfg) {
    return _underlying_log->gc(cfg);
}

ss::future<> failure_injectable_log::apply_segment_ms() {
    return _underlying_log->apply_segment_ms();
};

ss::future<model::record_batch_reader>
failure_injectable_log::make_reader(storage::log_reader_config cfg) {
    return _underlying_log->make_reader(cfg);
}
namespace {
struct delay_introducing_appender : public storage::log_appender::impl {
    delay_introducing_appender(
      storage::log_appender underlying, append_delay_generator generator)
      : _underlying(std::move(underlying))
      , _append_delay_generator(std::move(generator)) {}

    /// non-owning reference - do not steal the iobuf
    ss::future<ss::stop_iteration> operator()(model::record_batch& b) final {
        co_await ss::sleep(_append_delay_generator());
        co_return co_await _underlying(b);
    }

    ss::future<storage::append_result> end_of_stream() final {
        return _underlying.end_of_stream();
    }
    storage::log_appender _underlying;
    append_delay_generator _append_delay_generator;
};
} // namespace

storage::log_appender
failure_injectable_log::make_appender(storage::log_append_config cfg) {
    if (_append_delay_generator) {
        return storage::log_appender(
          std::make_unique<delay_introducing_appender>(
            _underlying_log->make_appender(cfg), *_append_delay_generator));
    }

    return _underlying_log->make_appender(cfg);
}

ss::future<std::optional<ss::sstring>> failure_injectable_log::close() {
    return _underlying_log->close();
}

ss::future<> failure_injectable_log::remove() {
    return _underlying_log->remove();
}

ss::future<> failure_injectable_log::flush() {
    return _underlying_log->flush();
}

ss::future<std::optional<storage::timequery_result>>
failure_injectable_log::timequery(storage::timequery_config cfg) {
    return _underlying_log->timequery(cfg);
}

ss::lw_shared_ptr<const storage::offset_translator_state>
failure_injectable_log::get_offset_translator_state() const {
    return _underlying_log->get_offset_translator_state();
}

model::offset_delta
failure_injectable_log::offset_delta(model::offset o) const {
    return _underlying_log->offset_delta(o);
}

model::offset failure_injectable_log::from_log_offset(model::offset o) const {
    return _underlying_log->from_log_offset(o);
}

model::offset failure_injectable_log::to_log_offset(model::offset o) const {
    return _underlying_log->to_log_offset(o);
}

bool failure_injectable_log::is_new_log() const {
    return _underlying_log->is_new_log();
}

size_t failure_injectable_log::segment_count() const {
    return _underlying_log->segment_count();
}

storage::offset_stats failure_injectable_log::offsets() const {
    return _underlying_log->offsets();
}

size_t failure_injectable_log::get_log_truncation_counter() const noexcept {
    return _underlying_log->get_log_truncation_counter();
}

model::offset failure_injectable_log::find_last_term_start_offset() const {
    return _underlying_log->find_last_term_start_offset();
}

model::timestamp failure_injectable_log::start_timestamp() const {
    return _underlying_log->start_timestamp();
}

std::ostream&
failure_injectable_log::failure_injectable_log::print(std::ostream& o) const {
    return _underlying_log->print(o);
}

std::optional<model::term_id>
failure_injectable_log::get_term(model::offset o) const {
    return _underlying_log->get_term(o);
}

std::optional<model::offset>
failure_injectable_log::get_term_last_offset(model::term_id t) const {
    return _underlying_log->get_term_last_offset(t);
}

std::optional<model::offset>
failure_injectable_log::index_lower_bound(model::offset o) const {
    return _underlying_log->index_lower_bound(o);
}

ss::future<model::offset>
failure_injectable_log::monitor_eviction(ss::abort_source& as) {
    return _underlying_log->monitor_eviction(as);
}

size_t failure_injectable_log::size_bytes() const {
    return _underlying_log->size_bytes();
}

uint64_t
failure_injectable_log::size_bytes_after_offset(model::offset o) const {
    return _underlying_log->size_bytes_after_offset(o);
}

ss::future<std::optional<storage::log::offset_range_size_result_t>>
failure_injectable_log::offset_range_size(
  model::offset first, model::offset last, ss::io_priority_class io_priority) {
    return _underlying_log->offset_range_size(first, last, io_priority);
}

ss::future<std::optional<failure_injectable_log::offset_range_size_result_t>>
failure_injectable_log::offset_range_size(
  model::offset first,
  offset_range_size_requirements_t target,
  ss::io_priority_class io_priority) {
    return _underlying_log->offset_range_size(first, target, io_priority);
}

bool failure_injectable_log::is_compacted(
  model::offset first, model::offset last) const {
    return _underlying_log->is_compacted(first, last);
}

void failure_injectable_log::set_overrides(
  storage::ntp_config::default_overrides overrides) {
    mutable_config().set_overrides(overrides);
    return _underlying_log->set_overrides(overrides);
}

bool failure_injectable_log::notify_compaction_update() {
    return _underlying_log->notify_compaction_update();
}

int64_t failure_injectable_log::compaction_backlog() const {
    return _underlying_log->compaction_backlog();
}

ss::future<storage::usage_report>
failure_injectable_log::disk_usage(storage::gc_config cfg) {
    return _underlying_log->disk_usage(cfg);
}

ss::future<storage::reclaimable_offsets>
failure_injectable_log::get_reclaimable_offsets(storage::gc_config cfg) {
    return _underlying_log->get_reclaimable_offsets(cfg);
}

void failure_injectable_log::set_cloud_gc_offset(model::offset o) {
    return _underlying_log->set_cloud_gc_offset(o);
}

const storage::segment_set& failure_injectable_log::segments() const {
    return _underlying_log->segments();
}
storage::segment_set& failure_injectable_log::segments() {
    return _underlying_log->segments();
}

ss::future<> failure_injectable_log::force_roll(ss::io_priority_class iop) {
    return _underlying_log->force_roll(iop);
}

storage::probe& failure_injectable_log::get_probe() {
    return _underlying_log->get_probe();
}

size_t failure_injectable_log::reclaimable_size_bytes() const {
    return _underlying_log->reclaimable_size_bytes();
}

std::optional<model::offset>
failure_injectable_log::retention_offset(storage::gc_config cfg) const {
    return _underlying_log->retention_offset(cfg);
}

} // namespace raft
