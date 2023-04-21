/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "storage/log_appender.h"
#include "storage/ntp_config.h"
#include "storage/segment_reader.h"
#include "storage/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>

#include <utility>

namespace storage {

/// \brief Non-synchronized log management class.
/// The class follows the pimpl idiom for: appends,reads and concrete.
/// there are 2 default implementations memory-backed and disk-backed
///
/// use like a seastar::shared_ptr<> (non-thread safe)
///
class log final {
public:
    class impl {
    public:
        explicit impl(ntp_config cfg) noexcept
          : _config(std::move(cfg))
          , _stm_manager(ss::make_lw_shared<storage::stm_manager>()) {}
        impl(impl&&) noexcept = default;
        impl& operator=(impl&&) noexcept = default;
        impl(const impl&) = delete;
        impl& operator=(const impl&) = delete;
        virtual ~impl() noexcept = default;

        // it shouldn't block for a long time as it will block other logs
        // eviction
        virtual ss::future<> compact(compaction_config) = 0;
        virtual ss::future<> truncate(truncate_config) = 0;
        virtual ss::future<> truncate_prefix(truncate_prefix_config) = 0;

        // TODO should compact be merged in this?
        // run housekeeping task, like rolling segments
        virtual ss::future<> do_housekeeping() = 0;

        virtual ss::future<model::record_batch_reader>
          make_reader(log_reader_config) = 0;
        virtual log_appender make_appender(log_append_config) = 0;

        // final operation. Invalid filesystem state after
        virtual ss::future<std::optional<ss::sstring>> close() = 0;
        // final operation. Invalid state after
        virtual ss::future<> remove() = 0;

        virtual ss::future<> flush() = 0;

        virtual ss::future<std::optional<timequery_result>>
          timequery(timequery_config) = 0;

        const ntp_config& config() const { return _config; }

        virtual size_t segment_count() const = 0;
        virtual storage::offset_stats offsets() const = 0;
        virtual model::timestamp start_timestamp() const = 0;
        virtual std::ostream& print(std::ostream& o) const = 0;
        virtual std::optional<model::term_id> get_term(model::offset) const = 0;
        virtual std::optional<model::offset>
          get_term_last_offset(model::term_id) const = 0;

        virtual ss::future<model::offset>
        monitor_eviction(ss::abort_source&) = 0;
        ss::lw_shared_ptr<storage::stm_manager> stm_manager() {
            return _stm_manager;
        }

        virtual size_t size_bytes() const = 0;
        // Byte size of the log for all segments before offset 'o'
        virtual size_t size_bytes_until_offset(model::offset o) const = 0;
        virtual ss::future<>
          update_configuration(ntp_config::default_overrides) = 0;

        virtual int64_t compaction_backlog() const = 0;

    private:
        ntp_config _config;

    protected:
        ntp_config& mutable_config() { return _config; }
        ss::lw_shared_ptr<storage::stm_manager> _stm_manager;
    };

public:
    explicit log(ss::shared_ptr<impl> i)
      : _impl(std::move(i)) {}
    ss::future<std::optional<ss::sstring>> close() { return _impl->close(); }
    ss::future<> remove() { return _impl->remove(); }
    ss::future<> flush() { return _impl->flush(); }

    /**
     * \brief Truncate the suffix of log at a base offset
     *
     * Having a segment:
     * segment: {[10,10][11,30][31,100][101,103]}
     * Truncate at offset (31) will result in
     * segment: {[10,10][11,30]}
     * Truncating mid-batch is forbidden:
     * Truncate at offset 35 will result in an exception.
     */
    ss::future<> truncate(truncate_config cfg) { return _impl->truncate(cfg); }

    /**
     * \brief Truncate the prefix of a log at a base offset
     *
     * Having a segment:
     * segment: {[10,10][11,30][31,100][101,103]}
     * Truncate prefix at offset (31) will result in
     * segment: {[31,100][101,103]}
     * Truncate prefix mid-batch (at offset 35) will result in
     * segment {[31, 100][101,103]}, but reported start_offset will be 35.
     *
     * The typical use case is installing a snapshot. In this case the snapshot
     * covers a section of the log up to and including a position X (generally
     * would be a batch's last offset). To prepare a log for a snapshot it would
     * be prefix truncated at offset X+1, so that the next element in the log to
     * be replayed against the snapshot is X+1.
     */
    ss::future<> truncate_prefix(truncate_prefix_config cfg) {
        return _impl->truncate_prefix(cfg);
    }

    ss::future<model::record_batch_reader> make_reader(log_reader_config cfg) {
        return _impl->make_reader(cfg);
    }

    log_appender make_appender(log_append_config cfg) {
        return _impl->make_appender(cfg);
    }

    const ntp_config& config() const { return _impl->config(); }

    size_t segment_count() const { return _impl->segment_count(); }

    model::timestamp start_timestamp() const {
        return _impl->start_timestamp();
    }

    storage::offset_stats offsets() const { return _impl->offsets(); }

    std::optional<model::term_id> get_term(model::offset o) const {
        return _impl->get_term(o);
    }

    std::optional<model::offset>
    get_term_last_offset(model::term_id term) const {
        return _impl->get_term_last_offset(term);
    }

    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) {
        return _impl->timequery(cfg);
    }

    ss::future<> compact(compaction_config cfg) { return _impl->compact(cfg); }

    ss::future<> housekeeping() { return _impl->do_housekeeping(); }
    /**
     * \brief Returns a future that resolves when log eviction is scheduled
     *
     * Important note: The api may throw ss::abort_requested_exception when
     * passed in abort source was triggered or storage::segment_closed_exception
     * when log was closed while waiting for eviction to happen.
     *
     */
    ss::future<model::offset> monitor_eviction(ss::abort_source& as) {
        return _impl->monitor_eviction(as);
    }

    ss::lw_shared_ptr<storage::stm_manager> stm_manager() {
        return _impl->stm_manager();
    }

    ss::future<> update_configuration(ntp_config::default_overrides o) {
        return _impl->update_configuration(o);
    }

    int64_t compaction_backlog() const { return _impl->compaction_backlog(); }

    std::ostream& print(std::ostream& o) const { return _impl->print(o); }

    size_t size_bytes() const { return _impl->size_bytes(); }

    size_t size_bytes_until_offset(model::offset o) const {
        return _impl->size_bytes_until_offset(o);
    }

    impl* get_impl() const { return _impl.get(); }

private:
    ss::shared_ptr<impl> _impl;

    friend std::ostream& operator<<(std::ostream& o, const log& lg) {
        return lg.print(o);
    }
};

class log_manager;
class segment_set;
class kvstore;
log make_disk_backed_log(
  ntp_config,
  log_manager&,
  segment_set,
  kvstore&,
  ss::sharded<features::feature_table>& feature_table);

} // namespace storage
