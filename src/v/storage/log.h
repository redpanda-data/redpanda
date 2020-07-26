#pragma once
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "storage/log_appender.h"
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
          : _config(std::move(cfg)) {}
        impl(impl&&) noexcept = default;
        impl& operator=(impl&&) noexcept = default;
        impl(const impl&) = delete;
        impl& operator=(const impl&) = delete;
        virtual ~impl() noexcept = default;

        virtual ss::future<> compact(compaction_config) = 0;
        virtual ss::future<> truncate(truncate_config) = 0;
        virtual ss::future<> truncate_prefix(truncate_prefix_config) = 0;

        virtual ss::future<model::record_batch_reader>
          make_reader(log_reader_config) = 0;
        virtual log_appender make_appender(log_append_config) = 0;

        // final operation. Invalid filesystem state after
        virtual ss::future<> close() = 0;
        // final operation. Invalid state after
        virtual ss::future<> remove() = 0;

        virtual ss::future<> flush() = 0;

        virtual ss::future<std::optional<timequery_result>>
          timequery(timequery_config) = 0;

        const ntp_config& config() const { return _config; }

        virtual size_t segment_count() const = 0;
        virtual storage::offset_stats offsets() const = 0;
        virtual std::ostream& print(std::ostream& o) const = 0;
        virtual std::optional<model::term_id> get_term(model::offset) const = 0;

        virtual ss::future<eviction_range_lock>
        monitor_eviction(ss::abort_source&) = 0;

    private:
        ntp_config _config;
    };

public:
    explicit log(ss::shared_ptr<impl> i)
      : _impl(std::move(i)) {}
    ss::future<> close() { return _impl->close(); }
    ss::future<> remove() { return _impl->remove(); }
    ss::future<> flush() { return _impl->flush(); }

    /**
     * \brief Truncate the suffix of log at a base offset
     *
     * Having a segment:
     * segment: {[10,10][11,30][31,100][101,103]}
     * Truncate at offset (31) will result in
     * segment: {[10,10][11,30]}
     */
    ss::future<> truncate(truncate_config cfg) { return _impl->truncate(cfg); }

    /**
     * \brief Truncate the prefix of a log at a base offset
     *
     * Having a segment:
     * segment: {[10,10][11,30][31,100][101,103]}
     * Truncate prefix at offset (31) will result in
     * segment: {[31,100][101,103]}
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

    storage::offset_stats offsets() const { return _impl->offsets(); }

    std::optional<model::term_id> get_term(model::offset o) const {
        return _impl->get_term(o);
    }
    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) {
        return _impl->timequery(cfg);
    }

    ss::future<> compact(compaction_config cfg) { return _impl->compact(cfg); }

    /**
     * \brief Returns a future that resolves when log eviction is scheduled
     *
     * This method allows user to hold the lock preventing garbadge collection.
     * The lock is released as soon as eviction_range_lock is deleted. Eviction
     * range lock contains RAII read lock holders and last offset of the last
     * batch that is going to be evicted during garbadge collection. The garbage
     * collection is a mechanism that is responsible to evict old log segments
     * according to size and time based retention policies.
     *
     * Important note: The api may throw ss::abort_requested_exception when
     * passed in abort source was triggered or storage::segment_closed_exception
     * when log was closed while waiting for eviction to happen.
     *
     */
    ss::future<eviction_range_lock> monitor_eviction(ss::abort_source& as) {
        return _impl->monitor_eviction(as);
    }

    std::ostream& print(std::ostream& o) const { return _impl->print(o); }

    impl* get_impl() const { return _impl.get(); }

private:
    ss::shared_ptr<impl> _impl;

    friend std::ostream& operator<<(std::ostream& o, const log& lg);
};

inline std::ostream& operator<<(std::ostream& o, const storage::log& lg) {
    return lg.print(o);
}

class log_manager;
class segment_set;
class kvstore;
log make_memory_backed_log(ntp_config);
log make_disk_backed_log(ntp_config, log_manager&, segment_set, kvstore&);

} // namespace storage
