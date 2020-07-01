#pragma once
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "storage/log_appender.h"
#include "storage/segment_reader.h"
#include "storage/types.h"

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
     * \brief Truncate log at base offset
     * Having a segment:
     * segment: {[10,10][11,30][31,100][101,103]}
     * Truncate at offset (31) will result in
     * segment: {[10,10][11,30]}
     */
    ss::future<> truncate(truncate_config cfg) { return _impl->truncate(cfg); }
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
