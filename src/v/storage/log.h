#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/log_segment_reader.h"
#include "storage/types.h"

#include <seastar/core/shared_ptr.hh>

/// \brief Non-synchronized log management class.
///
/// Offset management
///
/// Kafka records has the following offset-related fields
/// Batch level:
///
/// FirstOffset [int64] - base offset of the batch equal to  offset of the
///                       first record
/// LastOffsetDelta [int32] - offset delta for last record in batch equals
///                           FirstOffset + NumberOfMessages  + 1
/// Record level:
///
/// OffsetDelta [varint] - record position in the batch starting from 0
///
/// For the batch with base offset 10 and 4 records the offsets are calculated
/// as follows:
///
/// Batch header:
///   FirstOffset: 10
///   LastOffsetDelta: 3
///
///   Record #1:
///     OffsetDelta: 0
///   Record #2
///     OffsetDelta: 1
///   Record #3
///     OffsetDelta: 2
///   Record #4
///     OffsetDelta: 3
/// Subsequent batch will have offset 14.
///
namespace storage {

class log final {
public:
    class impl {
    public:
        explicit impl(model::ntp n, ss::sstring log_directory) noexcept
          : _ntp(std::move(n))
          , _workdir(std::move(log_directory)) {}
        virtual ~impl() noexcept = default;

        virtual ss::future<>
        truncate(model::offset offset, model::term_id term) = 0;

        virtual ss::future<append_result>
        append(model::record_batch_reader&& r, log_append_config cfg) = 0;

        virtual model::record_batch_reader make_reader(log_reader_config) = 0;
        virtual ss::future<> close() = 0;
        virtual ss::future<> flush() = 0;

        const model::ntp& ntp() const { return _ntp; }
        const ss::sstring& work_directory() const { return _workdir; }

        virtual size_t segment_count() const = 0;
        virtual model::offset max_offset() const = 0;
        virtual model::offset start_offset() const = 0;
        virtual model::offset committed_offset() const = 0;

    private:
        model::ntp _ntp;
        ss::sstring _workdir;
    };

public:
    explicit log(ss::shared_ptr<impl> i)
      : _impl(i) {}
    ss::future<> close() { return _impl->close(); }
    ss::future<> flush() { return _impl->flush(); }

    ss::future<> truncate(model::offset offset, model::term_id term) {
        return _impl->truncate(offset, term);
    }

    model::record_batch_reader make_reader(log_reader_config cfg) {
        return _impl->make_reader(std::move(cfg));
    }

    ss::future<append_result>
    append(model::record_batch_reader&& r, log_append_config cfg) {
        return _impl->append(std::move(r), cfg);
    }

    const model::ntp& ntp() const { return _impl->ntp(); }

    const ss::sstring& work_directory() const {
        return _impl->work_directory();
    }

    size_t segment_count() const { return _impl->segment_count(); }

    model::offset start_offset() const { return _impl->start_offset(); }

    model::offset max_offset() const { return _impl->max_offset(); }

    model::offset committed_offset() const { return _impl->committed_offset(); }

private:
    ss::shared_ptr<impl> _impl;
};

inline std::ostream& operator<<(std::ostream& o, storage::log lg) {
    return o << "{start:" << lg.start_offset() << ", max:" << lg.max_offset()
             << ", committed:" << lg.committed_offset() << "}";
}

class log_manager;
log make_memory_backed_log(model::ntp, ss::sstring);
log make_disk_backed_log(model::ntp, log_manager&, log_set);

} // namespace storage
