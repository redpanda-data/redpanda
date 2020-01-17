#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/types.h"

#include <seastar/core/future.hh>

namespace storage {

class disk_log_impl;
class log_segment_appender;

ss::future<> write(log_segment_appender&, const model::record_batch&);

// Log writer interface, which represents a
// consumer for record_batch_readers. It accepts
// a concrete implementation, which knows how to serialize
// data at a particular version.
class log_writer {
public:
    class impl {
    public:
        virtual ~impl() {}

        virtual ss::future<> initialize() = 0;
        virtual ss::future<ss::stop_iteration>
        operator()(model::record_batch&&) = 0;
        virtual ss::future<append_result> end_of_stream() = 0;
    };

    explicit log_writer(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    log_writer(log_writer&& o) = default;
    log_writer& operator=(log_writer&& o) = default;

    ss::future<> initialize() { return _impl->initialize(); }

    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        return _impl->operator()(std::move(batch));
    }

    ss::future<append_result> end_of_stream() { return _impl->end_of_stream(); }

private:
    std::unique_ptr<impl> _impl;
};

// Serializer for the current batch version.
class default_log_writer final : public log_writer::impl {
public:
    default_log_writer(
      disk_log_impl& log,
      log_append_config config,
      log_clock::time_point append_time,
      model::offset offset) noexcept;

    virtual ss::future<> initialize() override;

    virtual ss::future<ss::stop_iteration>
    operator()(model::record_batch&&) override;

    virtual ss::future<append_result> end_of_stream() override;

private:
    disk_log_impl& _log;
    log_append_config _config;
    log_clock::time_point _append_time;
    model::offset _base_offset;
    model::offset _last_offset;
};

} // namespace storage
