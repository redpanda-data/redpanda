#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/log_segment_appender.h"

#include <seastar/core/future.hh>

namespace storage {

class log;

future<> write(log_segment_appender&, const model::record_batch&);

// Log writer interface, which represents a
// consumer for record_batch_readers. It accepts
// a concrete implementation, which knows how to serialize
// data at a particular version.
class log_writer {
public:
    class impl {
    public:
        virtual ~impl() {
        }

        virtual future<stop_iteration> operator()(model::record_batch&&) = 0;
        virtual model::offset end_of_stream() = 0;
    };

    explicit log_writer(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {
    }

    log_writer(log_writer&& o) = default;
    log_writer& operator=(log_writer&& o) = default;

    future<stop_iteration> operator()(model::record_batch&& batch) {
        return _impl->operator()(std::move(batch));
    }

    model::offset end_of_stream() {
        return _impl->end_of_stream();
    }

private:
    std::unique_ptr<impl> _impl;
};

// Serializer for the current batch version.
class default_log_writer : public log_writer::impl {
public:
    explicit default_log_writer(log&) noexcept;

    virtual future<stop_iteration> operator()(model::record_batch&&) override;

    virtual model::offset end_of_stream() override;

private:
    log& _log;
    model::offset _last_offset;
};

} // namespace storage
