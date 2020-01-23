#pragma once
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/types.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

namespace storage {

// Log writer interface, which represents a
// consumer for record_batch_readers. It accepts
// a concrete implementation, which knows how to serialize
// data at a particular version.
class log_appender {
public:
    class impl {
    public:
        virtual ~impl() = default;

        virtual ss::future<> initialize() = 0;
        virtual ss::future<ss::stop_iteration>
        operator()(model::record_batch&&) = 0;
        virtual ss::future<append_result> end_of_stream() = 0;
    };

    explicit log_appender(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    log_appender(log_appender&&) noexcept = default;
    log_appender& operator=(log_appender&&) noexcept = default;

    ss::future<> initialize() { return _impl->initialize(); }

    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        return _impl->operator()(std::move(batch));
    }

    ss::future<append_result> end_of_stream() { return _impl->end_of_stream(); }

private:
    std::unique_ptr<impl> _impl;
};

} // namespace storage
