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
    struct impl {
        impl() noexcept = default;
        impl(const impl&) = default;
        impl& operator=(const impl&) = default;
        impl(impl&&) noexcept = default;
        impl& operator=(impl&&) noexcept = default;
        virtual ~impl() = default;

        /// non-owning reference - do not steal the iobuf
        virtual ss::future<ss::stop_iteration>
        operator()(model::record_batch&) = 0;

        virtual ss::future<append_result> end_of_stream() = 0;
    };

    explicit log_appender(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}
    ~log_appender() noexcept = default;
    log_appender(const log_appender&) = delete;
    log_appender& operator=(const log_appender&) = delete;
    log_appender(log_appender&&) noexcept = default;
    log_appender& operator=(log_appender&&) noexcept = default;

    [[deprecated("Use 'for_each_ref()' in record_batch_reader and not "
                 "'consume()' since we don't need to take ownership")]]
    // Alert users (clang-format workaround)
    ss::future<ss::stop_iteration> operator()(model::record_batch&& b) {
        return ss::do_with(std::move(b), [this](model::record_batch& b) {
            return _impl->operator()(b);
        });
    }

    ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
        return _impl->operator()(b);
    }

    ss::future<append_result> end_of_stream() { return _impl->end_of_stream(); }

private:
    std::unique_ptr<impl> _impl;
};

} // namespace storage
