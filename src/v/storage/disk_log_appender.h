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

#include "storage/log_appender.h"

#include <seastar/core/rwlock.hh>

namespace storage {

class disk_log_impl;

class disk_log_appender final : public log_appender::impl {
public:
    disk_log_appender(
      disk_log_impl& log,
      log_append_config config,
      log_clock::time_point append_time,
      model::offset next_offset) noexcept;

    ss::future<ss::stop_iteration> operator()(model::record_batch&) final;

    /*
     * if no batches are appended then ret.last_offset will be equal to the
     * default value of model::offset(), otherwise it is the offset of the last
     * record of the last batch appended.
     */
    ss::future<append_result> end_of_stream() final;

private:
    bool segment_is_appendable(model::term_id) const;
    void release_lock();
    ss::future<ss::stop_iteration>
    append_batch_to_segment(const model::record_batch&);
    ss::future<> initialize();

    disk_log_impl& _log;
    log_append_config _config;
    log_clock::time_point _append_time;
    model::offset _idx;

    ss::lw_shared_ptr<segment> _seg;
    std::optional<ss::rwlock::holder> _seg_lock;
    size_t _bytes_left_in_segment{0};

    // below are just copied from append
    model::offset _base_offset;
    model::offset _last_offset;
    model::term_id _last_term;
    size_t _byte_size{0};

    friend std::ostream& operator<<(std::ostream&, const disk_log_appender&);
};

} // namespace storage
