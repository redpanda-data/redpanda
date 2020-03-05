#pragma once

#include "storage/log_appender.h"
#include "storage/segment.h"

#include <seastar/core/rwlock.hh>

namespace storage {

class disk_log_impl;

class disk_log_appender final : public log_appender::impl {
public:
    disk_log_appender(
      disk_log_impl& log,
      log_append_config config,
      log_clock::time_point append_time,
      // expect the *next* valid (inclusive) offset
      model::offset offset) noexcept;

    ss::future<> initialize() final { return ss::make_ready_future<>(); }

    ss::future<ss::stop_iteration> operator()(model::record_batch&&) final;

    ss::future<append_result> end_of_stream() final;

private:
    disk_log_impl& _log;
    log_append_config _config;
    log_clock::time_point _append_time;
    model::offset _idx;

    ss::lw_shared_ptr<segment> _cache;
    std::optional<ss::rwlock::holder> _cache_lock;
    size_t _bytes_left_in_cache_segment{0};

    // below are just copied from append
    model::offset _base_offset;
    model::offset _last_offset;
    model::term_id _last_term;
    size_t _byte_size{0};
};

} // namespace storage
