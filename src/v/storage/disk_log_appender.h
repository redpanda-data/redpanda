#pragma once
#include "storage/log_appender.h"

namespace storage {

class disk_log_impl;
class log_segment_appender;

ss::future<> write(log_segment_appender&, const model::record_batch&);

class disk_log_appender final : public log_appender::impl {
public:
    disk_log_appender(
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
