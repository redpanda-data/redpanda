#pragma once

#include "filesystem/wal_writer_utils.h"
#include "redpanda/config/configuration.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <smf/human_bytes.h>

struct wal_opts {
    explicit wal_opts(config::configuration& cfg)
      : _cfg(cfg){

      };

    const sstring& directory() const {
        return _cfg.get().data_directory();
    }
    timer<>::duration writer_flush_period() const {
        return std::chrono::milliseconds(_cfg.get().writer_flush_period_ms());
    }
    timer<>::duration max_retention_period() const {
        return std::chrono::hours(_cfg.get().max_retention_period_hours());
    }
    int64_t max_retention_size() const {
        return _cfg.get().max_retention_size();
    }
    int32_t max_bytes_in_writer_cache() const {
        return _cfg.get().max_bytes_in_writer_cache();
    }
    int64_t max_log_segment_size() const {
        return align_up(
          _cfg.get().log_segment_size_bytes(), system_page_size());
    }

private:
    config::conf_ref _cfg;
};

std::ostream& operator<<(std::ostream& o, const wal_opts& opts);
