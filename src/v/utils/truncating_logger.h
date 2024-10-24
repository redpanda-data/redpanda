/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"
#include "ssx/sformat.h"

#include <seastar/util/log-impl.hh>
#include <seastar/util/log.hh>

#include <iostream>

/// Logger that supports proactive truncation of formatted output at the level
/// of seastar::logger::log_writer
class truncating_logger {
    static constexpr std::string_view trunc_msg_fmt
      = "... log line too large; dropped {}B";
    static constexpr size_t max_max_line_bytes = 1048576;
    static constexpr size_t max_trunc_msg_len = trunc_msg_fmt.size() + (20 - 2);

public:
    truncating_logger(ss::logger& logger, size_t max_line_bytes)
      : _logger(logger) {
        vassert(
          max_line_bytes <= max_max_line_bytes,
          "max_line_bytes {} too large; should be <= {}",
          max_line_bytes,
          max_max_line_bytes);
        vassert(
          max_line_bytes > max_trunc_msg_len,
          "max_line_bytes {} too small; should be > {}",
          max_line_bytes,
          max_trunc_msg_len);

        _max_line_bytes = max_line_bytes - max_trunc_msg_len;
    }

    template<typename... Args>
    void
    log(ss::log_level lvl, fmt::format_string<Args...> format, Args&&... args)
      const {
        if (_logger.is_enabled(lvl)) {
            ss::logger::lambda_log_writer writer(
              [&](ss::internal::log_buf::inserter_iterator it) {
                  auto res = fmt::format_to_n(
                    it, _max_line_bytes, format, std::forward<Args>(args)...);
                  if (res.size > _max_line_bytes) {
                      return fmt::format_to(
                        res.out, trunc_msg_fmt, res.size - _max_line_bytes);
                  }
                  return res.out;
              });
            _logger.log(lvl, writer);
        }
    }

    template<typename... Args>
    void error(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::error, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void warn(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::warn, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void info(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::info, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void debug(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::debug, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void trace(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::trace, format, std::forward<Args>(args)...);
    }

private:
    ss::logger& _logger;
    size_t _max_line_bytes;
};
