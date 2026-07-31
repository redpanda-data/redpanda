/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "ssx/sformat.h"

#include <seastar/util/log.hh>

/// Useful when all log messages for some component must be prefixed by
/// some kind of context (e.g. ntp).
class prefix_logger {
public:
    explicit prefix_logger(ss::logger& logger, ss::sstring prefix)
      : _logger(&logger)
      , _prefix(std::move(prefix)) {}

    template<typename... Args>
    void log(
      ss::log_level lvl,
      ss::logger::format_info_t<Args...> format,
      Args&&... args) const {
        if (_logger->is_enabled(lvl)) {
            ss::logger::lambda_log_writer writer(
              [&](ss::internal::log_buf::inserter_iterator it) {
                  it = fmt::format_to(it, "{} - ", _prefix);
                  return fmt::format_to(
                    it,
                    fmt::runtime(format.format),
                    std::forward<Args>(args)...);
              });
            _logger->log(lvl, writer);
        }
    }

    template<typename... Args>
    void
    error(ss::logger::format_info_t<Args...> format, Args&&... args) const {
        log(
          ss::log_level::error, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void warn(ss::logger::format_info_t<Args...> format, Args&&... args) const {
        log(
          ss::log_level::warn, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void info(ss::logger::format_info_t<Args...> format, Args&&... args) const {
        log(
          ss::log_level::info, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void
    debug(ss::logger::format_info_t<Args...> format, Args&&... args) const {
        log(
          ss::log_level::debug, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void
    trace(ss::logger::format_info_t<Args...> format, Args&&... args) const {
        log(
          ss::log_level::trace, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    ss::sstring format(const char* format, Args&&... args) const {
        auto line_fmt = ss::sstring("{} - ") + format;
        return ssx::sformat(
          fmt::runtime(fmt::string_view(line_fmt.begin(), line_fmt.length())),
          _prefix,
          std::forward<Args>(args)...);
    }

    const ss::logger& logger() const { return *_logger; }

private:
    ss::logger* _logger;
    ss::sstring _prefix;
};
