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
      : _logger(logger)
      , _prefix(std::move(prefix)) {}

    template<typename... Args>
    void log(ss::log_level lvl, const char* format, Args&&... args) const {
        if (_logger.is_enabled(lvl)) {
            auto line_fmt = ss::sstring("{} - ") + format;
            _logger.log(
              lvl, line_fmt.c_str(), _prefix, std::forward<Args>(args)...);
        }
    }

    template<typename... Args>
    void error(const char* format, Args&&... args) const {
        log(ss::log_level::error, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void warn(const char* format, Args&&... args) const {
        log(ss::log_level::warn, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void info(const char* format, Args&&... args) const {
        log(ss::log_level::info, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void debug(const char* format, Args&&... args) const {
        log(ss::log_level::debug, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void trace(const char* format, Args&&... args) const {
        log(ss::log_level::trace, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    ss::sstring format(const char* format, Args&&... args) const {
        auto line_fmt = ss::sstring("{} - ") + format;
        return ssx::sformat(
          fmt::runtime(fmt::string_view(line_fmt.begin(), line_fmt.length())),
          _prefix,
          std::forward<Args>(args)...);
    }

    const ss::logger& logger() const { return _logger; }

private:
    ss::logger& _logger;
    ss::sstring _prefix;
};
