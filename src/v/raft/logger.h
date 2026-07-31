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

#include "base/seastarx.h"
#include "model/fundamental.h"
#include "raft/fundamental.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

namespace raft {
extern ss::logger raftlog;

class ctx_log {
public:
    ctx_log(raft::group_id gr, model::ntp ntp)
      : _group_id(gr)
      , _ntp(std::move(ntp)) {}

    template<typename... Args>
    void error(ss::logger::format_info_t<Args...> format, Args&&... args) {
        log(
          ss::log_level::error, std::move(format), std::forward<Args>(args)...);
    }
    template<typename... Args>
    void warn(ss::logger::format_info_t<Args...> format, Args&&... args) {
        log(
          ss::log_level::warn, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void info(ss::logger::format_info_t<Args...> format, Args&&... args) {
        log(
          ss::log_level::info, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void debug(ss::logger::format_info_t<Args...> format, Args&&... args) {
        log(
          ss::log_level::debug, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void trace(ss::logger::format_info_t<Args...> format, Args&&... args) {
        log(
          ss::log_level::trace, std::move(format), std::forward<Args>(args)...);
    }

    template<typename... Args>
    void log(
      ss::log_level lvl,
      ss::logger::format_info_t<Args...> format,
      Args&&... args) {
        if (raftlog.is_enabled(lvl)) {
            ss::logger::lambda_log_writer writer(
              [&](ss::internal::log_buf::inserter_iterator it) {
                  it = fmt::format_to(
                    it, "[group_id:{}, {}] ", _group_id, _ntp);
                  return fmt::format_to(
                    it,
                    fmt::runtime(format.format),
                    std::forward<Args>(args)...);
              });
            raftlog.log(lvl, writer);
        }
    }

private:
    raft::group_id _group_id;
    model::ntp _ntp;
};

} // namespace raft
