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
    void error(const char* format, Args&&... args) {
        log(ss::log_level::error, format, std::forward<Args>(args)...);
    }
    template<typename... Args>
    void warn(const char* format, Args&&... args) {
        log(ss::log_level::warn, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void info(const char* format, Args&&... args) {
        log(ss::log_level::info, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void debug(const char* format, Args&&... args) {
        log(ss::log_level::debug, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void trace(const char* format, Args&&... args) {
        log(ss::log_level::trace, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void log(ss::log_level lvl, const char* format, Args&&... args) {
        if (raftlog.is_enabled(lvl)) {
            auto line_fmt = ss::sstring("[group_id:{}, {}] ") + format;
            raftlog.log(
              lvl,
              line_fmt.c_str(),
              _group_id,
              _ntp,
              std::forward<Args>(args)...);
        }
    }

private:
    raft::group_id _group_id;
    model::ntp _ntp;
};

} // namespace raft
