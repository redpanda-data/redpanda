#pragma once

#include "model/metadata.h"
#include "raft/types.h"
#include "seastarx.h"

#include <seastar/util/log.hh>

namespace raft {
extern ss::logger raftlog;

class raft_ctx_log {
public:
    raft_ctx_log(model::node_id n, raft::group_id gr)
      : _node_id(n)
      , _group_id(gr) {}

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
            auto line_fmt = fmt::format(
              "[n: {}, gr: {}] {}", _node_id, _group_id, format);

            raftlog.log(lvl, line_fmt.c_str(), std::forward<Args>(args)...);
        }
    }

private:
    model::node_id _node_id;
    raft::group_id _group_id;
};

} // namespace raft
