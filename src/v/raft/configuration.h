#pragma once

#include "redpanda/config/configuration.h"

#include <cstring>

namespace raft {
struct configuration {
    explicit configuration(config::configuration& cfg)
      : _cfg(cfg) {
    }
    model::node_id id() const {
        return model::node_id(_cfg.get().node_id());
    }
    int16_t min_version() const {
        return _cfg.get().min_version();
    }
    int16_t max_version() const {
        return _cfg.get().max_version();
    }

private:
    config::conf_ref _cfg;
};
} // namespace raft
