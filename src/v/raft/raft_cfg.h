#pragma once

#include "raft_seed_server.h"
#include "redpanda/config/configuration.h"

#include <cstring>

struct raft_cfg {
    explicit raft_cfg(config::configuration& cfg)
      : _cfg(cfg) {
    }

    const int64_t id() const {
        return _cfg.get().node_id();
    }
    const int16_t min_version() const {
        return _cfg.get().min_version();
    }
    const int16_t max_version() const {
        return _cfg.get().max_version();
    }
    const int32_t seed_server_meta_topic_partitions() const {
        return _cfg.get().seed_server_meta_topic_partitions();
    }
    const std::vector<raft_seed_server>& seeds() const {
        return _cfg.get().seed_servers();
    }

private:
    config::conf_ref _cfg;
};
