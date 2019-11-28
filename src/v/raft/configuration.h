#pragma once

#include "raft/types.h"

namespace raft {
struct configuration {
    configuration(model::node_id i, int16_t min, int16_t max)
      : id(std::move(i))
      , min_version(min)
      , max_version(max) {}
    configuration(const configuration& o) {
        id = o.id();
        min_version = o.min_version;
        max_version = o.max_version;
    }
    model::node_id id;
    int16_t min_version;
    int16_t max_version;
};
} // namespace raft
