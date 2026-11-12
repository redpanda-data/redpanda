/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/fundamental.h"

#include <absl/container/flat_hash_map.h>

#include <optional>

namespace storage {

/// Per-shard registry of the tiered-storage staging uploader's durable
/// cursor per ntp. Local eviction for cloud-engine (ctp) partitions
/// consults it so the local log is never trimmed past what the staging
/// tier has captured: trimming further would make the not-yet-staged range
/// unreadable, force the uploader's skip-on-error path, and punch permanent
/// holes into staged-tail restore coverage.
class staging_floor {
public:
    static staging_floor& instance() {
        static thread_local staging_floor f;
        return f;
    }

    void set(const model::ntp& ntp, model::offset staged_up_to) {
        _floors[ntp] = staged_up_to;
    }

    void erase(const model::ntp& ntp) { _floors.erase(ntp); }

    void clear() { _floors.clear(); }

    std::optional<model::offset> get(const model::ntp& ntp) const {
        auto it = _floors.find(ntp);
        if (it == _floors.end()) {
            return std::nullopt;
        }
        return it->second;
    }

private:
    absl::flat_hash_map<model::ntp, model::offset> _floors;
};

} // namespace storage
