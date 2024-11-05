// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

namespace storage {

/// Log manager per-shard storage probe.
class log_manager_probe {
public:
    log_manager_probe() = default;
    log_manager_probe(const log_manager_probe&) = delete;
    log_manager_probe& operator=(const log_manager_probe&) = delete;
    log_manager_probe(log_manager_probe&&) = delete;
    log_manager_probe& operator=(log_manager_probe&&) = delete;
    ~log_manager_probe() = default;

public:
    void setup_metrics();
    void clear_metrics();
};

}; // namespace storage
