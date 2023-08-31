/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/base_property.h"
#include "config/configuration.h"

// Scoped wrapper around config::shard_local_cfg() that tracks get() calls and,
// upon destructing, resets any properties that were potentially mutated.
class scoped_config {
public:
    ~scoped_config() {
        for (auto& p : _properties_to_reset) {
            config::shard_local_cfg().get(p).reset();
        }
    }

    config::base_property& get(std::string_view name) {
        _properties_to_reset.emplace_back(name);
        return config::shard_local_cfg().get(name);
    }

private:
    std::list<ss::sstring> _properties_to_reset;
};
