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

#include "config/types.h"
#include "model/fundamental.h"

namespace model {

inline fips_mode_flag from_config(config::fips_mode_flag f) {
    switch (f) {
    case config::fips_mode_flag::disabled:
        return fips_mode_flag::disabled;
    case config::fips_mode_flag::permissive:
        return fips_mode_flag::permissive;
    case config::fips_mode_flag::enabled:
        return fips_mode_flag::enabled;
    }

    __builtin_unreachable();
}

} // namespace model
