/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/errc.h"
#include "cluster/id_allocator.h"
#include "cluster/types.h"
#include "compat/generator.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <chrono>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace compat {

template<>
struct instance_generator<cluster::allocate_id_request> {
    static cluster::allocate_id_request random() {
        return cluster::allocate_id_request{
          tests::random_duration<model::timeout_clock::duration>()};
    }

    static std::vector<cluster::allocate_id_request> limits() {
        return {
          cluster::allocate_id_request{model::timeout_clock::duration(0)},
          cluster::allocate_id_request{min_duration()},
          cluster::allocate_id_request{max_duration()},
        };
    }
};

template<>
struct instance_generator<cluster::allocate_id_reply> {
    using errc_type = typename std::underlying_type_t<cluster::errc>;
    static constexpr auto int64_min = std::numeric_limits<int64_t>::min();
    static constexpr auto int64_max = std::numeric_limits<int64_t>::min();
    static constexpr auto errc_min = static_cast<errc_type>(
      cluster::errc::success);
    static constexpr auto errc_max = static_cast<errc_type>(
      cluster::errc::unknown_update_interruption_error);

    static cluster::allocate_id_reply random() {
        const auto errc_rand = random_generators::get_int<errc_type>(
          errc_min, errc_max);
        return {
          random_generators::get_int<int64_t>(int64_min, int64_max),
          cluster::errc(errc_rand)};
    }

    static std::vector<cluster::allocate_id_reply> limits() {
        return {
          {int64_min, cluster::errc::success},
          {int64_max, cluster::errc::success},
          {int64_min, cluster::errc::unknown_update_interruption_error},
          {int64_max, cluster::errc::unknown_update_interruption_error},
        };
    }
};

} // namespace compat
