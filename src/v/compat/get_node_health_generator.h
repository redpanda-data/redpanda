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

#include "cluster/tests/randoms.h"
#include "cluster/types.h"
#include "compat/generator.h"
#include "model/timeout_clock.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::get_node_health_request> {
    static cluster::get_node_health_request random() {
        return cluster::get_node_health_request{};
    }
    static std::vector<cluster::get_node_health_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::get_node_health_reply> {
    static cluster::get_node_health_reply random() {
        return {
          .error = instance_generator<cluster::errc>::random(),
          .report = tests::random_optional([] {
              return cluster::node_health_report_serde{
                cluster::random_node_health_report()};
          }),
        };
    }
    static std::vector<cluster::get_node_health_reply> limits() { return {}; }
};

} // namespace compat
