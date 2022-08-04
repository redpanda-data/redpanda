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

#include "cluster/types.h"
#include "compat/cluster_generator.h"
#include "compat/generator.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::create_acls_reply> {
    static cluster::create_acls_reply random() {
        using gen = instance_generator<cluster::errc>;
        auto f = []() { return gen::random(); };
        return {
          .results = tests::random_vector(std::move(f)),
        };
    }

    static std::vector<cluster::create_acls_reply> limits() { return {{}}; }
};

}; // namespace compat