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
#include "cluster/types.h"
#include "compat/cluster_generator.h"
#include "compat/generator.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::create_acls_request> {
    static cluster::create_acls_request random() {
        cluster::create_acls_cmd_data data;
        auto rand_bindings = tests::random_vector(
          [] { return tests::random_acl_binding(); });
        data.bindings.insert(
          data.bindings.end(), rand_bindings.begin(), rand_bindings.end());
        return {data, tests::random_duration<model::timeout_clock::duration>()};
    }

    static std::vector<cluster::create_acls_request> limits() { return {{}}; }
};

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

template<>
struct instance_generator<cluster::delete_acls_reply> {
    static cluster::delete_acls_reply random() {
        auto generator = []() {
            auto errc = instance_generator<cluster::errc>::random();
            auto acl_bindings = tests::random_vector(
              []() { return tests::random_acl_binding(); });
            return cluster::delete_acls_result{
              .error = errc, .bindings = acl_bindings};
        };
        return {
          .results = tests::random_vector(generator),
        };
    }

    static std::vector<cluster::delete_acls_reply> limits() { return {{}}; }
};

template<>
struct instance_generator<cluster::delete_acls_request> {
    static cluster::delete_acls_request random() {
        cluster::delete_acls_cmd_data data;
        auto rand_filters = tests::random_vector(
          [] { return tests::random_acl_binding_filter(); });
        data.filters.insert(
          data.filters.end(), rand_filters.begin(), rand_filters.end());
        return {data, tests::random_duration<model::timeout_clock::duration>()};
    }

    static std::vector<cluster::delete_acls_request> limits() { return {{}}; }
};

}; // namespace compat