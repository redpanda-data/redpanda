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

#include "cluster/metadata_dissemination_types.h"
#include "compat/generator.h"
#include "container/fragmented_vector.h"
#include "model/metadata.h"
#include "model/tests/randoms.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <seastar/core/chunked_fifo.hh>

namespace compat {

template<>
struct instance_generator<cluster::update_leadership_request_v2> {
    static cluster::update_leadership_request_v2 random() {
        chunked_vector<cluster::ntp_leader_revision> values;
        values.emplace_back(
          model::random_ntp(),
          tests::random_named_int<model::term_id>(),
          tests::random_named_int<model::node_id>(),
          tests::random_named_int<model::revision_id>());

        return cluster::update_leadership_request_v2(std::move(values));
    }

    static std::vector<cluster::update_leadership_request_v2> limits() {
        return {};
    }
};

EMPTY_COMPAT_GENERATOR(cluster::update_leadership_reply);

EMPTY_COMPAT_GENERATOR(cluster::get_leadership_request);

template<>
struct instance_generator<cluster::get_leadership_reply> {
    static cluster::get_leadership_reply random() {
        fragmented_vector<cluster::ntp_leader> leaders;
        leaders.emplace_back(
          model::random_ntp(),
          tests::random_named_int<model::term_id>(),
          tests::random_named_int<model::node_id>());
        return cluster::get_leadership_reply(
          std::move(leaders),
          random_generators::random_choice(
            {cluster::get_leadership_reply::is_success::yes,
             cluster::get_leadership_reply::is_success::no}));
    }

    static std::vector<cluster::get_leadership_reply> limits() { return {}; }
};

} // namespace compat
