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
#include "model/metadata.h"
#include "model/tests/randoms.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::update_leadership_request> {
    static cluster::update_leadership_request random() {
        return cluster::update_leadership_request({
          cluster::ntp_leader(
            model::random_ntp(),
            tests::random_named_int<model::term_id>(),
            tests::random_named_int<model::node_id>()),
        });
    }

    static std::vector<cluster::update_leadership_request> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::update_leadership_request_v2> {
    static cluster::update_leadership_request_v2 random() {
        return cluster::update_leadership_request_v2({
          cluster::ntp_leader_revision(
            model::random_ntp(),
            tests::random_named_int<model::term_id>(),
            tests::random_named_int<model::node_id>(),
            tests::random_named_int<model::revision_id>()),
        });
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
        return cluster::get_leadership_reply({
          cluster::ntp_leader(
            model::random_ntp(),
            tests::random_named_int<model::term_id>(),
            tests::random_named_int<model::node_id>()),
        });
    }

    static std::vector<cluster::get_leadership_reply> limits() { return {}; }
};

} // namespace compat
