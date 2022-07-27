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

#include "compat/generator.h"
#include "raft/types.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<raft::vnode> {
    static raft::vnode random() {
        return {
          tests::random_named_int<model::node_id>(),
          tests::random_named_int<model::revision_id>()};
    }

    static std::vector<raft::vnode> limits() {
        return {
          {model::node_id::min(), model::revision_id::min()},
          {model::node_id::max(), model::revision_id::max()},
          {model::node_id{0}, model::revision_id{0}},
        };
    }
};

template<>
struct instance_generator<raft::timeout_now_request> {
    static raft::timeout_now_request random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .node_id = instance_generator<raft::vnode>::random(),
          .group = tests::random_named_int<raft::group_id>(),
          .term = tests::random_named_int<model::term_id>(),
        };
    }

    static std::vector<raft::timeout_now_request> limits() {
        return {
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .node_id = instance_generator<raft::vnode>::random(),
            .group = raft::group_id::min(),
            .term = model::term_id::min(),
          },
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .node_id = instance_generator<raft::vnode>::random(),
            .group = raft::group_id::max(),
            .term = model::term_id::max(),
          },
        };
    }
};

template<>
struct instance_generator<raft::timeout_now_reply> {
    static raft::timeout_now_reply random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .term = tests::random_named_int<model::term_id>(),
          .result = random_generators::random_choice(
            {raft::timeout_now_reply::status::success,
             raft::timeout_now_reply::status::failure}),
        };
    }

    static std::vector<raft::timeout_now_reply> limits() {
        return {
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .term = model::term_id::min(),
            .result = raft::timeout_now_reply::status::success,
          },
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .term = model::term_id::max(),
            .result = raft::timeout_now_reply::status::failure,
          },
        };
    }
};

} // namespace compat
