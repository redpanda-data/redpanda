// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/dl_stm.h"
#include "cloud_topics/dl_stm/dl_stm_factory.h"
#include "cloud_topics/logger.h"
#include "raft/tests/raft_fixture.h"

namespace ct = experimental::cloud_topics;

class dl_stm_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    ss::future<> start() {
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;

            experimental::cloud_topics::dl_stm_factory stm_factory;
            stm_factory.create(builder, &*node->raft());

            vlog(ct::cd_log.info, "Starting node {}", id);

            co_await node->start(std::move(builder));

            stm_by_vnode[node->get_vnode()]
              = node->raft()->stm_manager()->get<ct::dl_stm>();
        }
    }

    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::dl_stm>> stm_by_vnode;
};

TEST_F_CORO(dl_stm_fixture, test_basic) { co_await start(); }
