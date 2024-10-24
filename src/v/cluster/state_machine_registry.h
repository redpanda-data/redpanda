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
#include "raft/consensus.h"
#include "raft/fwd.h"
#include "raft/state_machine_manager.h"
#include "storage/ntp_config.h"

namespace cluster {

/**
 * State machine factory is a class used by registry to create stm instance if
 * it is required for a given Raft group. The factory has two main
 * responsibilities expressed by its interface method.
 * Firstly it has to answer query if given stm is required for particular raft
 * group. Secondly it must created an stm instance using a builder which is
 * passed into `create` method. State machine factory must encapsulate all
 * dependencies required to create state machine
 */
struct state_machine_factory {
    /**
     * Must return true if STM should be created for a partition underlaid by
     * passed raft group
     */
    virtual bool is_applicable_for(const storage::ntp_config&) const = 0;

    /**
     * A method must call builder interface to create STM instance.
     */
    virtual void create(raft::state_machine_manager_builder&, raft::consensus*)
      = 0;

    virtual ~state_machine_factory() = default;
};

/**
 * State machine registry is a class holding all registered state machines
 * factories. Registry is used by partition_manger whenever a new partition
 * instance is created. Registry builds all state machines that are required for
 * a give partition instance.
 */
class state_machine_registry {
public:
    template<typename T, typename... Args>
    void register_factory(Args&&... args) {
        _stm_factories.push_back(
          std::make_unique<T>(std::forward<Args>(args)...));
    }

    raft::state_machine_manager_builder
    make_builder_for(raft::consensus* raft) {
        raft::state_machine_manager_builder builder;
        for (auto& factory : _stm_factories) {
            if (factory->is_applicable_for(raft->log_config())) {
                factory->create(builder, raft);
            }
        }
        return builder;
    }

private:
    std::vector<std::unique_ptr<state_machine_factory>> _stm_factories;
};
} // namespace cluster
