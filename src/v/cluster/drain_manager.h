/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/fwd.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <chrono>

namespace cluster {

/*
 * The drain manager is responsible for managing the draining of leadership from
 * a node. It is a core building block for implementing node maintenance mode.
 */
class drain_manager : public ss::peering_sharded_service<drain_manager> {
    static constexpr size_t max_parallel_transfers = 25;
    static constexpr std::chrono::duration transfer_throttle
      = std::chrono::seconds(5);

public:
    /*
     * finsihed:     draining has completed
     * errors:       draining finished with errors
     * partitions:   total partitions
     * eligible:     total drain-eligible partitions
     * transferring: total partitions currently transferring
     * failed:       total transfers failed in last batch
     *
     * the optional fields may not be set if draining has been requested, but
     * not yet started. in this case the values are not yet known.
     */
    struct drain_status {
        bool finished{false};
        bool errors{false};
        std::optional<size_t> partitions;
        std::optional<size_t> eligible;
        std::optional<size_t> transferring;
        std::optional<size_t> failed;
    };

    explicit drain_manager(ss::sharded<cluster::partition_manager>&);

    ss::future<> start();
    ss::future<> stop();

    /*
     * Start draining this broker.
     *
     * Invoke this on each core.
     */
    ss::future<> drain();

    /*
     * Restore broker to a non-drain[ing] state.
     *
     * Invoke this on each core.
     */
    ss::future<> restore();

    /*
     * Check the status of the draining process.
     *
     * This performs a global reduction across cores.
     */
    ss::future<std::optional<drain_status>> status();

private:
    ss::future<> task();
    ss::future<> do_drain();
    ss::future<> do_restore();

    ss::sharded<cluster::partition_manager>& _partition_manager;
    std::optional<ss::future<>> _drain;
    bool _draining{false};
    ss::semaphore _sem{0};
    drain_status _status;
    ss::abort_source _abort;
};

} // namespace cluster
