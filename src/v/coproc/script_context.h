/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/errc.h"
#include "coproc/exception.h"
#include "coproc/script_context_router.h"
#include "coproc/shared_script_resources.h"
#include "coproc/supervisor.h"
#include "coproc/types.h"
#include "random/simple_time_jitter.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>

namespace coproc {

/// Raised when futures enqueued by an async update were failed to be
/// fufilled due to shutdown before the event occurring
class wait_future_stranded final : public exception {
    using exception::exception;
};

/**
 * The script_context is the smallest schedulable unit in the coprocessor
 * framework. One context is created per registered coprocessor script,
 * representing one fiber.
 *
 * Important to note is the level of concurrency provided. Within a
 * script_context there is one fiber for which scheduled asynchronous work is
 * performed in sync, meaning that for each read -> send -> write that occurs
 * within the run loop, those actions will occur in order.
 *
 * Since each script_context has one of these fibers of its own, no one context
 * will wait for work to be finished by another in order to continue making
 * progress. They all operate independently of eachother.
 */
class script_context {
public:
    /**
     * class constructor
     * @param script_id Uniquely identifyable id
     * @param ctx Shared state, shared across all script_contexts on a shard
     * @param ntp_ctxs Map of interested ntps, strongly retained by 'this'
     **/
    explicit script_context(
      script_id, shared_script_resources&, routes_t&&) noexcept;

    script_context(const script_context&) = delete;
    script_context(script_context&&) = delete;
    script_context& operator=(script_context&&) = delete;
    script_context& operator=(const script_context&) = delete;

    ~script_context() noexcept = default;

    /// Startups up a single managed fiber responsible for maintaining the pace
    /// of the run loop.
    /// Returns a future which returns when the script fiber
    /// has completed or throws \ref script_failed_exception
    ss::future<> start();

    /**
     * Stop the single managed fiber started by 'start()'.
     * @returns a future that resolves when the fiber stops and all resources
     * held by this context are relinquished
     */
    ss::future<> shutdown();

    /// Query for particular route
    inline ss::lw_shared_ptr<coproc::source>
    get_route(const model::ntp& ntp) const {
        auto f = _routes.find(ntp);
        return f != _routes.end() ? f->second : nullptr;
    }

    /// Returns copy of active routes
    routes_t get_routes() const { return _routes; }

    /// Clean-up associated resources with removed output
    ///
    /// This is to be invoked after a materialized topic is deleted
    ss::future<errc>
    remove_output(const model::ntp& source, const model::ntp& materialized);

    /// Returns true if fiber is up to date with all of its inputs
    ///
    /// This is when the stored offsets currently match the input logs dirty
    /// offsets for respective tracked partitions
    bool is_up_to_date() const;

    /// Start ingestion on the interested ntp.
    ///
    /// Useful for the cases where an input partition was moved or updated.
    ss::future<errc> start_processing_ntp(const model::ntp&, read_context&&);

    /// Stop ingestion on the interested ntp.
    ///
    /// Useful for the cases where an input partition is to be moved or updated.
    ss::future<errc> stop_processing_ntp(const model::ntp&);

private:
    ss::future<> do_execute();

    ss::future<> process_reply(process_batch_reply);
    void notify_waiters();

    ss::future<ss::stop_iteration> process_send_write(rpc::transport*);

private:
    /// State to track in-progress ntp modifications
    struct update {
        std::vector<ss::promise<errc>> ps;
        virtual ~update() = default;
        virtual errc handle(const model::ntp&, routes_t&) = 0;
    };

    struct insert_update final : public update {
        /// New input topic is inserted after initial start
        read_context rctx;
        explicit insert_update(read_context) noexcept;
        errc handle(const model::ntp&, routes_t&) final;
    };

    struct remove_update final : public update {
        /// Existing input topic is removed after initial start
        errc handle(const model::ntp&, routes_t&) final;
    };

    struct output_remove_update final : public update {
        /// Materialized topic (output topic) removed
        model::ntp source;
        explicit output_remove_update(model::ntp) noexcept;
        errc handle(const model::ntp&, routes_t&) final;
    };

private:
    /// Killswitch for in-process reads
    ss::abort_source _abort_source;

    /// Manages async fiber that begins when calling 'start()'
    ss::gate _gate;

    /// Allows the ability to asynchronously remove items from the router map
    absl::node_hash_map<model::ntp, std::unique_ptr<update>> _updates;

    // Reference to resources shared across all script_contexts on 'this'
    // shard
    shared_script_resources& _resources;

    /// Collection representing all inputs and outputs for this coprocessor.
    /// Offsets per materialized topic are tracked and input is only incremented
    /// when all outputs are up to date.
    routes_t _routes;

    /// Uniquely identifying script id. Generated by coproc engine
    script_id _id;
};
} // namespace coproc
