/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/task.h"
#include "schema/registry.h"

namespace cluster_link::schema_registry_sync {

/// Shadows a source Schema Registry into the local (destination) Schema
/// Registry. Runs on the shard leading `_schemas/0`, a cluster-wide singleton.
/// Each run reconciles the source onto the destination; it currently reports
/// source and destination inventory and does not yet import.
///
/// Source failures travel as `source_error` values: an unavailable source
/// parks the link, a per-item failure is counted and skipped. Destination and
/// internal faults throw and become `faulted` via the base task runner.
class mirroring_task : public task {
public:
    static constexpr auto task_name = "Schema Registry Shadowing";

    mirroring_task(
      link* link,
      const model::metadata& link_metadata,
      schema::registry* destination,
      source_reader_factory* source_factory);
    mirroring_task(const mirroring_task&) = delete;
    mirroring_task(mirroring_task&&) = delete;
    mirroring_task& operator=(const mirroring_task&) = delete;
    mirroring_task& operator=(mirroring_task&&) = delete;
    ~mirroring_task() override = default;

    void update_config(const model::metadata& link_metadata) override;

    model::enabled_t is_enabled() const final;

    model::task_status_report get_status_report() const override;

protected:
    ss::future<state_transition> run_impl(ss::abort_source&) override;

    bool should_start_impl(ss::shard_id, ::model::node_id) const final;

    bool should_stop_impl(ss::shard_id, ::model::node_id) const final;

private:
    bool leads_schema_registry_partition() const;

    /// Whether a periodic full scan is due (first run, or the full-sync
    /// interval has elapsed). A config change additionally forces one via
    /// `_config_changed`, consumed in `run_impl`.
    bool should_long_sync() const;

    /// Refreshes destination inventory counters from the local registry in a
    /// single scatter-gather. Throws on internal/destination faults.
    ss::future<> refresh_destination_inventory();

    /// Full source scan: refreshes source inventory counters. Returns the
    /// resulting task state (active, or link_unavailable if the source is
    /// down).
    ss::future<state_transition>
    full_source_sync(ss::abort_source&, model::schema_registry_sync_summary&);

    [[nodiscard]] state_transition make_unavailable(const ss::sstring& reason);
    [[nodiscard]] state_transition make_active();

    model::schema_registry_sync_config _config;
    schema::registry* _destination;
    source_reader_factory* _source_factory;
    std::unique_ptr<source_reader> _reader;
    model::schema_registry_sync_status _status;
    std::optional<ss::lowres_clock::time_point> _last_full_sync;
    // Set by update_config, consumed by run_impl to force a full scan. A flag
    // (rather than mutating _status/_last_full_sync in update_config) avoids
    // racing an in-flight run_impl across its co_await suspension points.
    bool _config_changed{false};
};

class mirroring_task_factory : public task_factory {
public:
    mirroring_task_factory(
      schema::registry* destination, source_reader_factory* source_factory)
      : _destination(destination)
      , _source_factory(source_factory) {}

    std::string_view created_task_name() const noexcept override;

    std::unique_ptr<task> create_task(link* link) override;

private:
    schema::registry* _destination;
    source_reader_factory* _source_factory;
};

} // namespace cluster_link::schema_registry_sync
