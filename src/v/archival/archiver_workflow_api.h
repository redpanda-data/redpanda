/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include <seastar/core/future.hh>

namespace ss = seastar;

namespace archival {

/// Represents a per-NTP workflow like
/// segment upload or housekeeping. The workflow is active
/// during single term. After the start it is active until the
/// error occurs or the term changes.
/// The workflow can be interrupted.
class archiver_workflow_api {
public:
    archiver_workflow_api() = default;
    virtual ~archiver_workflow_api() = default;
    archiver_workflow_api(const archiver_workflow_api&) = default;
    archiver_workflow_api(archiver_workflow_api&&) noexcept = default;
    archiver_workflow_api& operator=(const archiver_workflow_api&) = default;
    archiver_workflow_api& operator=(archiver_workflow_api&&) noexcept
      = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;
};
} // namespace archival
