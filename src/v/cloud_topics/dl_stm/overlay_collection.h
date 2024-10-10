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

#include "cloud_topics/dl_stm/commands.h"
#include "cloud_topics/dl_stm/sorted_run.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

/// Container class for the dl_stm
/// which is supposed to store sorted list of overlays.
/// It should be easy to use columnar compression in the
/// future and also quickly find sorted runs. The container
/// uses patience sort algorithm to maintain order of overlay
/// values.
///
/// Deletion, replacement and truncation are not implemented yet.
class overlay_collection
  : public serde::envelope<
      overlay_collection,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr size_t max_runs = 8;

public:
    bool operator==(const overlay_collection&) const noexcept = default;

    void append(const dl_overlay& o) noexcept;

    /// Find best overlay to fetch the offset from.
    ///
    /// The overlay should contain the offset 'o' or any
    /// offset larger than 'o'. If the caller needs exact
    /// match it has to validate the returned output.
    std::optional<dl_overlay> lower_bound(
      kafka::offset o) const noexcept; // TODO: add overload for the timequery

    auto serde_fields() { return std::tie(_runs); }

    void compact() noexcept;

    void do_append(const dl_overlay& o) noexcept;

private:
    std::deque<sorted_run_t> _runs;
};
} // namespace experimental::cloud_topics
