/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/core/pipeline_stage.h"

#include "base/vassert.h"

namespace experimental::cloud_topics::core {

pipeline_stage_container::pipeline_stage_container(size_t max_pipeline_stages) {
    _stages.reserve(max_pipeline_stages);
    for (size_t i = 0; i < max_pipeline_stages; i++) {
        _stages.push_back(pipeline_stage_id(static_cast<int>(i)));
    }
}

pipeline_stage pipeline_stage_container::next_stage(pipeline_stage old) const {
    if (old == unassigned_pipeline_stage) {
        return first_stage();
    }
    auto old_ix = old()->get_numeric_id();
    // Check that the pipeline stage belongs to the collection
    vassert(
      &_stages.at(old_ix) == old(),
      "pipeline_stage belongs to another collection");

    auto next_ix = old_ix + 1;
    // Check that we have next stage
    vassert(
      static_cast<size_t>(next_ix) < _stages.size(),
      "Pipeline stage {} is not registered",
      next_ix);
    return pipeline_stage(&_stages.at(next_ix));
}

pipeline_stage pipeline_stage_container::first_stage() const {
    vassert(!_stages.empty(), "No pipeline stages registered");
    return pipeline_stage(&_stages.front());
}

pipeline_stage pipeline_stage_container::register_pipeline_stage() noexcept {
    vassert(!_stages.empty(), "No pipeline stages registered");
    return pipeline_stage(&_stages.front());
}

} // namespace experimental::cloud_topics::core

auto fmt::formatter<experimental::cloud_topics::core::pipeline_stage>::format(
  const experimental::cloud_topics::core::pipeline_stage& o,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    if (o == experimental::cloud_topics::core::unassigned_pipeline_stage) {
        return formatter<std::string_view>::format(
          fmt::format("pipeline_stage{{unassigned}}"), ctx);
    }
    return formatter<std::string_view>::format(
      fmt::format("pipeline_stage{{id:{}}}", o()->get_numeric_id()), ctx);
}

std::ostream& operator<<(
  std::ostream& o, experimental::cloud_topics::core::pipeline_stage stage) {
    fmt::print(o, "{}", stage);
    return o;
}
