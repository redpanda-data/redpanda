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

#include "utils/named_type.h"

namespace experimental::cloud_topics::core {

class pipeline_stage_id {
public:
    explicit pipeline_stage_id(int id)
      : _id(id) {}

    int get_numeric_id() const noexcept { return _id; }

    ~pipeline_stage_id() = default;
    pipeline_stage_id(const pipeline_stage_id& ps) = default;
    pipeline_stage_id(pipeline_stage_id&& ps) = delete;
    pipeline_stage_id& operator=(const pipeline_stage_id& ps) = default;
    pipeline_stage_id& operator=(pipeline_stage_id&& ps) = delete;

private:
    int _id;
};

/// Processing stage id
using pipeline_stage
  = named_type<const pipeline_stage_id*, struct _pipeline_stage_tag>;

/// Request is just added to the pipeline, no stage is assigned
inline constexpr auto unassigned_pipeline_stage = pipeline_stage{nullptr};

/// Manages set of pipeline stages
class pipeline_stage_container {
public:
    explicit pipeline_stage_container(size_t max_stages);
    pipeline_stage next_stage(pipeline_stage old) const;
    pipeline_stage first_stage() const;
    pipeline_stage register_pipeline_stage() noexcept;

private:
    // Pipeline stage counter used to assign ids to stages
    int _next_stage_id_to_alloc{0};

    std::vector<const pipeline_stage_id> _stages;
};

} // namespace experimental::cloud_topics::core

template<>
struct fmt::formatter<experimental::cloud_topics::core::pipeline_stage>
  : fmt::formatter<std::string_view> {
    auto format(
      const experimental::cloud_topics::core::pipeline_stage&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};

std::ostream& operator<<(
  std::ostream& o, experimental::cloud_topics::core::pipeline_stage stage);
