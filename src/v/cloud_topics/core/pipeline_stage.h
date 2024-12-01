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

class pipeline_stage_id;

/// Processing stage id
using pipeline_stage
  = named_type<const pipeline_stage_id*, struct _pipeline_stage_tag>;

/// Request is just added to the pipeline, no stage is assigned
inline constexpr auto unassigned_pipeline_stage = pipeline_stage{nullptr};

} // namespace experimental::cloud_topics::core
