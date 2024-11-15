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

#include "base/outcome.h"
#include "model/fundamental.h"

namespace datalake::coordinator {

struct commit_offset_metadata {
    // Offset of the control topic partition up to which the pending data files
    // are committed to an external catalog.
    model::offset offset;

    // TODO: version? topic revision id? partition id? cluster uuid?
};

using parse_offset_error = named_type<ss::sstring, struct parse_tag>;
checked<commit_offset_metadata, parse_offset_error>
parse_commit_offset_json(std::string_view s);

std::string to_json_str(const commit_offset_metadata&);

} // namespace datalake::coordinator
