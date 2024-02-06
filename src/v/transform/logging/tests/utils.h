/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "json/document.h"
#include "model/transform.h"

#include <string>

namespace transform::logging::testing {

json::Document parse_json(iobuf resp);
std::string get_message_body(iobuf);

model::transform_name random_transform_name(size_t len = 12);

iobuf random_length_iobuf(size_t data_max);

} // namespace transform::logging::testing
