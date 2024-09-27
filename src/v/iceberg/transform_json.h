// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "iceberg/transform.h"

#include <seastar/core/sstring.hh>

namespace iceberg {

// NOTE: while there are no complex JSON types here, the transforms are
// expected to be serialized as a part of JSON files (e.g. table metadata).
ss::sstring transform_to_str(const transform&);
transform transform_from_str(std::string_view);

} // namespace iceberg
