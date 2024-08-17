// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/transform.h"
#include "iceberg/values.h"

namespace iceberg {

// Transforms the given value to its appropriate Iceberg value based on the
// input transform.
//
// TODO: this is only implemented for the hourly transform on timestamp values!
// This will throw if used for anything else!
value apply_transform(const value&, const transform&);

} // namespace iceberg
