// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "bytes/bytes.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace iceberg {

bytes value_to_bytes(const value&);
value value_from_bytes(const field_type& type, const bytes&);

} // namespace iceberg
