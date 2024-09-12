// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/manifest_entry.h"
#include "iceberg/partition_key_type.h"
#include "iceberg/values.h"

namespace iceberg {

struct_value manifest_entry_to_value(const manifest_entry& entry);
manifest_entry manifest_entry_from_value(struct_value v);

} // namespace iceberg
