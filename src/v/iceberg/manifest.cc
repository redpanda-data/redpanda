// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest.h"

namespace iceberg {

manifest_metadata manifest_metadata::copy() const {
    return manifest_metadata{
      .schema = schema.copy(),
      .partition_spec = partition_spec.copy(),
      .format_version = format_version,
      .manifest_content_type = manifest_content_type,
    };
}

manifest manifest::copy() const {
    manifest ret{
      .metadata = metadata.copy(),
    };
    ret.entries.reserve(entries.size());
    for (const auto& e : entries) {
        ret.entries.push_back(e.copy());
    }
    return ret;
}

} // namespace iceberg
