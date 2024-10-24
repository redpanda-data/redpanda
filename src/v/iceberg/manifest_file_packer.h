// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "iceberg/manifest_list.h"

namespace iceberg {

class manifest_packer {
public:
    using bin_t = chunked_vector<manifest_file>;
    // Simple bin-packer for manifest files. Bins are packed back to front.
    // This means that for a list of files that is repeatedly prepended to and
    // packed, the resulting front bin will be well below the target size.
    //
    // This is a useful property, e.g. to opt out of merging the files in the
    // front bin when it is too small, to let more manifest files be prepended.
    static chunked_vector<bin_t>
    pack(size_t target_size_bytes, chunked_vector<manifest_file> files);
};

} // namespace iceberg
