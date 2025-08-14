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

#include "container/chunked_vector.h"
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
