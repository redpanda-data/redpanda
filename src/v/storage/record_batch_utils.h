/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "model/record.h"

namespace storage {

// Serializes the header to a buffer suitable to be stored on disk. Note that
// this is different from serde::envelope serialization in that only the exact
// batch header fields are serialized, with no additional bytes for size,
// versions, etc.
iobuf batch_header_to_disk_iobuf(const model::record_batch_header& h);
model::record_batch_header batch_header_from_disk_iobuf(iobuf b);

} // namespace storage
