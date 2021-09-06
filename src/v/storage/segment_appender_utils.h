/*
 * Copyright 2020 Vectorized, Inc.
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
#include "storage/segment_appender.h"

namespace storage {

iobuf disk_header_to_iobuf(const model::record_batch_header& h);

ss::future<>
write(segment_appender& appender, const model::record_batch& batch);

} // namespace storage
