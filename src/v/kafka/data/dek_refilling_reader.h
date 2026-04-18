/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/record_batch_reader.h"

namespace kafka {

/// Wraps a record_batch_reader to refill sentinel rp.encryption
/// headers within each batch. Cross-batch refill is handled by the
/// L1 reader; this wrapper handles intra-batch only.
model::record_batch_reader
make_dek_refilling_reader(model::record_batch_reader inner);

} // namespace kafka
