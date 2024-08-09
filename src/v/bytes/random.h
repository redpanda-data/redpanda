/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"

namespace random_generators {

bytes get_bytes(size_t n = 128 * 1024);

bytes get_crypto_bytes(size_t n = 128 * 1024, bool use_private_rng = false);

// Makes an random alphanumeric string, encoded in an iobuf.
iobuf make_iobuf(size_t n = 128);

} // namespace random_generators
