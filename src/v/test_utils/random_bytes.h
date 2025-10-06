/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "random/generators.h"

#include <cstdlib>

namespace tests {

using rng_t = random_generators::rng;

// generate n random bytes with the the given generator
bytes random_bytes(
  size_t n = 128 * 1024, rng_t& rng = random_generators::global());

// use limited set of printable ascii chars only
iobuf random_iobuf(size_t n = 128, rng_t& rng = random_generators::global());

// copy data from string into a randomly split iobuf
iobuf fragmented_iobuf(
  std::string_view str,
  int n_fragments,
  rng_t& rng = random_generators::global());

} // namespace tests
