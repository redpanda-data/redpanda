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

#include "io/page.h"
#include "random/generators.h"

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

namespace io = experimental::io;

/**
 * Generate random data for use in tests. If seed is unspecified then a
 * pre-seeded random generator will be used.
 */
seastar::future<seastar::temporary_buffer<char>> make_random_data(
  size_t size,
  std::optional<uint64_t> alignment = std::nullopt,
  std::optional<uint64_t> seed = std::nullopt);

/**
 * Generate a page with the given offset and random data. The seed is passed to
 * the random number generator. If no seed is given then a pre-seeded engine
 * will be used.
 *
 * Hard-coded as 4K pages with 4K alignment.
 */
seastar::lw_shared_ptr<io::page>
make_page(uint64_t offset, std::optional<uint64_t> seed = std::nullopt);

/**
 * Sleep for a random number of milliseconds chosen from the range (min, max).
 */
void sleep_ms(unsigned min, unsigned max);
