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

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

/**
 * Generate random data for use in tests.
 */
seastar::future<seastar::temporary_buffer<char>>
make_random_data(size_t size, std::optional<uint64_t> alignment = std::nullopt);
