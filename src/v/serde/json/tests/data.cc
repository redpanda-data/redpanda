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

#include "serde/json/tests/data.h"

#include "base/vassert.h"
#include "test_utils/runfiles.h"
#include "utils/file_io.h"

ss::future<iobuf> json_test_suite_sample() {
    static std::optional<iobuf> sample_buf;
    if (sample_buf.has_value()) {
        co_return sample_buf.value().copy();
    }

    auto test_case_path = test_utils::get_runfile_path(
      "src/v/serde/json/tests/testdata/json-test-suite/sample.json");
    sample_buf = co_await read_fully(test_case_path);

    co_return sample_buf.value().copy();
}
