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

#include <functional>

namespace seastar_fuzz {

/// Run \p fn on a Seastar reactor, for fuzzing code that returns
/// `ss::future<>`.
///
/// libFuzzer owns `main()` and the thread it calls the test body on, so a
/// reactor cannot be driven there, and a future's `.get()` is only legal inside
/// an `ss::thread`. So the first call starts Seastar's test reactor (see
/// `ss::testing::global_test_runner`) on its own thread and registers a clean
/// shutdown at process exit.
///
/// Exceptions thrown by \p fn propagate out of this call (and so abort the
/// process under libFuzzer/ASan).
///
/// Typical use:
/// \code
///   extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
///       std::string input(reinterpret_cast<const char*>(data), size);
///       seastar_fuzz::test_one_input([input = std::move(input)] {
///           my_fuzz_body(input);
///       });
///       return 0;
///   }
/// \endcode
void test_one_input(std::function<void()> fn);

} // namespace seastar_fuzz
