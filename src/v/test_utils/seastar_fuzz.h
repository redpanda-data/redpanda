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

#include <cstddef>
#include <cstdint>
#include <exception>

namespace seastar_fuzz::internal {

void check_no_abandoned_failed_futures();

/// Reports \p e and aborts, which libFuzzer handles as a deadly signal and
/// records with an artifact.
[[noreturn]] void abort_on_exception(std::exception_ptr e);

/// Expanded by RP_SEASTAR_FUZZ; not for direct use.
int run_driver(
  int argc, char** argv, int (*callback)(const uint8_t* data, size_t size));

/// The callback libFuzzer gets: the target's, plus the per-input checks. A
/// template parameter because LLVMFuzzerRunDriver takes a plain function
/// pointer.
template<int (*Callback)(const uint8_t* data, size_t size)>
int checked_callback(const uint8_t* data, size_t size) {
    int rc = 0;
    try {
        rc = Callback(data, size);
    } catch (...) {
        abort_on_exception(std::current_exception());
    }
    check_no_abandoned_failed_futures();
    return rc;
}

} // namespace seastar_fuzz::internal

/// Makes this translation unit a libFuzzer target that runs on a seastar
/// reactor. It defines main, so expand it exactly once per target and set
/// reactor = True on the redpanda_cc_fuzz_test rule; the target does not link
/// otherwise.
///
/// \code
///   int fuzz_one_input(const uint8_t* data, size_t size) {
///       // Fuzz body. Runs in a seastar thread, so it can .get() a future.
///       apply(data, size).get();
///       return 0;
///   }
///
///   RP_SEASTAR_FUZZ(fuzz_one_input);
/// \endcode
///
/// Build with --config=fuzz, which adds the coverage instrumentation libFuzzer
/// needs to guide the search.
///
/// The reactor comes up on the main thread and libFuzzer's loop runs inside it.
/// A crash, an exception out of the body, or an abandoned failed future ends
/// the run and writes an artifact to replay. So does a leak in the body:
/// libFuzzer checks between inputs and at exit. Reactor-boot allocations are
/// exempt (see run_driver's implementation).
///
/// Report a failed check with vassert rather than by throwing so the stack
/// points at the failure instead of at the harness.
///
/// Leave nothing behind: no unfinished work, no state built from the input.
/// Unfinished work reads the input buffer after libFuzzer frees it, a
/// use-after-free. Leftover state makes the crash depend on every input before
/// it, and the artifact holds only one. Immutable setup is fine to share.
///
/// Real I/O, timers and the wall clock tie a run to scheduling rather than to
/// the input, so artifacts replay less reliably and coverage drifts between
/// runs. Prefer an in-memory ss::file_impl to a file, ss::manual_clock to the
/// system clock, and calling a timer handler from the input to arming a timer.
///
/// The search also scales with executions per second: one 10ms sleep per input
/// caps the target at 100 a second, whatever the machine. Use the real file,
/// clock or timer when that is what you are testing; a slow target still finds
/// bugs.
///
/// Arguments before -- go to libFuzzer, those after to the reactor, which
/// runs one shard (--smp/-c are rejected) and defaults to warn-level logging:
///
///     bazel run --config=fuzz //target -- corpus/ -- --default-log-level=debug
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define RP_SEASTAR_FUZZ(callback)                                              \
    int main(int argc, char** argv) {                                          \
        return seastar_fuzz::internal::run_driver(                             \
          argc, argv, seastar_fuzz::internal::checked_callback<callback>);     \
    }                                                                          \
    static_assert(true)
