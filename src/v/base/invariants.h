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

/// \brief Framework-agnostic runtime invariant and coverage assertions.
///
/// Provides a backend-neutral vocabulary for expressing properties that
/// testing frameworks (Antithesis, libfuzzer, sanitizers) can consume to
/// guide exploration or detect violations. Developers write invariants
/// once; the build configuration selects the backend.
///
/// Five primitives:
///
///   INV_ALWAYS(cond, msg)
///     Asserts `cond` must hold on every evaluation.
///     Violation = bug.
///
///   INV_SOMETIMES(cond, msg)
///     Existential claim: `cond` should be true at least once across all
///     runs. Used to verify test coverage quality ("did we exercise this
///     path?"). Never causes failures on its own.
///
///   INV_UNREACHABLE(msg)
///     Marks a code path that must never execute. Reaching it = bug.
///
///   INV_REACHABLE(msg)
///     Marks a code path that should be reached at least once across all
///     runs. Coverage assertion, not a correctness check.
///
///   INV_ALWAYS_OR_UNREACHABLE(cond, msg)
///     `cond` must hold whenever evaluated, but the site may also be
///     entirely unreached (in which case it trivially passes).
///
/// These are macros so that file/line/function information is captured at
/// the call site, which is required for the Antithesis SDK's assertion
/// catalog and useful for vassert diagnostics.
///
/// Backend selection (via build defines):
///
///   REDPANDA_INVARIANTS_ANTITHESIS
///     Forwards to the Antithesis SDK (ALWAYS, SOMETIMES, etc.).
///     Enabled automatically by --config=antithesis.
///
///   REDPANDA_INVARIANTS_SANITIZER
///     Uses __sanitizer_cov_trace_const_cmp for coverage guidance
///     (useful with libfuzzer) and __builtin_trap() for violations.
///
///   Default (no define)
///     `INV_ALWAYS` and `INV_UNREACHABLE` map to vassert. `INV_SOMETIMES`
///     and `INV_REACHABLE` are no-ops.

#if defined(REDPANDA_INVARIANTS_ANTITHESIS)

// -----------------------------------------------------------------------
// Backend: Antithesis SDK
// -----------------------------------------------------------------------
#include <antithesis_sdk.h>

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define INV_ALWAYS(cond, msg) ALWAYS(cond, msg, {})

#define INV_SOMETIMES(cond, msg) SOMETIMES(cond, msg, {})

#define INV_UNREACHABLE(msg)                                                   \
    do {                                                                       \
        UNREACHABLE(msg, {});                                                  \
        __builtin_trap();                                                      \
    } while (0)

#define INV_REACHABLE(msg) REACHABLE(msg, {})

#define INV_ALWAYS_OR_UNREACHABLE(cond, msg)                                   \
    ALWAYS_OR_UNREACHABLE(cond, msg, {})

// NOLINTEND(cppcoreguidelines-macro-usage)

#elif defined(REDPANDA_INVARIANTS_SANITIZER)

// -----------------------------------------------------------------------
// Backend: Sanitizer / libfuzzer coverage guidance
// -----------------------------------------------------------------------
//
// `INV_ALWAYS`/`INV_UNREACHABLE` trap on violation so the fuzzer reports
// a crash. `INV_SOMETIMES`/`INV_REACHABLE` inject a comparison trace that
// gives the fuzzer a gradient to guide it toward exercising the path.

#include <cstdint>

extern "C" {
void __sanitizer_cov_trace_const_cmp1(uint8_t, uint8_t);
}

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define INV_ALWAYS(cond, msg)                                                  \
    do {                                                                       \
        if (__builtin_expect(!(cond), false)) {                                \
            __builtin_trap();                                                  \
        }                                                                      \
    } while (0)

#define INV_SOMETIMES(cond, msg)                                               \
    __sanitizer_cov_trace_const_cmp1(1, (cond) ? 1 : 0)

#define INV_UNREACHABLE(msg) __builtin_trap()

#define INV_REACHABLE(msg) __sanitizer_cov_trace_const_cmp1(1, 1)

#define INV_ALWAYS_OR_UNREACHABLE(cond, msg) INV_ALWAYS(cond, msg)

// NOLINTEND(cppcoreguidelines-macro-usage)

#else

// -----------------------------------------------------------------------
// Backend: Default (vassert for violations, no-ops for coverage)
// -----------------------------------------------------------------------
#include "base/vassert.h"

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define INV_ALWAYS(cond, msg) vassert(cond, "{}", msg)

#define INV_SOMETIMES(cond, msg)                                               \
    do {                                                                       \
    } while (0)

#define INV_UNREACHABLE(msg)                                                   \
    do {                                                                       \
        vassert(false, "{}", msg);                                             \
        __builtin_unreachable();                                               \
    } while (0)

#define INV_REACHABLE(msg)                                                     \
    do {                                                                       \
    } while (0)

#define INV_ALWAYS_OR_UNREACHABLE(cond, msg) vassert(cond, "{}", msg)

// NOLINTEND(cppcoreguidelines-macro-usage)

#endif
