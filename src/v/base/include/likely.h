/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

// https://godbolt.org/z/iRot_i
// they must be macros or behavior is altered
// Replace with c++20 [[likely]] attr
// https://en.cppreference.com/w/cpp/language/attributes/likely
//
// From Abseil:
// Recommendation: Modern CPUs dynamically predict branch execution paths,
// typically with accuracy greater than 97%. As a result, annotating every
// branch in a codebase is likely counterproductive; however, annotating
// specific branches that are both hot and consistently mispredicted is likely
// to yield performance improvements.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define likely(cond) __builtin_expect(!!(cond), true)
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define unlikely(cond) __builtin_expect(!!(cond), false)
