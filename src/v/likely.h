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
#define likely(cond) __builtin_expect(cond, true)
#define unlikely(cond) __builtin_expect(cond, false)
