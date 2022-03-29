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

// derived from cista.rocks

#include "reflection/arity.h"

#include <tuple>

namespace reflection {

// clang-format off
template<typename T>
inline auto to_tuple(T& t) {
    constexpr auto const a = arity<T>();
    if constexpr (a == 0) {
        return std::tie();
    } else if constexpr (a == 1) {
        auto& [p1] = t;
        return std::tie(p1);
    } else if constexpr (a == 2) {
        auto& [p1, p2] = t;
        return std::tie(p1, p2);
    } else if constexpr (a == 3) {
        auto& [p1, p2, p3] = t;
        return std::tie(p1, p2, p3);
    } else if constexpr (a == 4) {
        auto& [p1, p2, p3, p4] = t;
        return std::tie(p1, p2, p3, p4);
    } else if constexpr (a == 5) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5] = t;
        return std::tie(p1, p2, p3, p4, p5);
    } else if constexpr (a == 6) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6] = t;
        return std::tie(p1, p2, p3, p4, p5, p6);
    } else if constexpr (a == 7) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7);
    } else if constexpr (a == 8) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8);
    } else if constexpr (a == 9) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9);
    } else if constexpr (a == 10) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
    } else if constexpr (a == 11) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11);
    } else if constexpr (a == 12) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12);
    } else if constexpr (a == 13) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);
    } else if constexpr (a == 14) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14] = t;
        return std::tie(
          p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14);
    } else if constexpr (a == 15) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15]
          = t;
        return std::tie(
          p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15);
    } else if constexpr (a == 16) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16);
    } else if constexpr (a == 17) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17);
    } else if constexpr (a == 18) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17,
          p18);
    } else if constexpr (a == 19) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17,
          p18,
          p19);
    } else if constexpr (a == 20) { // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17,
          p18,
          p19,
          p20);
    }
}
// clang-format on

} // namespace reflection
