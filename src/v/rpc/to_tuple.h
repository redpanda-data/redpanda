#pragma once

// Credits: originally taken from cista.rocks (MIT License)

#include <tuple>

#include "rpc/arity.h"

namespace rpc {

template <typename T> inline auto to_tuple(T &t) {
  constexpr auto const a = arity<T>();
  if constexpr (a == 0) {
    return std::tie();
  } else if constexpr (a == 1) {
    auto &[p1] = t;
    return std::tie(p1);
  } else if constexpr (a == 2) {
    auto &[p1, p2] = t;
    return std::tie(p1, p2);
  } else if constexpr (a == 3) {
    auto &[p1, p2, p3] = t;
    return std::tie(p1, p2, p3);
  } else if constexpr (a == 4) {
    auto &[p1, p2, p3, p4] = t;
    return std::tie(p1, p2, p3, p4);
  } else if constexpr (a == 5) {
    auto &[p1, p2, p3, p4, p5] = t;
    return std::tie(p1, p2, p3, p4, p5);
  } else if constexpr (a == 6) {
    auto &[p1, p2, p3, p4, p5, p6] = t;
    return std::tie(p1, p2, p3, p4, p5, p6);
  } else if constexpr (a == 7) {
    auto &[p1, p2, p3, p4, p5, p6, p7] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7);
  } else if constexpr (a == 8) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8);
  } else if constexpr (a == 9) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8, p9] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9);
  } else if constexpr (a == 10) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
  } else if constexpr (a == 11) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11);
  } else if constexpr (a == 12) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12);
  } else if constexpr (a == 13) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);
  } else if constexpr (a == 14) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13,
                    p14);
  } else if constexpr (a == 15) {
    auto &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15] =
      t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14,
                    p15);
  } else if constexpr (a == 16) {
    auto
      &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16] =
      t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14,
                    p15, p16);
  } else if constexpr (a == 17) {
    auto
      &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16,
        p17] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14,
                    p15, p16, p17);
  } else if constexpr (a == 18) {
    auto
      &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16,
        p17,
        p18] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14,
                    p15, p16, p17, p18);
  } else if constexpr (a == 19) {
    auto
      &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16,
        p17, p18,
        p19] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14,
                    p15, p16, p17, p18, p19);
  } else if constexpr (a == 20) {
    auto
      &[p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16,
        p17, p18, p19,
        p20] = t;
    return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14,
                    p15, p16, p17, p18, p19, p20);
  }
}

}
