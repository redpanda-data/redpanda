/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include <string_view>

namespace util {
class cpu_arch {
    // consteval means our string_view will have static lifetime
    explicit consteval cpu_arch(std::string_view name)
      : name{name} {}

    friend struct arch;

public:
    std::string_view name;

    static inline constexpr cpu_arch current();

    bool operator<=>(const cpu_arch&) const = default;
};

struct arch {
    static constexpr cpu_arch AMD64{"amd64"};
    static constexpr cpu_arch ARM64{"arm64"};
    static constexpr cpu_arch RISCV64{"riscv64"};
};

inline constexpr cpu_arch cpu_arch::current() {
#if defined(__x86_64__)
    return arch::AMD64;
#elif defined(__aarch64__)
    return arch::ARM64;
#elif defined(__riscv) && __riscv_xlen == 64
    return arch::RISCV64;
#else
#error unknown arch
#endif
}

} // namespace util
