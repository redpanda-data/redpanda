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

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace vlog::detail {

/// Structural wrapper around a string literal so it can live inside a
/// non-type template parameter. C++20 forbids taking the address of a
/// string-literal subobject as an NTTP value, but a value-type containing
/// an array of char is structural and eligible — so we copy the literal's
/// bytes into this object and pass it by value.
template<std::size_t N>
struct fixed_string {
    char data[N]{};

    constexpr fixed_string(const char (&s)[N]) noexcept {
        for (std::size_t i = 0; i < N; ++i) {
            data[i] = s[i];
        }
    }

    constexpr const char* c_str() const noexcept { return data; }
};

template<std::size_t N>
fixed_string(const char (&)[N]) -> fixed_string<N>;

/// Per-callsite metadata carried as a non-type template parameter. Sizes
/// are deduced from the string-literal array bounds at the macro call so
/// each site's file and format bytes live inside the NTTP value itself.
template<std::size_t FileN, std::size_t FmtN>
struct site_info {
    fixed_string<FileN> file;
    unsigned line;
    fixed_string<FmtN> fmt;
};

/// Factory for the NTTP value. consteval because the only valid use is in
/// a template argument position — calling it at runtime would be a bug
/// that the constraint catches at compile time.
template<std::size_t FileN, std::size_t FmtN>
consteval site_info<FileN, FmtN> make_site_nttp(
  const char (&file)[FileN], unsigned line, const char (&fmt)[FmtN]) {
    return site_info<FileN, FmtN>{
      .file = fixed_string<FileN>{file},
      .line = line,
      .fmt = fixed_string<FmtN>{fmt},
    };
}

/// Non-template base. Holds the mutable per-site state and the registry
/// link so that the process-wide registry and the admin walk
/// (`apply_rules`, `for_each_callsite`) can work against a single concrete
/// type regardless of which class-template instantiation produced the
/// site.
class callsite_base {
public:
    callsite_base(const callsite_base&) = delete;
    callsite_base& operator=(const callsite_base&) = delete;
    callsite_base(callsite_base&&) = delete;
    callsite_base& operator=(callsite_base&&) = delete;

    /// The resolved state for this callsite. The vlog* macros switch on
    /// this value: default_ goes through the logger's level gate, force_on
    /// calls the logger's force-tag overload, force_off drops the call
    /// entirely.
    enum class state : std::uint8_t {
        uninit = 0, // zero-init ground state, must remain 0
        default_ = 1,
        force_on = 2,
        force_off = 3,
    };

    /// Cheap gate consulted on every vlog(...) invocation. A single relaxed
    /// load of one byte plus a well-predicted branch; the uninit slow path
    /// registers the site and evaluates the active rule set.
    state resolved_state() noexcept {
        auto s = _state.load(std::memory_order_relaxed);
        if (s == state::uninit) [[unlikely]] {
            s = slow_init();
        }
        return s;
    }

    void set_state(state s) noexcept {
        _state.store(s, std::memory_order_relaxed);
    }

    const char* file() const noexcept { return _file; }
    unsigned line() const noexcept { return _line; }
    const char* fmt() const noexcept { return _fmt; }

protected:
    constexpr callsite_base(const char* f, unsigned l, const char* fmt) noexcept
      : _file(f)
      , _line(l)
      , _fmt(fmt) {}
    ~callsite_base() = default;

private:
    // First-call slow path: claims init via CAS on _state, registers with
    // the process-wide registry, evaluates the current rule set, and
    // publishes the final state. Subsequent calls see a non-uninit state
    // and take only the fast path above.
    state slow_init() noexcept;

    const char* _file;
    unsigned _line;
    const char* _fmt;
    std::atomic<state> _state{state::uninit};
    callsite_base* _next{nullptr};

    friend class registry;
};

/// One class-template instantiation per vlog macro expansion. Because the
/// default constructor forwards constexpr NTTP fields to a constexpr base
/// constructor and no members require dynamic initialization, the static
/// instance is constant-initialized at program start: no __cxa_guard, no
/// per-entry once-check on the hot path.
template<auto Info>
class callsite final : public callsite_base {
public:
    constexpr callsite() noexcept
      : callsite_base(Info.file.c_str(), Info.line, Info.fmt.c_str()) {}
};

} // namespace vlog::detail
