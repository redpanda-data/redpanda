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
#include "base/vlog_callsite.h"

#include "base/vlog_filter.h"

#include <atomic>
#include <cstring>
#include <fnmatch.h>
#include <memory>
#include <vector>

namespace vlog {

namespace {

bool rule_matches(const rule& r, const detail::callsite_base& cs) {
    if (r.file) {
        if (fnmatch(r.file->c_str(), cs.file(), 0) != 0) {
            return false;
        }
    }
    if (r.line) {
        const auto [lo, hi] = *r.line;
        if (cs.line() < lo || cs.line() > hi) {
            return false;
        }
    }
    if (r.contains) {
        const char* f = cs.fmt();
        if (f == nullptr || std::strstr(f, r.contains->c_str()) == nullptr) {
            return false;
        }
    }
    return true;
}

detail::callsite_base::state
evaluate(const std::vector<rule>& rules, const detail::callsite_base& cs) {
    auto result = detail::callsite_base::state::default_;
    for (const auto& r : rules) {
        if (rule_matches(r, cs)) {
            result = r.state;
        }
    }
    return result;
}

// Current rule set. Stored as an immutable shared_ptr; readers (first-call
// slow_init) snapshot it atomically and hold a ref until they finish
// evaluating, the writer (apply_rules) swaps in a new shared_ptr, and the
// old one is freed when the last outstanding reader releases its ref.
//
// We use the C++11 free functions std::atomic_load/store on shared_ptr
// rather than std::atomic<std::shared_ptr<T>>: the latter is a C++20
// specialization that libc++ still doesn't ship. The free functions are
// deprecated in C++20 but implemented by both libstdc++ and libc++, and
// there is no planned removal.
std::shared_ptr<const std::vector<rule>> g_rules
  = std::make_shared<const std::vector<rule>>();

std::shared_ptr<const std::vector<rule>> load_rules() {
    return std::atomic_load_explicit(&g_rules, std::memory_order_acquire);
}

void store_rules(std::shared_ptr<const std::vector<rule>> r) {
    std::atomic_store_explicit(
      &g_rules, std::move(r), std::memory_order_release);
}

} // namespace

namespace detail {

// Lock-free singly-linked list of callsites. Nodes are push-only — once
// registered, a callsite lives for the program's lifetime — so traversal
// needs no synchronization beyond an acquire load of the head. This keeps
// the admin path out of the logger thread's way: apply_rules is a pure
// traversal plus relaxed stores, never blocking.
class registry {
public:
    static registry& instance() {
        static registry r;
        return r;
    }

    void add(callsite_base* cs) noexcept {
        callsite_base* prev = _head.load(std::memory_order_relaxed);
        do {
            cs->_next = prev;
        } while (!_head.compare_exchange_weak(
          prev, cs, std::memory_order_release, std::memory_order_relaxed));
    }

    template<typename Fn>
    void visit(const Fn& fn) {
        for (callsite_base* cs = _head.load(std::memory_order_acquire);
             cs != nullptr;
             cs = cs->_next) {
            fn(*cs);
        }
    }

private:
    std::atomic<callsite_base*> _head{nullptr};
};

// First-call slow path. Concurrent first-callers race on a CAS of _state
// from uninit; exactly one thread wins and performs the single registry
// insertion plus the initial rule evaluation. Losers observe the
// tentative "enabled" value the winner published and return it — a
// racing reader may observe a transiently-enabled site that the winner
// will subsequently flip to disabled, but the next call converges on
// the published final state.
//
// Ordering: push onto the registry *before* loading the rules pointer.
// This preserves the invariant from the prior ctor-based design — a
// concurrent apply_rules() either sees this site during its walk (and
// writes the correct enabled state) or publishes the new rules pointer
// before we load it (so we evaluate against the new rules ourselves).
callsite_base::state callsite_base::slow_init() noexcept {
    state expected = state::uninit;
    if (!_state.compare_exchange_strong(
          expected,
          state::default_,
          std::memory_order_acq_rel,
          std::memory_order_acquire)) {
        return expected;
    }
    registry::instance().add(this);
    auto rules = load_rules();
    auto final_state = evaluate(*rules, *this);
    _state.store(final_state, std::memory_order_relaxed);
    return final_state;
}

} // namespace detail

void apply_rules(std::vector<rule> rules) {
    auto snapshot = std::make_shared<const std::vector<rule>>(std::move(rules));
    // Publish the new rules *before* walking. Any callsite registering
    // concurrently loads this pointer (or a newer one) after its own push,
    // so it cannot end up evaluating against an earlier rule set.
    store_rules(snapshot);
    detail::registry::instance().visit([&snapshot](detail::callsite_base& cs) {
        cs.set_state(evaluate(*snapshot, cs));
    });
}

std::vector<rule> get_rules() { return *load_rules(); }

void reset_rules() {
    store_rules(std::make_shared<const std::vector<rule>>());
    detail::registry::instance().visit([](detail::callsite_base& cs) {
        cs.set_state(detail::callsite_base::state::default_);
    });
}

void for_each_callsite(std::function<void(detail::callsite_base&)> fn) {
    detail::registry::instance().visit(
      [&fn](detail::callsite_base& cs) { fn(cs); });
}

} // namespace vlog
