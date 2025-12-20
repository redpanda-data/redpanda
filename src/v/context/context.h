// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

/// \file context.h
/// \brief Hierarchical execution context for Seastar applications.
///
/// A hierarchical context that propagates cancellation, deadlines, and
/// diagnostics down the call tree. Designed for shard-local, stack-based
/// execution, it guarantees zero heap allocations and mandates structured
/// concurrency.
///
/// ## Core Concepts
///
/// - **Frame** (`context_frame<Mixins...>`): Stack-allocated context owner.
///   Holds cancellation flags, deadlines, and parent-child links.
/// - **Mixin**: Modular features composed into the frame. Examples:
///   - `deadline_timer`: Automatically cancels the frame upon expiration.
///   - `abort_source`: Bridge with `seastar::abort_source`.
/// - **Ref** (`context_ref`): Non-owning handle (8 bytes). Cheap to pass by
/// value.
/// - **background()**: Never-cancelled root context (thread-local singleton).
///
/// ## Lifetime
///
/// \warning Children MUST be destroyed before their parent (strict LIFO), and
/// references (`context_ref`, `cancel_handle`) MUST NOT outlive frames.
/// Siblings, however, may be destroyed in any order—concurrent operations
/// (e.g., parallel requests) complete independently.
///
/// \note **Shard-local:** Contexts strictly belong to a single shard and MUST
/// NOT be sent between shards.
///
/// ## Cancellation
///
/// Context provides **structured cancellation**: signals flow strictly from
/// parent to children. This enables **local reasoning** by ensuring two
/// invariants: you cannot accidentally cancel a scope you do not own (parent
/// scopes), and code you call cannot unexpectedly cancel your scope. Bottom-up
/// cancellation is only possible if you explicitly pass a `cancel_handle` to
/// the child or grant mutable access to the frame.
///
/// \warning **Cancel propagation is O(N)** over the subtree and may stall the
/// reactor. Realistic trees are small — most call sites pass `context_ref`
/// rather than create frames.
///
/// **Performance:**
/// - Status check (`is_cancelled()`): Inlined single-byte read.
/// - Propagation: O(N) over the subtree; incurs one virtual call per frame.
///
/// ## Deadlines
///
/// Deadlines are immutable once set. A child inherits its parent's deadline
/// and may shorten it, but cannot extend execution time beyond the parent's
/// limit.
///
/// \note Cancellation and deadlines are distinct. Always check `is_cancelled()`
/// to determine if work should stop, regardless of the remaining time.
/// \note Use `wall_deadline()` to convert timepoints when crossing system
/// boundaries.
///
/// ## Customization
///
/// Feature composition is handled via `context_frame<Mixins...>`. This allows
/// you to opt-in to features like timers or abort sources with zero overhead
/// for unused features.

#pragma once

#include "base/oncore.h"
#include "base/vassert.h"

#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <chrono>
#include <cstdint>

class context_ref;

namespace context {
class deadline_timer;
namespace detail {
class basic_context_frame;
} // namespace detail
} // namespace context

namespace context {

/// Canonical clock. seastar::lowres_clock is cached and zero-syscall with
/// granularity of ~task_quota (default 500µs).
using clock = seastar::lowres_clock;
using time_point = clock::time_point;
using duration = clock::duration;

/// System clock for wall-clock API boundaries.
using system_clock = std::chrono::system_clock;

/// Low resolution system clock for estimating wall-clock deadlines.
using lowres_system_clock = seastar::lowres_system_clock;

/// Infinite future (no deadline).
static inline constexpr time_point no_deadline = time_point::max();

/// Causes for context cancellation.
enum class cancel_cause : uint8_t {
    not_cancelled = 0, ///< Context is still active (not cancelled)
    manual = 1,        ///< Manually/explicitly cancelled
    deadline = 2,      ///< Deadline/timeout reached
};

namespace detail {
class basic_context_frame;

/// Operations table for frames needing custom cancellation behavior.
struct frame_ops {
    void (*on_cancel)(basic_context_frame&, cancel_cause) noexcept = nullptr;
};
} // namespace detail

/// \brief Handle to trigger cancellation.
/// \warning Invalidated when the associated frame is destroyed.
class cancel_handle {
public:
    explicit cancel_handle(detail::basic_context_frame& frame) noexcept;
    cancel_handle(const cancel_handle&) noexcept;
    cancel_handle(cancel_handle&&) noexcept;
    cancel_handle& operator=(const cancel_handle&) noexcept;
    cancel_handle& operator=(cancel_handle&&) noexcept;

#ifdef CONTEXT_DEBUG_REF_COUNTING
    ~cancel_handle();
#else
    ~cancel_handle() = default;
#endif

    /// \brief Cancels the associated frame and propagates to descendants.
    void trigger(cancel_cause cause) noexcept;

private:
    detail::basic_context_frame* frame_;
};

/// \brief Returns the never-cancelled root context singleton.
context_ref background() noexcept;

/// \brief A placeholder context_ref to use where a real context is not yet
/// available. May be used only for temporary stubbing during development.
/// \deprecated todo() is for temporary stubs only. For production, propagate
/// context_ref through your API to preserve cancellation, deadlines, and
/// tracing.
context_ref todo() noexcept;

/// \brief Estimates the Wall Clock deadline for crossing system boundaries.
/// \return system_clock::time_point or max() if no deadline.
system_clock::time_point wall_deadline(const context_ref ctx) noexcept;

} // namespace context

namespace context::detail {

struct background_ctor_tag {
private:
    friend class background_context_frame;
    background_ctor_tag() = default;
};

class basic_context_frame {
    friend class ::context_ref;
    friend class context::deadline_timer;
    friend class background_context_frame;
#ifdef CONTEXT_DEBUG_REF_COUNTING
    friend class context::cancel_handle;
#endif
public:
    basic_context_frame(const basic_context_frame&) = delete;
    basic_context_frame(basic_context_frame&&) = delete;
    basic_context_frame& operator=(const basic_context_frame&) = delete;
    basic_context_frame& operator=(basic_context_frame&&) = delete;

    /// \brief Returns a handle to trigger cancellation of this frame.
    [[nodiscard]] context::cancel_handle cancel_handle() noexcept
      [[clang::lifetimebound]] {
        return context::cancel_handle{*this};
    }

    /// \brief Returns true if cancelled.
    [[nodiscard]] bool is_cancelled() const noexcept {
        return cancel_cause_ != context::cancel_cause::not_cancelled;
    }

    /// \brief Returns the cancellation cause.
    [[nodiscard]] context::cancel_cause cancel_cause() const noexcept {
        return cancel_cause_;
    }

    /// \brief Returns the absolute deadline time point.
    /// \note Immutable once set. Unchanged by cancellation. Returns the
    /// original time quota regardless of cancellation state. Check
    /// is_cancelled() to determine if work should stop.
    /// \warning Relative to internal reference clock. For crossing system
    /// boundaries, see \ref wall_deadline().
    [[nodiscard]] context::time_point deadline() const noexcept {
        return deadline_;
    }

    /// \brief Returns time remaining until deadline.
    /// \note Continues counting down even after cancellation. Check
    /// is_cancelled() to determine if work should stop.
    /// \return Duration until deadline, or duration::max() if no deadline.
    /// Never negative.
    [[nodiscard]] context::duration time_left() const noexcept {
        if (deadline_ == context::no_deadline) {
            return context::duration::max();
        }
        return std::max(
          context::duration::zero(), deadline_ - context::clock::now());
    }

    /// \brief Cancels this frame and propagates to descendants.
    void trigger_cancel(const context::cancel_cause cause) noexcept {
        if (cause == context::cancel_cause::not_cancelled) {
            return;
        }
        if (!do_cancel_this_frame(cause)) {
            return;
        }
        propagate_cancel_to_children();
    }

protected:
    explicit basic_context_frame(context_ref parent) noexcept;
    explicit basic_context_frame(
      context_ref parent, const frame_ops* ops) noexcept;
    ~basic_context_frame() noexcept {
        vassert(
          child_ == nullptr,
          "Destroying context_frame with live children is not allowed");

#ifdef CONTEXT_DEBUG_REF_COUNTING
        vassert(
          live_refs_ == 0,
          "Destroying context_frame with live context_ref or cancel_handle is "
          "not allowed. live_refs_={}",
          live_refs_);
#endif

        // Update parent's child pointer if we're the head
        if (parent_ && parent_->child_ == this) {
            parent_->child_ = next_sibling_;
        }

        // Unlink from sibling list
        if (prev_sibling_) {
            prev_sibling_->next_sibling_ = next_sibling_;
        }
        if (next_sibling_) {
            next_sibling_->prev_sibling_ = prev_sibling_;
        }
    }

private:
    explicit basic_context_frame(background_ctor_tag) noexcept
      : parent_{nullptr} {}

    // Cause is already validated not to be not_cancelled.
    [[nodiscard]] bool
    do_cancel_this_frame(const context::cancel_cause cause) noexcept {
        // Do nothing if already cancelled. Do not override existing cause.
        if (is_cancelled()) {
            return false;
        }
        cancel_cause_ = cause;
        if (ops_ && ops_->on_cancel) {
            ops_->on_cancel(*this, cause);
        }
        return true;
    }

    void propagate_cancel_to_children() noexcept {
        basic_context_frame* curr = child_;
        while (curr) {
            bool did_cancel = curr->do_cancel_this_frame(cancel_cause_);

            // 1. Dive deeper (Depth First)
            if (curr->child_ && did_cancel) {
                curr = curr->child_;
                continue;
            }

            // 2. Visit siblings or ascend
            while (curr) {
                // If we have a sibling, visit it
                if (curr->next_sibling_) {
                    curr = curr->next_sibling_;
                    break;
                }

                // No sibling, ascend to parent
                curr = curr->parent_;

                // If we returned to 'this' (the frame being cancelled), we are
                // done
                if (curr == this) {
                    return;
                }
            }
        }
    }

    basic_context_frame* parent_;
    basic_context_frame* prev_sibling_{nullptr};
    basic_context_frame* next_sibling_{nullptr};
    basic_context_frame* child_{nullptr};
    const frame_ops* ops_{nullptr};

    context::time_point deadline_{context::no_deadline};

#ifdef CONTEXT_DEBUG_REF_COUNTING
    ssize_t live_refs_{0};
#endif

    context::cancel_cause cancel_cause_{context::cancel_cause::not_cancelled};
    expression_in_debug_mode(oncore _verify_shard);
};

class background_context_frame final : public basic_context_frame {
private:
    friend context_ref(context::background)() noexcept;

    background_context_frame() noexcept
      : basic_context_frame(background_ctor_tag{}) {}
};

} // namespace context::detail

class context_ref {
public:
    friend class context::detail::basic_context_frame;

    context_ref() = delete(
      "context_ref(s) may only be constructed from a context_frame. Consider "
      "context::background() if no other alternative is viable.");

    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    context_ref(
      context::detail::basic_context_frame& frame
      [[clang::lifetimebound]]) noexcept
      : frame_(&frame) {
#ifdef CONTEXT_DEBUG_REF_COUNTING
        ++frame_->live_refs_;
#endif
    }

    // Move = copy. Moved-from refs remain valid per [lib.types.movedfrom].
#ifdef CONTEXT_DEBUG_REF_COUNTING
    context_ref(const context_ref& other) noexcept
      : frame_(other.frame_) {
        ++frame_->live_refs_;
    }

    context_ref(context_ref&& other) noexcept
      : frame_(other.frame_) {
        ++frame_->live_refs_;
    }

    context_ref& operator=(const context_ref& other) noexcept {
        if (this != &other) {
            --frame_->live_refs_;
            frame_ = other.frame_;
            ++frame_->live_refs_;
        }
        return *this;
    }

    context_ref& operator=(context_ref&& other) noexcept {
        if (this != &other) {
            --frame_->live_refs_;
            frame_ = other.frame_;
            ++frame_->live_refs_;
        }
        return *this;
    }

    ~context_ref() noexcept { --frame_->live_refs_; }
#else
    context_ref(const context_ref&) = default;
    context_ref(context_ref&&) = default;
    context_ref& operator=(const context_ref&) = default;
    context_ref& operator=(context_ref&&) = default;
    ~context_ref() = default;
#endif

    /// \brief Returns true if cancelled.
    [[nodiscard]] bool is_cancelled() const noexcept {
        return frame_->is_cancelled();
    }

    /// \brief Returns the cancellation cause.
    [[nodiscard]] context::cancel_cause cancel_cause() const noexcept {
        return frame_->cancel_cause();
    }

    /// \brief Returns the absolute deadline time point.
    /// \note Immutable once set. Unchanged by cancellation. Returns the
    /// original time quota regardless of cancellation state. Check
    /// is_cancelled() to determine if work should stop.
    /// \warning Relative to internal reference clock. For crossing system
    /// boundaries, see \ref wall_deadline().
    [[nodiscard]] context::time_point deadline() const noexcept {
        return frame_->deadline();
    }

    /// \brief Returns true if a deadline is set.
    [[nodiscard]] bool has_deadline() const noexcept {
        return frame_->deadline() != context::no_deadline;
    }

    /// \brief Returns time remaining until deadline.
    /// \note Continues counting down even after cancellation. Check
    /// is_cancelled() to determine if work should stop.
    /// \return Duration until deadline, or duration::max() if no deadline.
    /// Never negative.
    [[nodiscard]] context::duration time_left() const noexcept {
        return frame_->time_left();
    }

private:
    context::detail::basic_context_frame* frame_;
    expression_in_debug_mode(oncore _verify_shard);
};

namespace context {

inline context_ref background() noexcept {
    static thread_local detail::background_context_frame instance;
    return instance;
}

inline context_ref todo() noexcept { return background(); }

inline detail::basic_context_frame::basic_context_frame(
  context_ref parent) noexcept
  : context::detail::basic_context_frame(parent, nullptr) {}

inline detail::basic_context_frame::basic_context_frame(
  context_ref parent, const frame_ops* ops) noexcept
  : parent_(parent.frame_)
  , ops_(ops)
  , deadline_(parent_->deadline_)
  , cancel_cause_(parent_->cancel_cause_) {
    // Prepend to parent's child list
    auto* old_child = parent_->child_;
    next_sibling_ = old_child;
    parent_->child_ = this;
    if (old_child) [[likely]] {
        old_child->prev_sibling_ = this;
    }
}

inline cancel_handle::cancel_handle(detail::basic_context_frame& frame) noexcept
  : frame_(&frame) {
#ifdef CONTEXT_DEBUG_REF_COUNTING
    ++frame_->live_refs_;
#endif
}

// Move = copy. Moved-from handles remain valid per [lib.types.movedfrom].
#ifdef CONTEXT_DEBUG_REF_COUNTING
inline cancel_handle::cancel_handle(const cancel_handle& other) noexcept
  : frame_(other.frame_) {
    ++frame_->live_refs_;
}

inline cancel_handle::cancel_handle(cancel_handle&& other) noexcept
  : frame_(other.frame_) {
    ++frame_->live_refs_;
}

inline cancel_handle&
cancel_handle::operator=(const cancel_handle& other) noexcept {
    if (this != &other) {
        --frame_->live_refs_;
        frame_ = other.frame_;
        ++frame_->live_refs_;
    }
    return *this;
}

inline cancel_handle& cancel_handle::operator=(cancel_handle&& other) noexcept {
    if (this != &other) {
        --frame_->live_refs_;
        frame_ = other.frame_;
        ++frame_->live_refs_;
    }
    return *this;
}

inline cancel_handle::~cancel_handle() { --frame_->live_refs_; }
#else
inline cancel_handle::cancel_handle(const cancel_handle& other) noexcept
  = default;
inline cancel_handle::cancel_handle(cancel_handle&& other) noexcept = default;
inline cancel_handle&
cancel_handle::operator=(const cancel_handle& other) noexcept
  = default;
inline cancel_handle& cancel_handle::operator=(cancel_handle&& other) noexcept
  = default;
#endif

inline void cancel_handle::trigger(cancel_cause cause) noexcept {
    frame_->trigger_cancel(cause);
}

} // namespace context
