// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "context/context.h"
#include "context/context_frame.h"

#include <cstddef>
#include <new>

namespace context {

/// Mixin for depending on multiple parent contexts, propagating state from any
/// linked parent.
///
/// \tparam N Number of additional parent contexts to link.
///
/// \note This mixin performs zero heap allocation. Storage for link frames
/// is allocated inline within the mixin using compile-time sized storage.
///
/// \warning All linked parents must outlive the child context, same as the
/// primary parent. This is enforced by assertions in debug builds.
template<size_t N>
class linker {
    template<typename...>
    friend class context_frame;

    struct link_frame final : detail::basic_context_frame {
        context::cancel_handle target_;

        static void on_cancel_impl(
          detail::basic_context_frame& base,
          context::cancel_cause cause) noexcept {
            static_cast<link_frame&>(base).target_.trigger(cause);
        }

        static constexpr detail::frame_ops ops{.on_cancel = &on_cancel_impl};

        link_frame(context_ref parent, context::cancel_handle target) noexcept
          : basic_context_frame(parent, &ops)
          , target_(target) {}
    };

    // Raw storage for N link frames; objects constructed via placement new.
    // NOLINTNEXTLINE(hicpp-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    alignas(link_frame) std::byte storage_[N * sizeof(link_frame)] = {};

    // Number of link frames actually constructed (for safe destruction)
    size_t initialized_{0};

    // Address for placement new at index i (no object need exist yet)
    void* slot(size_t i) noexcept { return &storage_[i * sizeof(link_frame)]; }

    // Access constructed link_frame at index i (object must exist)
    link_frame& link_at(size_t i) noexcept {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return *std::launder(reinterpret_cast<link_frame*>(slot(i)));
    }

public:
    linker() = default;

    linker& operator=(linker&&) = delete;
    linker(const linker&) = delete;
    linker& operator=(const linker&) = delete;

    ~linker() {
        // Destroy only the link frames that were actually constructed
        for (size_t i = initialized_; i-- > 0;) {
            link_at(i).~link_frame();
        }
    }

private:
    // Move constructor exists only to satisfy mixin construction requirements.
    // Moves are only valid before on_context_init (i.e., before any link_frame
    // objects exist in storage).
    linker(linker&& other) noexcept {
        vassert(
          other.initialized_ == 0,
          "linker mixin move after initialization is not supported");
    }

    template<typename Self, typename... Refs>
    requires(
      sizeof...(Refs) == N
      && (std::same_as<std::decay_t<Refs>, context_ref> && ...))
    void on_context_init(this Self& self, Refs... extras) {
        auto handle = self.cancel_handle();
        (
          [&](this auto) {
              self.deadline_ = std::min(self.deadline_, extras.deadline());
              new (self.slot(self.initialized_)) link_frame(extras, handle);
              ++self.initialized_;
              if (extras.is_cancelled()) {
                  handle.trigger(extras.cancel_cause());
              }
          }(),
          ...);
    }
};

/// Creates a linked context frame (N is deduced from the argument count).
template<typename... Refs>
requires(
  sizeof...(Refs) >= 1
  && (std::same_as<std::decay_t<Refs>, context_ref> && ...))
[[nodiscard]] auto link(context_ref primary, Refs... additional) {
    return context_frame<linker<sizeof...(Refs)>>{
      primary, with<linker<sizeof...(Refs)>>(additional...)};
}

} // namespace context
