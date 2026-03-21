// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "context/context.h"
#include "context/context_frame.h"

#include <seastar/testing/perf_tests.hh>

#include <array>
#include <optional>
#include <ranges>

namespace {

using frame_t = context::context_frame<>;

// Noop frame: empty struct to measure construction/loop overhead only.
struct noop_frame;
struct noop_ref {
    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    noop_ref(noop_frame&) noexcept {}
};

struct noop_frame {
    explicit noop_frame(noop_ref) noexcept {}
};

// Never-cancelled noop root.
inline noop_ref noop_background() noexcept {
    static noop_frame instance{noop_ref{instance}};
    return instance;
}

// Tree structure: 2 spine + 100 fanout + 100*2 tails = 302 elements, depth 5
template<typename Frame, typename Ref, auto Background>
struct tree_5_deep_fanout_100 {
    std::array<std::optional<Frame>, 2> spine;
    std::array<std::optional<Frame>, 100> fanout;
    std::array<std::array<std::optional<Frame>, 2>, 100> tails;

    void build() {
        spine[0].emplace(Background());
        for (size_t i = 1; i < 2; ++i) {
            spine[i].emplace(Ref{*spine[i - 1]});
        }
        for (size_t i = 0; i < 100; ++i) {
            fanout[i].emplace(Ref{*spine[1]});
        }
        for (size_t i = 0; i < 100; ++i) {
            tails[i][0].emplace(Ref{*fanout[i]});
            for (size_t j = 1; j < 2; ++j) {
                tails[i][j].emplace(Ref{*tails[i][j - 1]});
            }
        }
    }

    void destroy() {
        for (auto& tail : tails) {
            for (int j = 1; j >= 0; --j) {
                tail[j].reset();
            }
        }
        for (int i = 99; i >= 0; --i) {
            fanout[i].reset();
        }
        for (int i = 1; i >= 0; --i) {
            spine[i].reset();
        }
    }

    Frame& root() { return *spine[0]; }
};

using noop_tree = tree_5_deep_fanout_100<noop_frame, noop_ref, noop_background>;
using real_tree
  = tree_5_deep_fanout_100<frame_t, context_ref, context::background>;

} // namespace

// Baseline: measures loop/emplace overhead with empty noop_frame
PERF_TEST(context, baseline_tree_5_deep_fanout_100) {
    noop_tree tree;

    perf_tests::start_measuring_time();
    tree.build();
    tree.destroy();
    perf_tests::do_not_optimize(tree.tails);
    perf_tests::stop_measuring_time();
}

// Measure tree construction (302 frames)
PERF_TEST(context, create_tree_5_deep_fanout_100) {
    real_tree tree;

    perf_tests::start_measuring_time();
    tree.build();
    tree.destroy();
    perf_tests::do_not_optimize(tree.tails);
    perf_tests::stop_measuring_time();
}

// Measure cancel propagation (302 frames)
PERF_TEST(context, cancel_tree_5_deep_fanout_100) {
    real_tree tree;
    tree.build();

    perf_tests::start_measuring_time();
    tree.root().trigger_cancel(context::cancel_cause::manual);
    perf_tests::do_not_optimize(tree.tails);
    perf_tests::stop_measuring_time();

    tree.destroy();
}

// Shallow tree: 1 parent with 100 children
PERF_TEST(context, create_tree_depth_1_fanout_100) {
    std::optional<frame_t> root;
    std::array<std::optional<frame_t>, 100> children;

    perf_tests::start_measuring_time();

    root.emplace(context::background());
    for (auto& f : children) {
        f.emplace(context_ref{*root});
    }

    perf_tests::do_not_optimize(children);
    perf_tests::stop_measuring_time();

    for (auto& it : std::ranges::reverse_view(children)) {
        it.reset();
    }
    root.reset();
}
