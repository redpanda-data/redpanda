#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"
#include "oc_latency_fwd.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <chrono>
#include <memory>
#include <string_view>

using oc_default_clock = std::chrono::steady_clock;

struct event_record {
    const char* what;
    std::chrono::nanoseconds when; // offset from start time
};

struct oc_latency;

struct oc_tracker {
    using CLOCK = oc_default_clock;
    using parent = oc_latency;
    using time_point = CLOCK::time_point;

    explicit oc_tracker(parent* p)
      : _state{std::make_unique<state_holder>(p, now())} {}

    oc_tracker(oc_tracker&&) noexcept = default;
    oc_tracker(const oc_tracker&) noexcept = delete;

    ~oc_tracker();

    static time_point now() { return CLOCK::now(); }

    std::chrono::nanoseconds now_from_start() {
        using namespace std::chrono;
        return now() - state()._start;
    }

    /** record the given event at time now() */
    void record(const char* what) {
        auto when = now_from_start();
        state()._events.emplace_back(what, when);
    }

    struct state_holder {
        parent* _parent;
        time_point _start;
        std::vector<event_record> _events;
    };

    state_holder& state() {
        vassert(_state, "state() caled on empty tracker");
        return *_state;
    };

    const state_holder& state() const {
        vassert(_state, "state() caled on empty tracker");
        return *_state;
    };

    bool has_state() const { return _state.get(); }

    static oc_tracker empty_tracker;

    std::unique_ptr<state_holder> _state;
};

struct oc_latency {
    using CLOCK = oc_default_clock;
    using tracker = oc_tracker;

    tracker new_tracker() { return tracker{this}; }

    void tracker_finished(const tracker* t);

    /**
     * @brief Add a new series with the event label set to the given value.
     *
     * @param what
     */
    auto add_metric(ss::sstring what);

    log_hist_internal _hist;
    ss::metrics::metric_groups _metrics;
    absl::node_hash_map<ss::sstring, log_hist_internal> _event_hists;
};

inline shared_tracker make_shared_tracker(oc_tracker&& tracker) {
    return std::make_shared<oc_tracker>(std::move(tracker));
}
