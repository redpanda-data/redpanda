#include "utils/oc_latency.h"

oc_tracker::~oc_tracker() {
    if (has_state() && state()._parent) {
        record("end");
        state()._parent->tracker_finished(this);
    }
}

oc_tracker oc_tracker::empty_tracker{nullptr};

auto oc_latency::add_metric(ss::sstring what) {
    auto [it, added] = _event_hists.try_emplace(what);
    vassert(added, "duplicate event {}", what);

    const ss::metrics::label event_label{"event"};

    _metrics.add_group(
      "latency_tracker",
      {ss::metrics::make_histogram(
        "oc_lat",
        ss::metrics::description("oclat events"),
        {event_label(what)},
        [&hist = it->second] { // relies on node_hash_map pointer stability
            return hist.internal_histogram_logform();
        })});

    return it;
}

void oc_latency::tracker_finished(const tracker* t) {
    for (auto& e : t->state()._events) {
        ss::sstring what{e.what};
        // TODO: make lookup heterogeneous
        auto it = _event_hists.find(what);
        if (unlikely(it == _event_hists.end())) {
            it = add_metric(what);
        }
        it->second.record(e.when / std::chrono::microseconds(1));
    }
}

void record(const tracker_vector& tv, const char* what) {
    for (auto& t : tv) {
        t->record(what);
    }
}
