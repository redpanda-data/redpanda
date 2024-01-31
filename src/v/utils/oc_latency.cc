#include "utils/oc_latency.h"

#include "ssx/future-util.h"

oc_tracker::~oc_tracker() {
    // if (has_state()) {
    // record("end");
    // ssx::background = ss::smp::submit_to(
    //   state()._source_shard, [state = std::move(_state)] {
    //       state->_parent->tracker_finished(*state);
    //   });
    // }
}

// oc_tracker oc_tracker::empty_tracker{};

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

void oc_tracker::record(const char* whats) {
    auto when = now_from_start();

    ss::sstring what{whats};
    // TODO: make lookup heterogeneous
    auto it = oc_latency::_local_instance._event_hists.find(what);
    if (unlikely(it == oc_latency::_local_instance._event_hists.end())) {
        it = oc_latency::_local_instance.add_metric(what);
    }
    it->second.record(when / std::chrono::microseconds(1));
}

// void oc_latency::tracker_finished(const tracker::state_holder& t) {
//     for (auto& e : t._events) {
//         ss::sstring what{e.what};
//         // TODO: make lookup heterogeneous
//         auto it = _event_hists.find(what);
//         if (unlikely(it == _event_hists.end())) {
//             it = add_metric(what);
//         }
//         it->second.record(e.when / std::chrono::microseconds(1));
//     }
// }

void record(const tracker_vector& tv, const char* what) {
    for (auto& t : tv) {
        t->record(what);
    }
}

thread_local oc_latency oc_latency::
  _local_instance; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
