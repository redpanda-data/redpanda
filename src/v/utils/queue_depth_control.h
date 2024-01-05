#pragma once
#include "base/seastarx.h"
#include "ssx/semaphore.h"
#include "utils/ema.h"

/*
 * exponential moving average queue depth control. given a maximum latency
 * update the number of units available in the contained semaphore which can be
 * used to limit queue depth to control latency.
 *
 * when updated with a sample of `0` idleness is assumed. when idle if the depth
 * falls below the idle depth then idle depth takes precedence. otherwise, the
 * depth is constrained by min/max depth configuration parameters.
 */
class queue_depth_control {
public:
    queue_depth_control(
      std::chrono::milliseconds max_latency,
      double smoothing_factor,
      size_t idle_depth,
      size_t min_depth,
      size_t max_depth)
      : _max_latency(max_latency.count()) // NOLINT
      , _gamma(smoothing_factor)
      , _idle_depth(idle_depth)
      , _min_depth(min_depth)
      , _max_depth(max_depth)
      , _curr_depth(_idle_depth)
      , _queue(_curr_depth, "qdc") {}

    void update(const double sample) {
        auto new_depth = static_cast<size_t>(
          (1.0 - _gamma) * static_cast<double>(_curr_depth));

        // if it appears we've gone idle no windowing update is made based on
        // the current latency measurement, and the overall queue depth will
        // begin to decrease reverting eventually to the idle depth.
        if (sample > 0) {
            new_depth += static_cast<size_t>(
              (_gamma
               * ((_max_latency / sample) * static_cast<double>(_curr_depth))));
        }

        // apply depth bounds
        if (new_depth < _min_depth) {
            new_depth = _min_depth;
        } else if (new_depth > _max_depth) {
            new_depth = _max_depth;
        }

        // revert to idle
        if (sample == 0 && new_depth < _idle_depth) {
            new_depth = _idle_depth;
        }

        // adjust allowed queue depth
        if (new_depth < _curr_depth) {
            _queue.consume(_curr_depth - new_depth);
        } else if (new_depth > _curr_depth) {
            _queue.signal(new_depth - _curr_depth);
        }

        _curr_depth = new_depth;
    }

    size_t depth() const { return _curr_depth; }

    ss::future<ssx::semaphore_units> get_unit() {
        return ss::get_units(_queue, 1);
    }

private:
    const double _max_latency;
    const double _gamma;
    const size_t _idle_depth;
    const size_t _min_depth;
    const size_t _max_depth;
    size_t _curr_depth;
    ssx::semaphore _queue;
};
