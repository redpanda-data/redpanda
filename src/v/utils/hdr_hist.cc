#include "utils/hdr_hist.h"

#include "utils/human.h"

#include <iostream>

void hdr_hist::record(uint64_t value) {
    _sample_count++;
    _sample_sum += value;
    ::hdr_record_value(_hist.get(), value);
}
void hdr_hist::record_multiple_times(uint64_t value, uint32_t times) {
    _sample_count += times;
    _sample_sum += value * times;
    ::hdr_record_values(_hist.get(), value, times);
}
void hdr_hist::record_corrected(uint64_t value, uint64_t interval) {
    // TODO: how should summation work for coordinated omission values? the sum
    // is tracked outside hdr, currently.
    _sample_count++;
    _sample_sum += value;
    ::hdr_record_corrected_value(_hist.get(), value, interval);
}
// getters
int64_t hdr_hist::get_value_at(double percentile) const {
    return ::hdr_value_at_percentile(_hist.get(), percentile);
}
double hdr_hist::stddev() const {
    return ::hdr_stddev(_hist.get());
}
double hdr_hist::mean() const {
    return ::hdr_mean(_hist.get());
}
size_t hdr_hist::memory_size() const {
    return ::hdr_get_memory_size(_hist.get());
}
metrics::histogram hdr_hist::seastar_histogram_logform() const {
    // logarithmic histogram configuration. this will range from 10 microseconds
    // through around 6000 seconds with 26 buckets doubling.
    //
    // TODO:
    //   1) expose these settings through arguments
    //   2) upstream log_base fix for sub-2.0 values. in hdr the log_base is a
    //   double but is truncated (see the int64_t casts on log_base below which
    //   is the same as in the hdr C library). this means that if we want
    //   buckets with a log base of 1.5, the histogram becomes linear...
    constexpr size_t num_buckets = 26;
    constexpr int64_t first_value = 10;
    constexpr double log_base = 2.0;

    seastar::metrics::histogram sshist;
    sshist.buckets.resize(num_buckets);

    sshist.sample_count = _sample_count;
    sshist.sample_sum = static_cast<double>(_sample_sum);

    // stack allocated; no cleanup needed
    struct hdr_iter iter;
    struct hdr_iter_log* log = &iter.specifics.log;
    hdr_iter_log_init(&iter, _hist.get(), first_value, log_base);

    // fill in buckets from hdr histogram logarithmic iteration. there may be
    // more or less hdr buckets reported than what will be returned to seastar.
    size_t bucket_idx = 0;
    for (; hdr_iter_next(&iter) && bucket_idx < sshist.buckets.size();
         bucket_idx++) {
        auto& bucket = sshist.buckets[bucket_idx];
        bucket.count = iter.cumulative_count;
        bucket.upper_bound = iter.value_iterated_to;
    }

    if (bucket_idx == 0) {
        // if the histogram is empty hdr_iter_init doesn't initialize the first
        // bucket value which is neede by the loop below.
        iter.value_iterated_to = first_value;
    } else if (bucket_idx < sshist.buckets.size()) {
        // if there are padding buckets that need to be created, advance the
        // bucket boundary which would normally be done by hdr_iter_next, except
        // that doesn't happen when iteration reaches the end of the recorded
        // values.
        iter.value_iterated_to *= static_cast<int64_t>(log->log_base);
    }

    // prometheus expects a fixed number of buckets. hdr iteration will stop
    // after the max observed value. this loops pads buckets past iteration, if
    // needed.
    for (; bucket_idx < sshist.buckets.size(); bucket_idx++) {
        auto& bucket = sshist.buckets[bucket_idx];
        bucket.count = iter.cumulative_count;
        bucket.upper_bound = iter.value_iterated_to;
        iter.value_iterated_to *= static_cast<int64_t>(log->log_base);
    }

    return sshist;
}

std::unique_ptr<hdr_hist::measurement> hdr_hist::auto_measure() {
    return std::make_unique<measurement>(*this);
}

hdr_hist::~hdr_hist() {
    for (auto& m : _probes) {
        m.detach_hdr_hist();
    }
}

std::ostream& operator<<(std::ostream& o, const hdr_hist& h) {
    return o << "{p10=" << human::latency{(double)h.get_value_at(.1)}
             << ",p50=" << human::latency{(double)h.get_value_at(.5)}
             << ",p90=" << human::latency{(double)h.get_value_at(.9)}
             << ",p99=" << human::latency{(double)h.get_value_at(.99)}
             << ",p999=" << human::latency{(double)h.get_value_at(.990)}
             << ",max=" << human::latency{(double)h.get_value_at(1)} << "}";
};
