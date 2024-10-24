// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/hdr_hist.h"

#include "base/likely.h"
#include "utils/human.h"

#include <fmt/format.h>

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

hdr_hist& hdr_hist::operator+=(const hdr_hist& o) {
    ::hdr_add(_hist.get(), o._hist.get());
    return *this;
}

ss::temporary_buffer<char> hdr_hist::print_classic() const {
    char* buf = nullptr;
    std::size_t len = 0;
    FILE* fp = open_memstream(&buf, &len);
    if (unlikely(fp == nullptr)) {
        throw std::runtime_error("Failed to allocate filestream");
    }
    const int p_ret = ::hdr_percentiles_print(
      _hist.get(),
      fp,       // File to write to
      5,        // Granularity of printed values
      1.0,      // Multiplier for results
      CLASSIC); // Format CLASSIC/CSV supported.
    // fflush in order to have len update
    fflush(fp);
    fclose(fp);
    if (p_ret != 0) {
        throw std::runtime_error(
          fmt::format("Failed to print histogram: {}", p_ret));
    }
    return ss::temporary_buffer<char>(buf, len, ss::make_free_deleter(buf));
}
// getters
int64_t hdr_hist::get_value_at(double percentile) const {
    return ::hdr_value_at_percentile(_hist.get(), percentile);
}
double hdr_hist::stddev() const { return ::hdr_stddev(_hist.get()); }
double hdr_hist::mean() const { return ::hdr_mean(_hist.get()); }
size_t hdr_hist::memory_size() const {
    return ::hdr_get_memory_size(_hist.get());
}
ss::metrics::histogram hdr_hist::seastar_histogram_logform(
  size_t num_buckets,
  int64_t first_value,
  double log_base,
  int64_t scale) const {
    // logarithmic histogram configuration. this will range from 10 microseconds
    // through around 6000 seconds with 26 buckets doubling.
    //
    // TODO:
    //   1) expose these settings through arguments
    //   2) upstream log_base fix for sub-2.0 values. in hdr the log_base is a
    //   double but is truncated (see the int64_t casts on log_base below which
    //   is the same as in the hdr C library). this means that if we want
    //   buckets with a log base of 1.5, the histogram becomes linear...

    ss::metrics::histogram sshist;
    sshist.buckets.resize(num_buckets);

    sshist.sample_count = _sample_count;
    sshist.sample_sum = static_cast<double>(_sample_sum)
                        / static_cast<double>(scale);

    // stack allocated; no cleanup needed
    struct hdr_iter iter;
    struct hdr_iter_log* log = &iter.specifics.log;

    const auto log_iter_first_bucket_size = std::max(
      _first_discernible_value, first_value);
    hdr_iter_log_init(&iter, _hist.get(), log_iter_first_bucket_size, log_base);

    // hdr_iter.value_iterated_to does not get updated by hdr_iter_next
    // if the size of the histogram bucket is smaller than the size of
    // the logarithmic iteration bucket. For this reason, we keep track
    // of the value we have iterated to separately.
    int64_t iterated_to = log_iter_first_bucket_size;

    // fill in buckets from hdr histogram logarithmic iteration. there may be
    // more or less hdr buckets reported than what will be returned to
    // seastar.
    size_t bucket_idx = 0;
    for (; hdr_iter_next(&iter) && bucket_idx < sshist.buckets.size();
         bucket_idx++) {
        auto& bucket = sshist.buckets[bucket_idx];
        bucket.count = iter.cumulative_count;
        bucket.upper_bound = static_cast<double>(iter.highest_equivalent_value)
                             / static_cast<double>(scale);

        iterated_to *= static_cast<int64_t>(log->log_base);
    }

    // prometheus expects a fixed number of buckets. hdr iteration will stop
    // after the max observed value. this loops pads buckets past iteration, if
    // needed.
    for (; bucket_idx < sshist.buckets.size(); bucket_idx++) {
        auto& bucket = sshist.buckets[bucket_idx];
        bucket.count = iter.cumulative_count;

        const int64_t range_size = hdr_size_of_equivalent_value_range(
          _hist.get(), iterated_to);
        const int64_t lowest_equivalent_value = hdr_lowest_equivalent_value(
          _hist.get(), iterated_to);
        const int64_t highest_equivalent_value = lowest_equivalent_value
                                                 + range_size - 1;
        bucket.upper_bound = static_cast<double>(highest_equivalent_value)
                             / static_cast<double>(scale);

        iterated_to *= static_cast<int64_t>(log->log_base);
    }

    return sshist;
}

std::unique_ptr<hdr_hist::measurement> hdr_hist::auto_measure() {
    return std::make_unique<measurement>(*this);
}

hdr_hist::~hdr_hist() noexcept {
    for (auto& m : _probes) {
        m.detach_hdr_hist();
    }
}

std::ostream& operator<<(std::ostream& o, const hdr_hist::measurement& m) {
    return o << "{duration:" << m.compute_duration_micros() << "us}";
}
std::ostream& operator<<(std::ostream& o, const hdr_hist& h) {
    return o << "{p10=" << human::latency{(double)h.get_value_at(10.0)}
             << ",p50=" << human::latency{(double)h.get_value_at(50.0)}
             << ",p90=" << human::latency{(double)h.get_value_at(90.0)}
             << ",p99=" << human::latency{(double)h.get_value_at(99.0)}
             << ",p999=" << human::latency{(double)h.get_value_at(99.9)}
             << ",max=" << human::latency{(double)h.get_value_at(100.0)} << "}";
};
