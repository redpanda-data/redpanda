/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/log_hist.h"

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
template<typename cfg>
seastar::metrics::histogram
log_hist<number_of_buckets, first_bucket_upper_bound>::
  seastar_histogram_logform() const {
    seastar::metrics::histogram hist;
    hist.buckets.resize(cfg::bucket_count);
    hist.sample_sum = static_cast<double>(_sample_sum)
                      / static_cast<double>(cfg::scale);

    const unsigned first_bucket_exp
      = 64 - std::countl_zero(first_bucket_upper_bound - 1);
    const unsigned cfg_first_bucket_exp
      = 64 - std::countl_zero(cfg::first_bucket_bound - 1);

    // Write bounds to seastar histogram
    for (int i = 0; i < cfg::bucket_count; i++) {
        auto& bucket = hist.buckets[i];
        bucket.count = 0;

        uint64_t unscaled_upper_bound = ((uint64_t)1
                                         << (cfg_first_bucket_exp + i))
                                        - 1;
        bucket.upper_bound = static_cast<double>(unscaled_upper_bound)
                             / static_cast<double>(cfg::scale);
    }

    uint64_t cumulative_count = 0;
    size_t current_hist_idx = 0;
    double current_hist_upper_bound = hist.buckets[0].upper_bound;

    // Write _counts to seastar histogram
    for (size_t i = 0; i < _counts.size(); i++) {
        uint64_t unscaled_upper_bound = ((uint64_t)1 << (first_bucket_exp + i))
                                        - 1;
        double scaled_upper_bound = static_cast<double>(unscaled_upper_bound)
                                    / static_cast<double>(cfg::scale);

        cumulative_count += _counts[i];

        while (scaled_upper_bound > current_hist_upper_bound
               && current_hist_idx != (hist.buckets.size() - 1)) {
            current_hist_idx++;
            current_hist_upper_bound
              = hist.buckets[current_hist_idx].upper_bound;
        }

        hist.buckets[current_hist_idx].count = cumulative_count;
    }

    hist.sample_count = cumulative_count;
    return hist;
}

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
seastar::metrics::histogram
log_hist<number_of_buckets, first_bucket_upper_bound>::
  public_histogram_logform() const {
    using public_hist_config = logform_config<1'000'000l, 256ul, 18>;

    return seastar_histogram_logform<public_hist_config>();
}

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
seastar::metrics::histogram
log_hist<number_of_buckets, first_bucket_upper_bound>::
  internal_histogram_logform() const {
    using internal_hist_config = logform_config<1l, 8ul, 26>;

    return seastar_histogram_logform<internal_hist_config>();
}

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
seastar::metrics::histogram
log_hist<number_of_buckets, first_bucket_upper_bound>::
  read_dist_histogram_logform() const {
    using read_distribution_config = logform_config<1l, 4ul, 16>;

    return seastar_histogram_logform<read_distribution_config>();
}

template<int number_of_buckets, uint64_t first_bucket_upper_bound>
seastar::metrics::histogram
log_hist<number_of_buckets, first_bucket_upper_bound>::
  client_quota_histogram_logform() const {
    using client_quota_config = logform_config<1'000l, 1ul, 15>;

    return seastar_histogram_logform<client_quota_config>();
}

template<
  class duration_t,
  int number_of_buckets,
  uint64_t first_bucket_upper_bound>
requires detail::is_duration_v<duration_t>
seastar::metrics::histogram
latency_log_hist<duration_t, number_of_buckets, first_bucket_upper_bound>::
  public_histogram_logform() const {
    return _histo.public_histogram_logform();
}

template<
  class duration_t,
  int number_of_buckets,
  uint64_t first_bucket_upper_bound>
requires detail::is_duration_v<duration_t>
seastar::metrics::histogram
latency_log_hist<duration_t, number_of_buckets, first_bucket_upper_bound>::
  internal_histogram_logform() const {
    return _histo.internal_histogram_logform();
}

template<
  class duration_t,
  int number_of_buckets,
  uint64_t first_bucket_upper_bound>
requires detail::is_duration_v<duration_t>
seastar::metrics::histogram
latency_log_hist<duration_t, number_of_buckets, first_bucket_upper_bound>::
  read_dist_histogram_logform() const {
    return _histo.read_dist_histogram_logform();
}

template<
  class duration_t,
  int number_of_buckets,
  uint64_t first_bucket_upper_bound>
requires detail::is_duration_v<duration_t>
seastar::metrics::histogram
latency_log_hist<duration_t, number_of_buckets, first_bucket_upper_bound>::
  client_quota_histogram_logform() const {
    return _histo.client_quota_histogram_logform();
}

// Explicit instantiation for log_hist_public
template class latency_log_hist<std::chrono::microseconds, 18, 256ul>;
// Explicit instantiation for log_hist_internal
template class latency_log_hist<std::chrono::microseconds, 26, 8ul>;
// Explicit instantiation for log_hist_read_dist
template class latency_log_hist<std::chrono::minutes, 16, 4ul>;
// Explicit instantiation for log_hist_client_quota
template class latency_log_hist<std::chrono::milliseconds, 15, 1ul>;
