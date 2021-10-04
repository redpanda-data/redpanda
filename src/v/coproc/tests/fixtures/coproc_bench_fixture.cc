/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/fixtures/coproc_bench_fixture.h"

coproc_bench_fixture::router_test_plan::all_opts
coproc_bench_fixture::build_simple_opts(log_layout_map data, std::size_t rate) {
    using rtp = coproc_bench_fixture::router_test_plan;
    rtp::all_opts results;
    rtp::options rate_opts = {.number_of_batches = rate, .number_of_pushes = 1};
    for (auto& [topic, n_partitions] : data) {
        for (decltype(n_partitions) i = 0; i < n_partitions; ++i) {
            model::ntp ntp(
              model::kafka_namespace, topic, model::partition_id(i));
            results.emplace(std::move(ntp), rate_opts);
        }
    }
    return results;
}

ss::future<std::tuple<
  coproc_bench_fixture::push_results,
  coproc_bench_fixture::drain_results>>
coproc_bench_fixture::start_benchmark(router_test_plan plan) {
    return send_all<push_action_tag>(std::move(plan.input))
      .then([this, output = std::move(plan.output)](push_results prs) mutable {
          return send_all<drain_action_tag>(std::move(output))
            .then([prs = std::move(prs)](drain_results drs) mutable {
                return std::make_tuple(std::move(prs), std::move(drs));
            });
      });
}

template<typename ActionTag>
ss::future<coproc_bench_fixture::action_results<ActionTag>>
coproc_bench_fixture::send_all(router_test_plan::all_opts options) {
    using ret_t = action_results<ActionTag>;
    return ss::do_with(
      ret_t(),
      std::move(options),
      [this](ret_t& rt, router_test_plan::all_opts& opts) mutable {
          return ss::parallel_for_each(
                   opts,
                   [this, &rt](router_test_plan::all_opts::value_type& vt) {
                       return send_n_times<ActionTag>(vt.first, vt.second)
                         .then([ntp = vt.first, &rt](auto value) {
                             rt.emplace(ntp, value);
                         });
                   })
            .then([&rt] { return std::move(rt); });
      });
}

template<typename T>
auto initial_mapped_value() {
    if constexpr (std::is_same_v<T, model::offset>) {
        return model::offset(0);
    } else {
        return std::make_pair(model::offset(0), std::size_t(0));
    }
}

template<typename ActionTag, typename ResultType>
ss::future<ResultType> coproc_bench_fixture::send_n_times(
  const model::ntp& ntp, router_test_plan::options opts) {
    auto r = boost::irange<std::size_t>(0, opts.number_of_pushes);
    return ss::do_with(
      initial_mapped_value<ResultType>(),
      r,
      [this,
       n_pushes = opts.number_of_pushes,
       n_batches = opts.number_of_batches,
       ntp](ResultType& rt, auto& r) {
          /// Within the context of pushing or draining from a single log,
          /// must use do_for_each
          const auto total_expected_batches = n_pushes * n_batches;
          return ss::do_for_each(
                   r,
                   [this, &rt, total_expected_batches, ntp, n_batches](
                     std::size_t) {
                       return do_action<ActionTag>(
                         ntp, n_batches, total_expected_batches, rt);
                   })
            .then([&rt] { return std::move(rt); });
      });
}

template<typename ActionTag, typename ResultType>
ss::future<> coproc_bench_fixture::do_action(
  const model::ntp& ntp,
  std::size_t n_batches,
  std::size_t total_expected_batches,
  ResultType& rt) {
    /// whats this template hackery? To reduce code duplication I chose this
    /// static method of applying either push or drain functionality. All of
    /// the above source is the same no mater which option is selected
    if constexpr (std::is_same_v<ActionTag, push_action_tag>) {
        return push(
                 ntp,
                 storage::test::make_random_memory_record_batch_reader(
                   rt, n_batches, 1))
          .then([ntp, &rt](model::offset offset) { rt = offset++; });
    } else {
        if (rt.second >= total_expected_batches) {
            /// All records have been read, exit out
            return ss::now();
        }
        return drain(
                 ntp, n_batches, rt.first, model::timeout_clock::now() + 1min)
          .then(
            [ntp, &rt](
              std::optional<model::record_batch_reader::data_t> maybe_data) {
                if (maybe_data && !maybe_data->empty()) {
                    rt.first = ++maybe_data->back().last_offset();
                    rt.second += maybe_data->size();
                }
            });
    }
}
