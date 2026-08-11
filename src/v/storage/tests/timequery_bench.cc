// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "base/units.h"
#include "base/vassert.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/test_env.h"

#include <seastar/core/future.hh>
#include <seastar/testing/perf_tests.hh>

namespace {

constexpr size_t batch_payload_bytes = 32_KiB;
constexpr int data_batch_count = 512;
constexpr int64_t batch_ts_interval_ms = 1000;

// Large enough that the data never rolls into a second segment.
constexpr size_t bench_segment_size = 1_GiB;

// A backfill sits far enough in the past that a walltime-stamped batch is
// always above every data timestamp.
constexpr int64_t backfill_age_ms = 30LL * 24 * 60 * 60 * 1000;

storage::log_config bench_log_config() {
    return {
      test_env::random_dir_path(),
      bench_segment_size,
      storage::with_cache::no,
      std::nullopt};
}

model::record_batch make_batch(
  model::offset o, model::timestamp ts, model::record_batch_type type) {
    auto b = model::test::make_random_batch(
      o,
      1,
      /*allow_compression=*/false,
      type,
      std::vector<size_t>(1, batch_payload_bytes),
      ts);
    b.set_term(model::term_id(0));
    return b;
}

} // namespace

class timequery_bench {
public:
    timequery_bench(const timequery_bench&) = delete;
    timequery_bench& operator=(const timequery_bench&) = delete;
    timequery_bench(timequery_bench&&) = delete;
    timequery_bench& operator=(timequery_bench&&) = delete;

    explicit timequery_bench(
      bool with_non_data_batch,
      bool disordered = false,
      bool interleave_non_data = false)
      : _disordered(disordered)
      , _interleave_non_data(interleave_non_data)
      , _builder(bench_log_config()) {
        build(with_non_data_batch).get();
    }

    ~timequery_bench() { _builder.stop().get(); }

    ss::future<size_t> query() {
        auto result = co_await _builder.get_log()->timequery(
          storage::timequery_config(
            model::offset(0),
            _query_ts,
            model::offset::max(),
            {model::record_batch_type::raft_data},
            std::nullopt));
        vassert(
          result.has_value() && result->offset == _answer,
          "expected timequery to answer {}, got {}",
          _answer,
          result);
        co_return static_cast<size_t>(result->offset());
    }

private:
    ss::future<> build(bool with_non_data_batch) {
        co_await _builder.start(
          model::ntp(
            model::kafka_namespace,
            model::topic("timequery-bench"),
            model::partition_id(0)));
        co_await _builder.add_segment(model::offset(0));

        model::offset offset{0};
        auto ts = _base_ts;

        co_await _builder.add_batch(
          make_batch(offset, ts, model::record_batch_type::raft_data));
        if (ts >= _query_ts) {
            _answer = offset;
        }
        offset += model::offset(1);

        if (with_non_data_batch) {
            co_await _builder.add_batch(make_batch(
              offset,
              model::new_timestamp(),
              model::record_batch_type::raft_configuration));
            offset += model::offset(1);
        }

        for (int i = 1; i < data_batch_count; ++i) {
            ts = model::timestamp(ts() + batch_ts_interval_ms);
            auto batch_ts = ts;
            if (_disordered && i % 2 == 0) {
                batch_ts = model::timestamp(ts() - 2 * batch_ts_interval_ms);
            }
            if (_interleave_non_data) {
                // Stands in for the archival_metadata and raft_configuration
                // batches that land continuously in a real partition log. The
                // query filters for raft_data, so every one of these is skipped
                // by the reader on the way to the answer.
                co_await _builder.add_batch(make_batch(
                  offset,
                  model::new_timestamp(),
                  model::record_batch_type::archival_metadata));
                offset += model::offset(1);
            }

            co_await _builder.add_batch(make_batch(
              offset, batch_ts, model::record_batch_type::raft_data));
            if (_answer == model::offset{} && batch_ts >= _query_ts) {
                _answer = offset;
            }
            offset += model::offset(1);
        }

        vassert(
          _answer != model::offset{},
          "query timestamp must fall inside the generated data");
        vassert(
          _builder.get_log_segments().size() == 1,
          "data must land in a single segment, got {}",
          _builder.get_log_segments().size());

        const auto& index = _builder.get_log_segments().back()->index();
        fmt::print(
          "setup: non_data_batch={} interleaved={} disordered={} monotonic={} "
          "answer={}\n",
          with_non_data_batch,
          _interleave_non_data,
          _disordered,
          index.batch_timestamps_are_monotonic(),
          _answer);
    }

    const bool _disordered{false};
    const bool _interleave_non_data{false};
    storage::disk_log_builder _builder;

    const model::timestamp _base_ts{model::new_timestamp()() - backfill_age_ms};

    // Near the end of the data, so the scan traverses almost the whole segment.
    const model::timestamp _query_ts{
      _base_ts()
      + (data_batch_count - data_batch_count / 20) * batch_ts_interval_ms};

    model::offset _answer{};
};

struct only_data_timequery : timequery_bench {
    only_data_timequery()
      : timequery_bench(/*with_non_data_batch=*/false) {}
};

struct non_data_timequery : timequery_bench {
    non_data_timequery()
      : timequery_bench(/*with_non_data_batch=*/true) {}
};

// Non-monotonic case
struct disordered_timequery : timequery_bench {
    disordered_timequery()
      : timequery_bench(/*with_non_data_batch=*/false, /*disordered=*/true) {}
};

struct interleaved_timequery : timequery_bench {
    interleaved_timequery()
      : timequery_bench(
          /*with_non_data_batch=*/false,
          /*disordered=*/false,
          /*interleave_non_data=*/true) {}
};

PERF_TEST_CN(only_data_timequery, seek) {
    auto o = co_await query();
    perf_tests::do_not_optimize(o);
    co_return 1;
}

PERF_TEST_CN(non_data_timequery, seek) {
    auto o = co_await query();
    perf_tests::do_not_optimize(o);
    co_return 1;
}

PERF_TEST_CN(disordered_timequery, seek) {
    auto o = co_await query();
    perf_tests::do_not_optimize(o);
    co_return 1;
}

PERF_TEST_CN(interleaved_timequery, seek) {
    auto o = co_await query();
    perf_tests::do_not_optimize(o);
    co_return 1;
}
