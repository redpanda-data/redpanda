/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "base/seastarx.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/util.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "test_utils/boost_fixture.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <ranges>

using namespace cloud_storage;

static const remote_path_provider path_provider(std::nullopt, std::nullopt);

namespace {

ss::logger cost_log("timequery_cost"); // NOLINT

/// Timestamps are split into two eras to model a backfilled topic: the data
/// carries "historical" timestamps while the non-data batches archival
/// generates carry walltime.
constexpr int64_t data_era_base = 1'000'000;
constexpr int64_t data_era_step = 10;
constexpr int64_t walltime_era = 9'000'000;

/// A query above every data timestamp but below walltime. On an accurate
/// manifest this is provably unanswerable and should ideally cost nothing.
constexpr int64_t query_ts = 5'000'000;

constexpr int num_segments = 40;
constexpr int data_batches_per_segment = 8;

batch_t data_batch(int64_t ts) {
    return batch_t{
      .num_records = 1,
      .type = model::record_batch_type::raft_data,
      .timestamp = model::timestamp(ts)};
}

batch_t archival_batch(int64_t ts) {
    return batch_t{
      .num_records = 1,
      .type = model::record_batch_type::archival_metadata,
      .timestamp = model::timestamp(ts)};
}

/// Build the batch layout. Every `inflate_every`-th segment ends on a
/// walltime-stamped archival_metadata batch, which is what makes the manifest's
/// max_timestamp for that segment walltime.
std::vector<std::vector<batch_t>> make_layout(int inflate_every) {
    std::vector<std::vector<batch_t>> layout;
    int64_t ts = data_era_base;
    for (int s = 0; s < num_segments; s++) {
        std::vector<batch_t> segment;
        for (int b = 0; b < data_batches_per_segment; b++) {
            segment.push_back(data_batch(ts));
            ts += data_era_step;
        }
        const bool inflated
          = inflate_every > 0
            && ((s % inflate_every) == 0 || s == num_segments - 1);
        segment.push_back(archival_batch(inflated ? walltime_era : ts));
        ts += data_era_step;
        layout.push_back(std::move(segment));
    }
    return layout;
}

/// Number of segments in `layout` whose manifest max_timestamp ends up at or
/// above the query, i.e. the candidates the manifest cannot rule out.
size_t count_inflated(const std::vector<std::vector<batch_t>>& layout) {
    return std::ranges::count_if(layout, [](const auto& segment) {
        return segment.back().timestamp.value_or(model::timestamp::missing())
               >= model::timestamp(query_ts);
    });
}

/// The maximum timestamp over the segment's raft_data batches only.
model::timestamp data_only_max(const std::vector<batch_t>& segment) {
    model::timestamp max = model::timestamp::missing();
    for (const auto& b : segment) {
        if (
          b.type == model::record_batch_type::raft_data
          && b.timestamp.has_value()
          && (max == model::timestamp::missing() || b.timestamp.value() > max)) {
            max = b.timestamp.value();
        }
    }
    return max;
}

struct timequery_cost {
    size_t segment_gets{0};
    size_t index_gets{0};
    uint64_t bytes_parsed{0};
    std::chrono::microseconds elapsed{0};
    bool found{false};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "segment GETs: {:>3}, index GETs: {:>3}, bytes parsed: {:>8}, "
          "elapsed: {:>8}us, found: {}",
          segment_gets,
          index_gets,
          bytes_parsed,
          elapsed.count(),
          found);
    }
};

bool is_index_get(const http_test_utils::request_info& r) {
    return r.method == "GET" && r.url.find(".index") != ss::sstring::npos;
}

bool is_segment_get(const http_test_utils::request_info& r) {
    return r.method == "GET" && !is_index_get(r)
           && r.url.find(".log") != ss::sstring::npos;
}

timequery_cost
measure_timequery(cloud_storage_fixture& fixture, model::timestamp ts) {
    ss::lowres_clock::update();

    auto manifest = hydrate_manifest(fixture.api.local(), fixture.bucket_name);

    partition_probe probe(manifest.get_ntp());
    auto manifest_view = ss::make_shared<async_manifest_view>(
      fixture.api, fixture.cache, manifest, fixture.bucket_name, path_provider);
    auto manifest_view_stop = ss::defer(
      [&manifest_view] { manifest_view->stop().get(); });
    manifest_view->start().get();

    auto partition = ss::make_shared<remote_partition>(
      manifest_view,
      fixture.api.local(),
      fixture.cache.local(),
      fixture.bucket_name,
      probe);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });
    partition->start().get();

    const size_t reqs_before = fixture.get_requests().size();
    const auto skip_before = probe.get_bytes_skip();
    const auto accept_before = probe.get_bytes_accept();

    storage::timequery_config cfg(
      model::offset(0),
      ts,
      model::offset::max(),
      model::record_batch_type::raft_data);

    const auto start = std::chrono::steady_clock::now();
    auto result = partition->timequery(cfg).get();
    const auto end = std::chrono::steady_clock::now();

    timequery_cost cost;
    cost.elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
      end - start);
    cost.found = result.has_value();
    cost.bytes_parsed = (probe.get_bytes_skip() - skip_before)
                        + (probe.get_bytes_accept() - accept_before);

    const auto& reqs = fixture.get_requests();
    for (size_t i = reqs_before; i < reqs.size(); i++) {
        const auto& r = reqs[i];
        if (is_index_get(r)) {
            cost.index_gets++;
        } else if (is_segment_get(r)) {
            cost.segment_gets++;
        }
    }
    return cost;
}

/// Publish `layout` to the imposter. When `data_only_manifest` is set the
/// per-segment max_timestamp written into the manifest is the maximum over
/// raft_data batches only, modelling an accurate manifest, otherwise it is
/// the last batch's timestamp, which is what the upload path historically
/// produced.
void setup(
  cloud_storage_fixture& fixture,
  const std::vector<std::vector<batch_t>>& layout,
  bool data_only_manifest) {
    auto segments = make_segments(layout, model::offset(0));
    if (data_only_manifest) {
        BOOST_REQUIRE_EQUAL(segments.size(), layout.size());
        for (size_t i = 0; i < segments.size(); i++) {
            segments[i].last_timestamp = data_only_max(layout[i]);
        }
    }
    partition_manifest manifest(manifest_ntp, manifest_revision);
    auto expectations = make_imposter_expectations(
      manifest, segments, false, model::offset_delta(0));
    fixture.set_expectations_and_listen(std::move(expectations));
}

void report(const ss::sstring& label, const timequery_cost& cost) {
    vlog(cost_log.info, "{:<34} {}", label, cost);
}

} // namespace

FIXTURE_TEST(timequery_cost_all_segments_inflated, cloud_storage_fixture) {
    // Every segment ends on a walltime archival_metadata batch, so every
    // segment's manifest max_timestamp is walltime while all of its data is
    // historical. The query sits between the two eras.
    setup(
      *this, make_layout(/*inflate_every=*/1), /*data_only_manifest=*/false);

    auto cost = measure_timequery(*this, model::timestamp(query_ts));
    report("all_segments_inflated", cost);

    // No data batch anywhere can match, so the query is unanswerable.
    BOOST_REQUIRE(!cost.found);

    vlog(
      cost_log.info,
      "segments in log: {}, segment GETs per timequery: {}",
      num_segments,
      cost.segment_gets);
}

FIXTURE_TEST(timequery_cost_data_only_manifest, cloud_storage_fixture) {
    // Control: identical data, but the manifest carries data-only
    // max_timestamps. The query is provably unanswerable from the manifest
    // alone, so it must not touch a single segment.
    setup(*this, make_layout(/*inflate_every=*/1), /*data_only_manifest=*/true);

    auto cost = measure_timequery(*this, model::timestamp(query_ts));
    report("data_only_manifest", cost);

    BOOST_REQUIRE(!cost.found);
    BOOST_REQUIRE_EQUAL(cost.segment_gets, 0);
}

FIXTURE_TEST(timequery_cost_every_fourth_inflated, cloud_storage_fixture) {
    // A realistic mix: only every fourth upload range happens to end on a
    // non-data batch. The other three quarters carry honest, historical
    // max_timestamps and the manifest alone proves they cannot match, so a
    // forward walk that consults the manifest should skip them.
    auto layout = make_layout(/*inflate_every=*/4);
    setup(*this, layout, /*data_only_manifest=*/false);

    auto cost = measure_timequery(*this, model::timestamp(query_ts));
    report("every_fourth_inflated", cost);

    BOOST_REQUIRE(!cost.found);

    // The walk must not hydrate a segment the manifest already rules out, so
    // the cost is bounded by the number of candidates rather than by the
    // length of the log.
    BOOST_REQUIRE_LE(cost.segment_gets, count_inflated(layout));

    vlog(
      cost_log.info,
      "segments in log: {}, manifest candidates: {}, segment GETs per "
      "timequery: {}",
      num_segments,
      count_inflated(layout),
      cost.segment_gets);
}

FIXTURE_TEST(timequery_cost_hit_in_last_segment, cloud_storage_fixture) {
    // Answerable query whose match lives in the final segment: the floor for
    // how much work a successful timequery has to do.
    auto layout = make_layout(/*inflate_every=*/0);
    const auto last_data = data_only_max(layout.back());
    setup(*this, layout, /*data_only_manifest=*/true);

    auto cost = measure_timequery(*this, last_data);
    report("hit_in_last_segment", cost);

    BOOST_REQUIRE(cost.found);
}
