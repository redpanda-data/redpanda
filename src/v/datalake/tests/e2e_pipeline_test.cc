/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/tests/datalake_pipeline_test_fixture.h"
#include "datalake/tests/record_batch_factory.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "serde/parquet/value.h"

#include <gtest/gtest.h>

using namespace datalake::tests;
namespace sp = serde::parquet;

class DatalakePipelineTest : public datalake_pipeline_test_fixture {};

namespace {

/// Helper: run records through the full pipeline and return surviving rows.
chunked_vector<sp::group_value> run_pipeline(
  DatalakePipelineTest& t, const model::ntp& ntp, model::record_batch batch) {
    ss::abort_source as;
    auto mux = t.make_multiplexer(
      ntp, model::revision_id{1}, model::iceberg_mode::cdc_key_value);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    mux.multiplex(std::move(reader), kafka::offset{0}, model::no_timeout, as)
      .get();
    datalake::record_multiplexer::finished_files files;
    auto result = std::move(mux).finish(files).get();
    if (result.has_error()) {
        throw std::runtime_error("Multiplex failed");
    }
    return t.commit_and_read(ntp.tp.topic, result.value(), std::move(files))
      .get();
}

} // namespace

// Ghost row: insert then tombstone the same key in a single batch.
// Without the position delete fix, the tombstoned row survives.
TEST_F(DatalakePipelineTest, CdcKeyValueRapidToggle) {
    const model::ntp ntp(
      model::ns{"kafka"}, model::topic{"cdc-toggle"}, model::partition_id{0});

    record_batch_factory bf;
    bf.add_record("k1", "alice");
    bf.add_record("doomed", "die");
    bf.add_tombstone("doomed");

    auto rows = run_pipeline(*this, ntp, bf.build(model::offset{0}));
    EXPECT_EQ(rows.size(), 1) << "Only k1 should survive the tombstone";
}

// Duplicate row: insert k1=v1 then overwrite k1=v2 in a single batch.
// The equality delete for k1 from the second insert should remove v1.
// Without the position delete fix, both v1 and v2 survive.
TEST_F(DatalakePipelineTest, CdcKeyValueUpdate) {
    const model::ntp ntp(
      model::ns{"kafka"}, model::topic{"cdc-update"}, model::partition_id{0});

    record_batch_factory bf;
    bf.add_record("k1", "v1");
    bf.add_record("k1", "v2");

    auto rows = run_pipeline(*this, ntp, bf.build(model::offset{0}));
    EXPECT_EQ(rows.size(), 1) << "Only the latest value should survive";
}
