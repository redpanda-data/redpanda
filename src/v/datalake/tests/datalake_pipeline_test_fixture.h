/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "datalake/catalog_schema_manager.h"
#include "datalake/cdc_key_value_translator.h"
#include "datalake/coordinator/iceberg_file_committer.h"
#include "datalake/coordinator/state.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/test_data_writer.h"
#include "datalake/tests/test_utils.h"
#include "datalake/translation/translation_probe.h"
#include "features/feature_table.h"
#include "iceberg/manifest_io.h"
#include "serde/parquet/value.h"
#include "storage/api.h"

#include <gtest/gtest.h>

namespace datalake::tests {

/// \brief End-to-end test fixture for the datalake pipeline.
///
/// Provides: produce record batches → record_multiplexer → coordinator
/// state → iceberg_file_committer → iceberg reader readback.
///
/// Test authors pick the iceberg mode; the fixture creates the right
/// resolver + translator.
class datalake_pipeline_test_fixture
  : public catalog_and_registry_fixture
  , public ::testing::Test {
public:
    datalake_pipeline_test_fixture();
    void TearDown() override;

    /// Create a record_multiplexer configured for the given mode.
    record_multiplexer make_multiplexer(
      const model::ntp& ntp, model::revision_id rev, model::iceberg_mode mode);

    /// Multiplex → upload → convert → commit → readback, all in one.
    /// Returns the surviving records after spec-correct delete filtering.
    ss::future<chunked_vector<serde::parquet::group_value>> commit_and_read(
      const model::topic& topic,
      record_multiplexer::write_result result,
      record_multiplexer::finished_files files);

    /// After multiplex + finish: upload files to S3, convert to
    /// coordinator state, and commit.
    ss::future<checked<
      chunked_vector<coordinator::mark_files_committed_update>,
      coordinator::file_committer::errc>>
    commit_finished_files(
      const model::topic& topic,
      const record_multiplexer::write_result& result,
      const record_multiplexer::finished_files& files);

    /// Read back all surviving records from a committed table with
    /// spec-correct sequence number delete filtering.
    ss::future<chunked_vector<serde::parquet::group_value>>
    read_committed_table(const model::topic& topic);

    iceberg::manifest_io& io() { return manifest_io_; }
    catalog_schema_manager& schema_mgr() { return schema_mgr_; }

private:
    ss::sharded<features::feature_table> feature_table_;
    storage::api storage_;
    catalog_schema_manager schema_mgr_;
    iceberg::manifest_io manifest_io_;
    coordinator::iceberg_file_committer committer_;
    noop_mem_tracker mem_tracker_;

    // Per-multiplexer state. Recreated on each make_multiplexer() call.
    binary_type_resolver bin_resolver_;
    key_value_translator kv_translator_;
    cdc_key_value_translator cdc_kv_translator_;
    std::unique_ptr<direct_table_creator> table_creator_;
    std::unique_ptr<translation_probe> probe_;
};

} // namespace datalake::tests
