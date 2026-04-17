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

#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/partitioning_writer.h"
#include "datalake/record_multiplexer.h"
#include "iceberg/manifest_io.h"

#include <seastar/core/future.hh>

namespace datalake::tests {

/// \brief Upload multiplexer output to S3 and convert to coordinator state.
///
/// Reads each local parquet file, uploads it to the mock S3 at the
/// correct remote path (base/partition/filename), and converts the
/// partitioned_file entries to coordinator data_file entries.
///
/// Returns a translated_offset_range ready for insertion into
/// topics_state and committing via iceberg_file_committer.
ss::future<coordinator::translated_offset_range> upload_and_convert(
  iceberg::manifest_io& io,
  const record_multiplexer::write_result& result,
  const record_multiplexer::finished_files& files);

} // namespace datalake::tests
