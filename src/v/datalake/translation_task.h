/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "datalake/cloud_data_io.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/data_writer_interface.h"
#include "datalake/fwd.h"
#include "model/record_batch_reader.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"

namespace datalake {
/**
 * An abstraction representing a task of consuming data, translating them to
 * parquet and uploading to the object store
 */
class translation_task {
public:
    explicit translation_task(cloud_data_io& uploader);
    enum class errc {
        file_io_error,
        cloud_io_error,
    };
    /**
     * Executes the translation and uploads files to the object store. The tasks
     * accepts an abort source indicating when the upload retires should be
     * stopped and a root retry chain node.
     */
    ss::future<checked<coordinator::translated_offset_range, errc>> translate(
      std::unique_ptr<data_writer_factory> writer_factory,
      model::record_batch_reader reader,
      const remote_path& remote_path_prefix,
      retry_chain_node& parent_rcn,
      lazy_abort_source& lazy_as);

private:
    friend std::ostream& operator<<(std::ostream&, errc);

    ss::future<checked<remote_path, errc>> execute_single_upload(
      const local_file_metadata& lf_meta,
      const remote_path& remote_path_prefix,
      retry_chain_node& parent_rcn,
      lazy_abort_source& lazy_as);

    ss::future<errc> delete_remote_files(
      chunked_vector<remote_path>, retry_chain_node& parent_rcn);

    ss::future<checked<std::nullopt_t, errc>>
    delete_local_data_files(const chunked_vector<local_file_metadata>&);

    static constexpr std::chrono::milliseconds _read_timeout{30000};
    cloud_data_io* _cloud_io;
};
} // namespace datalake
