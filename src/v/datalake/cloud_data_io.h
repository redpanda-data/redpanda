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
#include "container/fragmented_vector.h"
#include "datalake/base_types.h"
#include "model/fundamental.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"

namespace cloud_io {
class remote;
}
namespace datalake {
class cloud_data_io {
public:
    enum class errc { file_io_error, cloud_op_error, cloud_op_timeout };

    cloud_data_io(
      cloud_io::remote& io, cloud_storage_clients::bucket_name bucket)
      : _cloud_io{&io}
      , _bucket{std::move(bucket)} {}

    /**
     * Simple interface to upload a data file from the path indicated by local
     * file metadata to remote path
     */
    ss::future<checked<std::nullopt_t, errc>> upload_data_file(
      const local_file_metadata& local_file,
      const remote_path& remote_path,
      retry_chain_node& rtc_parent,
      lazy_abort_source& lazy_abort_source);

    ss::future<checked<std::nullopt_t, errc>> delete_data_files(
      chunked_vector<remote_path> files_to_delete,
      retry_chain_node& rtc_parent);

    friend std::ostream& operator<<(std::ostream&, errc);

private:
    cloud_io::remote* _cloud_io;
    cloud_storage_clients::bucket_name _bucket;
};
} // namespace datalake
