/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/remote.h"
#include "cloud_storage/types.h"
#include "datalake/coordinator/data_file.h"
#include "datalake/data_writer_interface.h"

#include <memory>
namespace datalake {
class cloud_uploader {
public:
    cloud_uploader(
      cloud_io::remote& io,
      cloud_storage_clients::bucket_name bucket,
      std::filesystem::path remote_directory)
      : _cloud_io{io}
      , _bucket{std::move(bucket)}
      , _remote_directory{std::move(remote_directory)} {}

    ss::future<result<coordinator::data_file, data_writer_error>>
    upload_data_file(
      datalake::local_data_file local_file,
      std::filesystem::path remote_filename,
      retry_chain_node& rtc_parent,
      lazy_abort_source& lazy_abort_source);

private:
    cloud_io::remote& _cloud_io;
    cloud_storage_clients::bucket_name _bucket;
    std::filesystem::path _remote_directory;
};
} // namespace datalake
