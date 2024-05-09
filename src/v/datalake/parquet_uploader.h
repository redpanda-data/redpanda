#pragma once

#include "archival/archival_policy.h"
#include "datalake/arrow_writing_consumer.h"
#include "datalake/configuration.h"
#include "datalake/schema_registry_interface.h"
#include "storage/segment.h"

#include <seastar/core/shared_ptr.hh>

namespace datalake {

class parquet_uploader {
public:
    explicit parquet_uploader(
      ss::shared_ptr<storage::log> log,
      ss::shared_ptr<schema_registry_interface> schema_registry)
      : _log(log)
      , _schema_registry(schema_registry) {}
    /** High-level interface to write a log segment out as Parquet and upload
     * it.
     */
    ss::future<bool> upload_parquet(
      std::filesystem::path path,
      archival::upload_candidate candidate,
      const cloud_storage_clients::bucket_name& bucket_name,
      cloud_storage::remote& remote,
      retry_chain_node& rtc,
      retry_chain_logger& logger);

private:
    ss::future<bool> write_parquet_locally(
      const std::filesystem::path inner_path,
      model::offset starting_offset,
      model::offset ending_offset);

    ss::future<cloud_storage::upload_result> put_parquet_file(
      const cloud_storage_clients::bucket_name& bucket,
      const std::string_view topic_name,
      const std::filesystem::path& inner_path,
      cloud_storage::remote& remote,
      retry_chain_node& rtc,
      retry_chain_logger& logger);

    ss::shared_ptr<storage::log> _log;
    ss::shared_ptr<schema_registry_interface> _schema_registry;
};

} // namespace datalake
