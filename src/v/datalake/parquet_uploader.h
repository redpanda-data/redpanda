#pragma once

#include "archival/archival_policy.h"
#include "datalake/arrow_writing_consumer.h"
#include "datalake/configuration.h"
#include "storage/segment.h"

namespace datalake {

// /** High-level interface to write a log segment out as Parquet and upload it.
//  */
// ss::future<bool> upload_parquet(
//   std::filesystem::path path,
//   ss::shared_ptr<storage::log> log,
//   archival::upload_candidate candidate,
//   const cloud_storage_clients::bucket_name& bucket_name,
//   cloud_storage::remote& remote,
//   retry_chain_node& rtc,
//   retry_chain_logger& logger);

// ss::future<bool> write_parquet(
//   const std::filesystem::path inner_path,
//   ss::shared_ptr<storage::log> log,
//   model::offset starting_offset,
//   model::offset ending_offset);

// /** Low-level wrapper for writing an arrow table to parquet*/
// arrow::Status write_table_to_parquet(
//   std::shared_ptr<arrow::Table>, std::filesystem::path path);

// ss::future<cloud_storage::upload_result> put_parquet_file(
//   const cloud_storage_clients::bucket_name& bucket,
//   const std::string_view topic_name,
//   const std::filesystem::path& inner_path,
//   cloud_storage::remote& remote,
//   retry_chain_node& rtc,
//   retry_chain_logger& logger);

class parquet_uploader {
public:
    explicit parquet_uploader(ss::shared_ptr<storage::log> log)
      : _log(log) {}
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

    ss::future<bool> write_parquet_locally(
      const std::filesystem::path inner_path,
      model::offset starting_offset,
      model::offset ending_offset);

    /** Low-level wrapper for writing an arrow table to parquet*/
    arrow::Status write_table_to_parquet(
      std::shared_ptr<arrow::Table>, std::filesystem::path path);

    ss::future<cloud_storage::upload_result> put_parquet_file(
      const cloud_storage_clients::bucket_name& bucket,
      const std::string_view topic_name,
      const std::filesystem::path& inner_path,
      cloud_storage::remote& remote,
      retry_chain_node& rtc,
      retry_chain_logger& logger);

private:
    ss::shared_ptr<storage::log> _log;
};

} // namespace datalake
