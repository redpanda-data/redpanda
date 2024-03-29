#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/log.h"
#include "storage/parser_utils.h"

#include <seastar/core/shared_ptr.hh>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/writer.h>
#include <ssx/thread_worker.h>

namespace datalake {

/** High-level interface to write a log segment out as Parquet.
 */
ss::future<bool> write_parquet(
  const std::filesystem::path inner_path,
  ss::shared_ptr<storage::log> log,
  model::offset starting_offset,
  model::offset ending_offset);

/** Is this a datalake topic? Should we write it to Iceberg/Parquet?
 */
bool is_datalake_topic(cluster::partition& partition);

ss::future<cloud_storage::upload_result> put_parquet_file(
  const cloud_storage_clients::bucket_name& bucket,
  const std::string_view topic_name,
  const std::filesystem::path& inner_path,
  cloud_storage::remote& remote,
  retry_chain_node& rtc,
  retry_chain_logger& logger);

class arrow_writing_consumer {
    /** Consumes logs and writes the results out to a Parquet file.
     *
     * Uses an arrow ChunkedArray with one chunk per batch to avoid large
     * allocations. However, it does hold all the data in RAM until the file
     * is written.
     */
public:
    explicit arrow_writing_consumer(std::filesystem::path file_name);
    ss::future<ss::stop_iteration> operator()(model::record_batch batch);
    arrow::Status write_file();
    ss::future<arrow::Status> end_of_stream();

private:
    uint32_t iobuf_to_uint32(const iobuf& buf);
    std::string iobuf_to_string(const iobuf& buf);

    std::filesystem::path _local_file_name;
    uint32_t _compressed_batches = 0;
    uint32_t _uncompressed_batches = 0;
    uint32_t _rows = 0;
    arrow::Status _ok = arrow::Status::OK();
    std::shared_ptr<arrow::Field> _field_key, _field_value;
    std::shared_ptr<arrow::Schema> _schema;
    arrow::ArrayVector _key_vector;
    arrow::ArrayVector _value_vector;
};

} // namespace datalake
