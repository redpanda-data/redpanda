#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/log.h"
#include "storage/parser_utils.h"

#include <seastar/core/shared_ptr.hh>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/writer.h>
#include <ssx/thread_worker.h>

namespace datalake {
class arrow_writing_consumer {
    /** Consumes logs and writes the results out to a Parquet file.
     *
     * Uses an arrow ChunkedArray with one chunk per batch to avoid large
     * allocations. However, it does hold all the data in RAM until the file
     * is written.
     */
public:
    explicit arrow_writing_consumer(std::filesystem::path local_file_path);
    ss::future<ss::stop_iteration> operator()(model::record_batch batch);
    ss::future<arrow::Status> end_of_stream();
    std::shared_ptr<arrow::Table> get_table();

    bool write_row_group();

    arrow::Status status() { return _ok; }

private:
    std::filesystem::path _local_file_path;

    uint32_t iobuf_to_uint32(const iobuf& buf);
    std::string iobuf_to_string(const iobuf& buf);

    uint32_t _compressed_batches = 0;
    uint32_t _uncompressed_batches = 0;
    uint32_t _rows = 0;
    uint64_t _current_rows = 0;
    arrow::Status _ok = arrow::Status::OK();
    std::shared_ptr<arrow::Field> _field_key, _field_value, _field_timestamp;
    std::shared_ptr<arrow::Schema> _schema;
    arrow::ArrayVector _key_vector;
    arrow::ArrayVector _value_vector;
    arrow::ArrayVector _timestamp_vector;

    // File writing
    std::unique_ptr<parquet::arrow::FileWriter> _file_writer;
};

} // namespace datalake
