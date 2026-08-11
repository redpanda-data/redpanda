#pragma once

#include "datalake/data_writer_interface.h"
#include "iceberg/datatypes.h"
#include "serde/parquet/writer.h"

namespace datalake {
class serde_parquet_writer : public parquet_ostream {
public:
    explicit serde_parquet_writer(
      serde::parquet::writer writer, writer_mem_tracker& mem_tracker)
      : _writer(std::move(writer))
      , _mem_tracker(mem_tracker) {}
    ss::future<writer_error>
    add_data_struct(iceberg::struct_value, size_t, ss::abort_source&) final;

    size_t buffered_bytes() const final;

    size_t flushed_bytes() const final;

    ss::future<> flush() final;

    ss::future<writer_error> finish() final;

private:
    serde::parquet::writer _writer;
    writer_mem_tracker& _mem_tracker;
    int64_t _buffered_bytes{0};
    int64_t _flushed_bytes{0};
    // Used to store any errors that occur after a row write is successful.
    writer_error _error{writer_error::ok};
    writer_error set_error(writer_error);
};

class serde_parquet_writer_factory : public parquet_ostream_factory {
public:
    ss::future<std::unique_ptr<parquet_ostream>> create_writer(
      const iceberg::struct_type&,
      ss::output_stream<char>,
      writer_mem_tracker&) final;
};

} // namespace datalake
