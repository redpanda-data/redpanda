#pragma once

#include "datalake/data_writer_interface.h"
#include "iceberg/datatypes.h"
#include "serde/parquet/writer.h"

namespace datalake {
class serde_parquet_writer : public parquet_ostream {
public:
    explicit serde_parquet_writer(serde::parquet::writer writer)
      : _writer(std::move(writer)) {}
    ss::future<writer_error>
      add_data_struct(iceberg::struct_value, size_t) final;

    ss::future<writer_error> finish() final;

private:
    serde::parquet::writer _writer;
};

class serde_parquet_writer_factory : public parquet_ostream_factory {
public:
    ss::future<std::unique_ptr<parquet_ostream>>
    create_writer(const iceberg::struct_type&, ss::output_stream<char>) final;
};

} // namespace datalake
