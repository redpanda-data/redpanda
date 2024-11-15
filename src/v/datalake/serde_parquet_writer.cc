#include "datalake/serde_parquet_writer.h"

#include "base/vlog.h"
#include "datalake/logger.h"
#include "datalake/schema_parquet.h"
#include "datalake/values_parquet.h"

namespace datalake {

ss::future<writer_error>
serde_parquet_writer::add_data_struct(iceberg::struct_value value, size_t) {
    auto conversion_result = co_await to_parquet_value(
      std::make_unique<iceberg::struct_value>(std::move(value)));
    if (conversion_result.has_error()) {
        co_return writer_error::parquet_conversion_error;
    }

    auto group = std::get<serde::parquet::group_value>(
      std::move(conversion_result.value()));
    try {
        co_await _writer.write_row(std::move(group));
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error writing parquet row - {}",
          std::current_exception());
        co_return writer_error::file_io_error;
    }
    co_return writer_error::ok;
}

ss::future<writer_error> serde_parquet_writer::finish() {
    co_await _writer.close();
    co_return writer_error::ok;
}

ss::future<std::unique_ptr<parquet_ostream>>
serde_parquet_writer_factory::create_writer(
  const iceberg::struct_type& schema, ss::output_stream<char> out) {
    serde::parquet::writer::options opts{.schema = schema_to_parquet(schema)};
    serde::parquet::writer writer(std::move(opts), std::move(out));
    co_await writer.init();
    co_return std::make_unique<serde_parquet_writer>(std::move(writer));
}

} // namespace datalake
