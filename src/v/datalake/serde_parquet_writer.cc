#include "datalake/serde_parquet_writer.h"

#include "base/vlog.h"
#include "datalake/logger.h"
#include "datalake/schema_parquet.h"
#include "datalake/values_parquet.h"
#include "version/version.h"

namespace datalake {

ss::future<writer_error> serde_parquet_writer::add_data_struct(
  iceberg::struct_value value, size_t approx_size, ss::abort_source& as) {
    auto conversion_result = co_await to_parquet_value(
      std::make_unique<iceberg::struct_value>(std::move(value)));
    if (conversion_result.has_error()) {
        co_return writer_error::parquet_conversion_error;
    }

    auto group = std::get<serde::parquet::group_value>(
      std::move(conversion_result.value()));
    try {
        co_await _mem_tracker.maybe_reserve_memory(approx_size, as);
        auto stats = co_await _writer.write_row(std::move(group));
        _buffered_bytes = stats.buffered_size;
        _flushed_bytes = stats.flushed_size;
        _mem_tracker.update_current_memory_usage(_buffered_bytes);
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error writing parquet row - {}",
          std::current_exception());
        co_return writer_error::file_io_error;
    }
    co_return writer_error::ok;
}

size_t serde_parquet_writer::buffered_bytes() const { return _buffered_bytes; }
size_t serde_parquet_writer::flushed_bytes() const { return _flushed_bytes; }

ss::future<> serde_parquet_writer::flush() {
    co_await _writer.flush_row_group();
    auto stats = _writer.stats();
    _buffered_bytes = stats.buffered_size;
    _flushed_bytes = stats.flushed_size;
    vassert(
      _buffered_bytes == 0,
      "Memory buffered in the writer after flush: {}",
      _buffered_bytes);
    _mem_tracker.release();
}

ss::future<writer_error> serde_parquet_writer::finish() {
    co_await _writer.close();
    _mem_tracker.release();
    _buffered_bytes = _flushed_bytes = 0;
    co_return writer_error::ok;
}

ss::future<std::unique_ptr<parquet_ostream>>
serde_parquet_writer_factory::create_writer(
  const iceberg::struct_type& schema,
  ss::output_stream<char> out,
  writer_mem_tracker& mem_tracker) {
    serde::parquet::writer::options opts{
      .schema = schema_to_parquet(schema),
      .version = ss::sstring(redpanda_git_version()),
      .build = ss::sstring(redpanda_git_revision()),
      .compress = true,
    };
    serde::parquet::writer writer(std::move(opts), std::move(out));
    co_await writer.init();
    co_return std::make_unique<serde_parquet_writer>(
      std::move(writer), mem_tracker);
}

} // namespace datalake
