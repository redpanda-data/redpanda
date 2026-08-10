#include "datalake/serde_parquet_writer.h"

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "datalake/logger.h"
#include "iceberg/conversion/schema_parquet.h"
#include "iceberg/conversion/values_parquet.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/schema.h"
#include "version/version.h"

#include <seastar/util/defer.hh>

#include <fmt/ranges.h>

#include <variant>

namespace datalake {

absl::flat_hash_map<ss::sstring, size_t> resolve_bloom_filter_columns(
  const parquet_write_config& config,
  const iceberg::struct_type& iceberg_schema) {
    absl::flat_hash_map<ss::sstring, size_t> result;
    if (config.bloom_filter_columns.empty()) {
        return result;
    }

    auto schema_copy = schema_to_parquet(iceberg_schema);
    serde::parquet::index_schema(schema_copy);

    // Build dot-paths for all leaves.
    chunked_vector<ss::sstring> leaf_dot_paths;
    schema_copy.for_each([&](const serde::parquet::schema_element& element) {
        if (!element.is_leaf()) {
            return;
        }
        // Drop the root name from the path.
        chunked_vector<ss::sstring> segments;
        auto it = element.path.begin();
        for (++it; it != element.path.end(); ++it) {
            segments.push_back(*it);
        }
        leaf_dot_paths.push_back(fmt::format("{}", fmt::join(segments, ".")));
    });

    for (const auto& col : config.bloom_filter_columns) {
        size_t match_count = 0;
        for (const auto& dot_path : leaf_dot_paths) {
            if (dot_path == col.name) {
                result[dot_path] = col.ndv;
                ++match_count;
            }
        }
        if (match_count == 0) {
            vlog(
              datalake_log.warn,
              "bloom filter property references unknown column '{}', ignoring",
              col.name);
        } else if (match_count > 1) {
            vlog(
              datalake_log.warn,
              "bloom filter column name '{}' matches {} schema paths "
              "(ambiguous dot-separated name), applying to all",
              col.name,
              match_count);
        }
    }

    return result;
}

writer_error serde_parquet_writer::set_error(writer_error e) {
    _error = e;
    return _error;
}

ss::future<writer_error> serde_parquet_writer::add_data_struct(
  iceberg::struct_value value, size_t, ss::abort_source& as) {
    // This method should always return `writer_error::ok` if
    // `_writer.write_row(...)` is successful. However, we still want to convey
    // memory and disk reservation errors that happen after the row write.
    // Hence, the current solution is to return those errors on the subsequent
    // call to `add_data_struct`.
    //
    // Similar to `local_parquet_file_writer` once an error has occurred further
    // writes are prevented.
    if (_error != writer_error::ok) {
        co_return _error;
    }

    auto conversion_result = co_await to_parquet_value(
      std::make_unique<iceberg::struct_value>(std::move(value)));
    if (conversion_result.has_error()) {
        vlog(
          datalake_log.warn,
          "Error converting iceberg struct to parquet value - {}",
          conversion_result.error());
        co_return set_error(writer_error::parquet_conversion_error);
    }

    auto group = std::get<serde::parquet::group_value>(
      std::move(conversion_result.value()));
    try {
        auto stats = co_await _writer.write_row(std::move(group));
        auto stats_updater = ss::defer([this, stats] {
            _buffered_bytes = stats.buffered_size;
            _flushed_bytes = stats.flushed_size;
        });

        /*
         * handle disk reservation. see writer_disk_tracker for more info.
         */
        const auto total_bytes = _buffered_bytes + _flushed_bytes;
        const auto new_total_bytes = stats.buffered_size + stats.flushed_size;
        if (new_total_bytes > total_bytes) {
            auto& disk = _mem_tracker.disk();
            auto result = co_await disk.reserve_bytes(
              new_total_bytes - total_bytes, as);
            if (result != reservation_error::ok) {
                set_error(map_to_writer_error(result));
                co_return writer_error::ok;
            }
        } else if (new_total_bytes < total_bytes) {
            auto& disk = _mem_tracker.disk();
            co_await disk.free_bytes(total_bytes - new_total_bytes, as);
        }

        /*
         * handle memory reservation
         */
        auto new_buffered_bytes = stats.buffered_size;
        if (new_buffered_bytes > _buffered_bytes) {
            auto reservation_result = co_await _mem_tracker.reserve_bytes(
              new_buffered_bytes - _buffered_bytes, as);
            if (reservation_result != reservation_error::ok) {
                set_error(map_to_writer_error(reservation_result));
                co_return writer_error::ok;
            }
        } else if (new_buffered_bytes < _buffered_bytes) {
            // underlying writer may choose to compress data when
            // a page worth of data is batched, at which point the
            // resulting compressed size is smaller than before and
            // allows us to free up some bytes.
            co_await _mem_tracker.free_bytes(
              _buffered_bytes - new_buffered_bytes, as);
        }
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error writing parquet row - {}",
          std::current_exception());
        co_return set_error(writer_error::file_io_error);
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
}

ss::future<writer_error> serde_parquet_writer::finish() {
    co_await _writer.close();
    for (auto& cs : _writer.column_file_stats()) {
        if (!cs.field_id) {
            continue;
        }
        per_column_stats ps;
        ps.field_id = *cs.field_id;
        ps.value_count = cs.value_count;
        ps.column_size_bytes = cs.column_size_bytes;
        if (cs.bounds.null_count) {
            ps.null_value_count = *cs.bounds.null_count;
        }
        if (cs.bounds.min) {
            ps.lower_bound = iobuf_to_bytes(cs.bounds.min->value);
        }
        if (cs.bounds.max) {
            ps.upper_bound = iobuf_to_bytes(cs.bounds.max->value);
        }
        _column_stats.push_back(std::move(ps));
    }
    _buffered_bytes = _flushed_bytes = 0;
    co_return writer_error::ok;
}

chunked_vector<per_column_stats> serde_parquet_writer::column_stats() const {
    return _column_stats.copy();
}

ss::future<std::unique_ptr<parquet_ostream>>
serde_parquet_writer_factory::create_writer(
  const iceberg::struct_type& schema,
  const parquet_write_config& write_config,
  ss::output_stream<char> out,
  writer_mem_tracker& mem_tracker) {
    auto bloom_columns = resolve_bloom_filter_columns(write_config, schema);
    serde::parquet::writer::options opts{
      .schema = schema_to_parquet(schema),
      .version = ss::sstring(redpanda_git_version()),
      .build = ss::sstring(redpanda_git_revision()),
      .compress = write_config.compress,
      .bloom_filter_columns = std::move(bloom_columns),
    };
    serde::parquet::writer writer(std::move(opts), std::move(out));
    co_await writer.init();
    co_return std::make_unique<serde_parquet_writer>(
      std::move(writer), mem_tracker);
}

} // namespace datalake
