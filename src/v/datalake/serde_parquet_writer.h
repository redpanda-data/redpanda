#pragma once

#include "absl/container/flat_hash_map.h"
#include "datalake/data_writer_interface.h"
#include "iceberg/datatypes.h"
#include "serde/parquet/writer.h"

#include <seastar/core/sstring.hh>

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
    chunked_vector<per_column_stats> column_stats() const final;

private:
    serde::parquet::writer _writer;
    writer_mem_tracker& _mem_tracker;
    int64_t _buffered_bytes{0};
    int64_t _flushed_bytes{0};
    // Used to store any errors that occur after a row write is successful.
    writer_error _error{writer_error::ok};
    writer_error set_error(writer_error);
    chunked_vector<per_column_stats> _column_stats;
};

class serde_parquet_writer_factory : public parquet_ostream_factory {
public:
    ss::future<std::unique_ptr<parquet_ostream>> create_writer(
      const iceberg::struct_type&,
      const parquet_write_config&,
      ss::output_stream<char>,
      writer_mem_tracker&) final;
};

/// Resolve per-column bloom filter config against the given Iceberg schema.
/// Returns a map from dot-joined parquet column path (without root) to NDV.
/// Warns on unknown columns (0 matches) and ambiguous dot-separated names
/// (>1 match), applying config to all matches in the latter case.
absl::flat_hash_map<ss::sstring, size_t> resolve_bloom_filter_columns(
  const parquet_write_config& config, const iceberg::struct_type& schema);

} // namespace datalake
