/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/parquet/writer.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "container/contiguous_range_map.h"
#include "serde/parquet/column_writer.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/shredder.h"

namespace serde::parquet {

namespace {

// The path within the schema starts at the root, so we can drop the root's
// name.
chunked_vector<ss::sstring> path_in_schema(const schema_element& element) {
    chunked_vector<ss::sstring> path;
    auto it = element.path.begin();
    for (++it; it != element.path.end(); ++it) {
        path.push_back(*it);
    }
    return path;
}

} // namespace

class writer::impl {
public:
    impl(options opts, ss::output_stream<char> output)
      : _opts(std::move(opts))
      , _output(std::move(output)) {}

    ss::future<> init() {
        index_schema(_opts.schema);
        _opts.schema.for_each([this](const schema_element& element) {
            if (!element.is_leaf()) {
                return;
            }
            _columns.emplace(
              element.position,
              column{
                .leaf = &element,
                .writer = column_writer(
                  element,
                  {
                    .compress = _opts.compress,
                  }),
              });
        });
        // write the leading magic bytes
        co_await write_iobuf(iobuf::from("PAR1"));
    }

    ss::future<> write_row(group_value row) {
        co_await shred_record(
          _opts.schema, std::move(row), [this](shredded_value sv) {
              return write_value(std::move(sv));
          });
        ++_current_row_group_stats.rows;
    }

    row_group_stats current_row_group_stats() const {
        return _current_row_group_stats;
    }

    ss::future<> flush_row_group() {
        if (_current_row_group_stats.rows == 0) {
            co_return;
        }
        row_group rg{};
        rg.file_offset = static_cast<int64_t>(_offset);
        for (auto& [pos, col] : _columns) {
            auto page = co_await col.writer.flush_page();
            auto& data_header = std::get<data_page_header>(page.header.type);
            rg.num_rows = data_header.num_rows;
            auto page_size = static_cast<int64_t>(page.serialized.size_bytes());
            rg.total_byte_size += page_size;
            rg.columns.push_back(column_chunk{
              .meta_data = column_meta_data{
                .type = col.leaf->type,
                .encodings = {data_header.data_encoding},
                .path_in_schema = path_in_schema(*col.leaf),
                .codec = _opts.compress ? compression_codec::zstd : compression_codec::uncompressed,
                .num_values = data_header.num_values,
                .total_uncompressed_size = page.header.uncompressed_page_size + page.serialized_header_size,
                .total_compressed_size = page.header.compressed_page_size + page.serialized_header_size,
                .key_value_metadata = {},
                .data_page_offset = static_cast<int64_t>(_offset),
                // Because we only write a single page per row group at the moment,
                // a column chunk's stats are trivially the same as it's page.
                .stats = std::move(data_header.stats),
              },
            });
            co_await write_iobuf(std::move(page.serialized));
        }
        _current_row_group_stats = {};
        _row_groups.push_back(std::move(rg));
    }

    ss::future<> close() {
        co_await flush_row_group();
        int64_t num_rows = 0;
        for (const auto& rg : _row_groups) {
            num_rows += rg.num_rows;
        }
        auto encoded_footer = encode(file_metadata{
          .version = 2,
          .schema = flatten(_opts.schema),
          .num_rows = num_rows,
          .row_groups = std::move(_row_groups),
          .key_value_metadata = std::move(_opts.metadata),
          .created_by = fmt::format(
            "Redpanda version {} (build {})", _opts.version, _opts.build),
        });
        size_t footer_size = encoded_footer.size_bytes();
        co_await write_iobuf(std::move(encoded_footer));
        co_await write_iobuf(encode_footer_size(footer_size));
        co_await write_iobuf(iobuf::from("PAR1"));
        co_await _output.close();
    }

private:
    iobuf encode_footer_size(size_t size) {
        iobuf b;
        auto le_size = ss::cpu_to_le(static_cast<uint32_t>(size));
        // NOLINTNEXTLINE(*reinterpret-cast*)
        b.append(reinterpret_cast<const uint8_t*>(&le_size), sizeof(le_size));
        return b;
    }

    ss::future<> write_value(shredded_value sv) {
        auto& col = _columns.at(sv.schema_element_position);
        auto stats = col.writer.add(
          std::move(sv.val), sv.rep_level, sv.def_level);
        _current_row_group_stats.memory_usage += stats.memory_usage;
        return ss::now();
    }

    ss::future<> write_iobuf(iobuf b) {
        _offset += b.size_bytes();
        co_await write_iobuf_to_output_stream(std::move(b), _output);
    }

    struct column {
        const schema_element* leaf;
        column_writer writer;
    };

    options _opts;
    ss::output_stream<char> _output;
    size_t _offset = 0; // offset written to the stream
    contiguous_range_map<int32_t, column> _columns;
    chunked_vector<row_group> _row_groups;
    row_group_stats _current_row_group_stats;
};

writer::writer(options opts, ss::output_stream<char> output)
  : _impl(std::make_unique<writer::impl>(std::move(opts), std::move(output))) {}

writer::writer(writer&&) noexcept = default;
writer& writer::operator=(writer&&) noexcept = default;
writer::~writer() noexcept = default;

ss::future<> writer::init() { return _impl->init(); }

ss::future<> writer::write_row(group_value row) {
    return _impl->write_row(std::move(row));
}

row_group_stats writer::current_row_group_stats() const {
    return _impl->current_row_group_stats();
}

ss::future<> writer::flush_row_group() { return _impl->flush_row_group(); }

ss::future<> writer::close() { return _impl->close(); }

} // namespace serde::parquet
