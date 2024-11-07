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

#include "serde/parquet/column_writer.h"

#include "hashing/crc32.h"
#include "serde/parquet/encoding.h"

#include <seastar/util/variant_utils.hh>

#include <limits>
#include <stdexcept>
#include <type_traits>

namespace serde::parquet {

class column_writer::impl {
public:
    impl() = default;
    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;
    impl(impl&&) noexcept = default;
    impl& operator=(impl&&) noexcept = default;
    virtual ~impl() noexcept = default;

    virtual void add(value, rep_level, def_level) = 0;
    virtual data_page flush_page() = 0;
};

namespace {

void extend_crc32(crc::crc32& crc, const iobuf& buf) {
    for (const auto& frag : buf) {
        crc.extend(frag.get(), frag.size());
    }
}

template<typename... Args>
crc::crc32 compute_crc32(Args&&... args) {
    crc::crc32 crc;
    (extend_crc32(crc, std::forward<Args>(args)), ...);
    return crc;
}

template<typename value_type>
class buffered_column_writer final : public column_writer::impl {
public:
    buffered_column_writer(def_level max_def_level, rep_level max_rep_level)
      : _max_rep_level(max_rep_level)
      , _max_def_level(max_def_level) {}

    void add(value val, rep_level rl, def_level dl) override {
        ++_num_values;
        // A repetition level of zero means that it's the start of a new row and
        // not a repeated value within the same row.
        if (rl == rep_level(0)) {
            ++_num_rows;
        }
        ss::visit(
          std::move(val),
          [this](value_type& v) { _value_buffer.push_back(std::move(v)); },
          [this](null_value&) {
              // null values are valid, but are not encoded in the actual data,
              // they are encoded in the defintion levels.
              ++_num_nulls;
          },
          [](auto& v) {
              throw std::runtime_error(fmt::format(
                "invalid value for column: {:.32}", value(std::move(v))));
          });
        _rep_levels.push_back(rl);
        _def_levels.push_back(dl);
    }

    data_page flush_page() override {
        iobuf encoded_def_levels;
        // If the max level is 0 then we don't write levels at all.
        if (_max_def_level > def_level(0)) {
            encoded_def_levels = encode_levels(_max_def_level, _def_levels);
        }
        _def_levels.clear();
        iobuf encoded_rep_levels;
        // If the max level is 0 then we don't write levels at all.
        if (_max_rep_level > rep_level(0)) {
            encoded_rep_levels = encode_levels(_max_rep_level, _rep_levels);
        }
        _rep_levels.clear();
        iobuf encoded_data;
        if constexpr (std::is_trivially_copyable_v<value_type>) {
            encoded_data = encode_plain(_value_buffer);
            _value_buffer.clear();
        } else {
            encoded_data = encode_plain(std::exchange(_value_buffer, {}));
        }
        size_t page_size = encoded_def_levels.size_bytes()
                           + encoded_rep_levels.size_bytes()
                           + encoded_data.size_bytes();
        if (page_size > std::numeric_limits<int32_t>::max()) {
            throw std::runtime_error(
              fmt::format("page size limit exceeded: {} bytes", page_size));
        }
        page_header header{
          .uncompressed_page_size = static_cast<int32_t>(page_size),
          .compressed_page_size = static_cast<int32_t>(page_size),
          .crc = compute_crc32(encoded_rep_levels, encoded_def_levels, encoded_data),
          .type = data_page_header{
            .num_values = std::exchange(_num_values, 0),
            .num_nulls = std::exchange(_num_nulls, 0),
            .num_rows = std::exchange(_num_rows, 0),
            .data_encoding = encoding::plain,
            .definition_levels_byte_length = static_cast<int32_t>(encoded_def_levels.size_bytes()),
            .repetition_levels_byte_length = static_cast<int32_t>(encoded_rep_levels.size_bytes()),
            .is_compressed = false,
          },
        };
        iobuf full_page_data = encode(header);
        auto header_size = static_cast<int64_t>(full_page_data.size_bytes());
        full_page_data.append(std::move(encoded_rep_levels));
        full_page_data.append(std::move(encoded_def_levels));
        full_page_data.append(std::move(encoded_data));
        return {
          .header = header,
          .serialized_header_size = header_size,
          .serialized = std::move(full_page_data),
        };
    }

private:
    // TODO: add compression and detailed stats
    chunked_vector<value_type> _value_buffer;
    chunked_vector<def_level> _def_levels;
    chunked_vector<rep_level> _rep_levels;
    int32_t _num_rows = 0;
    int32_t _num_nulls = 0;
    int32_t _num_values = 0;
    rep_level _max_rep_level;
    def_level _max_def_level;
};

template class buffered_column_writer<boolean_value>;
template class buffered_column_writer<int32_value>;
template class buffered_column_writer<int64_value>;
template class buffered_column_writer<float32_value>;
template class buffered_column_writer<float64_value>;
template class buffered_column_writer<byte_array_value>;
template class buffered_column_writer<fixed_byte_array_value>;

std::unique_ptr<column_writer::impl>
make_impl(const schema_element&, std::monostate) {
    throw std::runtime_error("invariant error: cannot make a column writer "
                             "from an intermediate value");
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, bool_type) {
    return std::make_unique<buffered_column_writer<boolean_value>>(
      e.max_definition_level, e.max_repetition_level);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, i32_type) {
    return std::make_unique<buffered_column_writer<int32_value>>(
      e.max_definition_level, e.max_repetition_level);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, i64_type) {
    return std::make_unique<buffered_column_writer<int64_value>>(
      e.max_definition_level, e.max_repetition_level);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, f32_type) {
    return std::make_unique<buffered_column_writer<float32_value>>(
      e.max_definition_level, e.max_repetition_level);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, f64_type) {
    return std::make_unique<buffered_column_writer<float64_value>>(
      e.max_definition_level, e.max_repetition_level);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, byte_array_type t) {
    if (t.fixed_length.has_value()) {
        return std::make_unique<buffered_column_writer<fixed_byte_array_value>>(
          e.max_definition_level, e.max_repetition_level);
    }
    return std::make_unique<buffered_column_writer<byte_array_value>>(
      e.max_definition_level, e.max_repetition_level);
}

} // namespace

column_writer::column_writer(const schema_element& col)
  : _impl(std::visit([&col](auto x) { return make_impl(col, x); }, col.type)) {}

column_writer::column_writer(column_writer&&) noexcept = default;
column_writer& column_writer::operator=(column_writer&&) noexcept = default;
column_writer::~column_writer() noexcept = default;

void column_writer::add(value val, rep_level rep_level, def_level def_level) {
    return _impl->add(std::move(val), rep_level, def_level);
}

data_page column_writer::flush_page() { return _impl->flush_page(); }

} // namespace serde::parquet
