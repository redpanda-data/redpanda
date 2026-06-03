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

#include "absl/numeric/int128.h"
#include "bytes/iobuf_parser.h"
#include "compression/compression.h"
#include "container/chunked_vector.h"
#include "hashing/crc32.h"
#include "hashing/xx.h"
#include "serde/parquet/bloom_filter.h"
#include "serde/parquet/column_stats_collector.h"
#include "serde/parquet/encoding.h"
#include "strings/utf8.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/variant_utils.hh>

#include <bit>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

namespace serde::parquet {

using options = column_writer::options;

class column_writer::impl {
public:
    impl() = default;
    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;
    impl(impl&&) noexcept = default;
    impl& operator=(impl&&) noexcept = default;
    virtual ~impl() noexcept = default;

    virtual void add(value, rep_level, def_level) = 0;
    virtual int64_t memory_usage() const = 0;
    virtual int64_t current_page_memory_usage() const = 0;
    virtual ss::future<> next_page() = 0;
    virtual ss::future<flushed_pages> flush_pages() = 0;
    virtual statistics file_column_stats() = 0;
};

namespace {

std::pair<iobuf, bool>
truncate_min(iobuf value, int32_t max_len, bool is_utf8) {
    if (max_len <= 0 || static_cast<int32_t>(value.size_bytes()) <= max_len) {
        return {std::move(value), true};
    }
    if (!is_utf8) {
        iobuf_parser parser(std::move(value));
        return {parser.copy(max_len), false};
    }
    // Read one extra byte so utf8_truncate_min's "string fits" fast path
    // doesn't fire and it properly scans for a codepoint boundary.
    auto read_len = std::min(
      value.size_bytes(), static_cast<size_t>(max_len) + 1);
    iobuf_parser parser(std::move(value));
    auto buf = parser.read_bytes(read_len);
    auto sv = std::string_view(
      reinterpret_cast<const char*>(buf.data()), buf.size());
    auto prefix = utf8_truncate_min(sv, static_cast<size_t>(max_len));
    iobuf result;
    result.append(prefix.data(), prefix.size());
    return {std::move(result), false};
}

std::optional<std::pair<iobuf, bool>>
truncate_max(iobuf value, int32_t max_len, bool is_utf8) {
    if (max_len <= 0 || static_cast<int32_t>(value.size_bytes()) <= max_len) {
        return {{std::move(value), true}};
    }
    if (!is_utf8) {
        iobuf_parser parser(std::move(value));
        auto prefix = parser.read_bytes(max_len);
        for (int i = static_cast<int>(prefix.size()) - 1; i >= 0; --i) {
            if (prefix[i] < 0xFF) {
                prefix[i]++;
                iobuf result;
                result.append(prefix.data(), i + 1);
                return {{std::move(result), false}};
            }
        }
        // All prefix bytes are 0xFF — no valid truncated upper bound.
        return std::nullopt;
    }
    // Same +1 trick: give utf8_truncate_max a string longer than max_len so
    // it takes the truncation path rather than the "string fits" fast path.
    auto read_len = std::min(
      value.size_bytes(), static_cast<size_t>(max_len) + 1);
    iobuf_parser parser(std::move(value));
    auto buf = parser.read_bytes(read_len);
    auto sv = std::string_view(
      reinterpret_cast<const char*>(buf.data()), buf.size());
    auto opt = utf8_truncate_max(sv, static_cast<size_t>(max_len));
    if (opt) {
        iobuf result;
        result.append(opt->data(), opt->size());
        return {{std::move(result), false}};
    }
    return std::nullopt;
}

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

// Hash a parquet value to uint64_t using xxHash64 (seed=0), as required by
// the Parquet bloom filter spec. Fixed-size values are hashed as their
// little-endian byte representation; variable-length values are hashed
// directly over their bytes.
uint64_t hash_for_bloom(int32_value v) {
    auto le = ss::cpu_to_le(v.val);
    return xxhash_64(reinterpret_cast<const char*>(&le), sizeof(le));
}
uint64_t hash_for_bloom(int64_value v) {
    auto le = ss::cpu_to_le(v.val);
    return xxhash_64(reinterpret_cast<const char*>(&le), sizeof(le));
}
uint64_t hash_for_bloom(float32_value v) {
    auto bits = ss::cpu_to_le(std::bit_cast<uint32_t>(v.val));
    return xxhash_64(reinterpret_cast<const char*>(&bits), sizeof(bits));
}
uint64_t hash_for_bloom(float64_value v) {
    auto bits = ss::cpu_to_le(std::bit_cast<uint64_t>(v.val));
    return xxhash_64(reinterpret_cast<const char*>(&bits), sizeof(bits));
}
uint64_t hash_for_bloom(const byte_array_value& v) {
    incremental_xxhash64 h;
    for (const auto& frag : v.val) {
        h.update(frag.get(), frag.size());
    }
    return h.digest();
}
uint64_t hash_for_bloom(const fixed_byte_array_value& v) {
    incremental_xxhash64 h;
    for (const auto& frag : v.val) {
        h.update(frag.get(), frag.size());
    }
    return h.digest();
}
// Boolean columns never have bloom filters (see constructor below).
// This overload exists only so the template compiles for boolean_value.
uint64_t hash_for_bloom(boolean_value) {
    vassert(false, "unreachable: boolean columns are never bloom-filtered");
}

template<typename value_type, auto comparator>
class buffered_column_writer final : public column_writer::impl {
public:
    buffered_column_writer(const schema_element& schema_element, options opts)
      : _bloom_filter(
          opts.bloom_filter_ndv > 0
            ? std::make_optional<bloom_filter>(opts.bloom_filter_ndv)
            : std::nullopt)
      , _max_rep_level(schema_element.max_repetition_level)
      , _max_def_level(schema_element.max_definition_level)
      , _opts(opts) {}

    void add(value val, rep_level rl, def_level dl) override {
        // A repetition level of zero means that it's the start of a new row and
        // not a repeated value within the same row.
        if (rl == rep_level(0)) {
            ++_num_rows;
        }
        ++_num_values;

        int64_t value_memory_usage = 0;

        ss::visit(
          std::move(val),
          [this, &value_memory_usage](value_type v) {
              if constexpr (!std::is_trivially_copyable_v<value_type>) {
                  value_memory_usage = v.val.size_bytes();
              } else {
                  value_memory_usage = sizeof(value_type);
              }
              _current_page_stats.record_value(v);
              if (_bloom_filter) {
                  _bloom_filter->insert(hash_for_bloom(v));
              }
              _value_buffer.add_value(std::move(v));
          },
          [this](null_value) {
              // null values are valid, but are not encoded in the actual data,
              // they are encoded in the defintion levels.
              _current_page_stats.record_null();
          },
          [](auto v) {
              throw std::runtime_error(
                fmt::format(
                  "invalid value for column: {:.32}", value(std::move(v))));
          });
        _rep_levels.push_back(rl);
        _def_levels.push_back(dl);
    }

    ss::future<> flush_page() {
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
        iobuf encoded_data = _value_buffer.get_encoded_buf();
        size_t uncompressed_page_size = encoded_def_levels.size_bytes()
                                        + encoded_rep_levels.size_bytes()
                                        + encoded_data.size_bytes();
        if (uncompressed_page_size > std::numeric_limits<int32_t>::max()) {
            throw std::runtime_error(
              fmt::format(
                "page size limit exceeded: {} bytes", uncompressed_page_size));
        }
        if (_opts.compress) {
            encoded_data = co_await compression::stream_compressor::compress(
              std::move(encoded_data), compression::type::zstd);
        }
        size_t compressed_page_size = encoded_def_levels.size_bytes()
                                      + encoded_rep_levels.size_bytes()
                                      + encoded_data.size_bytes();
        using bound_type = decltype(_flushed_stats)::bound_ref_type;
        std::optional<statistics::bound> max_bound;
        if (bound_type max = _current_page_stats.max()) {
            if constexpr (std::is_same_v<value_type, byte_array_value>) {
                if (
                  auto truncated = truncate_max(
                    encode_for_stats(*max),
                    _opts.max_stats_truncate_length,
                    _opts.is_utf8_string)) {
                    auto [val, is_exact] = std::move(*truncated);
                    max_bound.emplace(std::move(val), is_exact);
                }
            } else {
                max_bound.emplace(encode_for_stats(*max), true);
            }
        }
        std::optional<statistics::bound> min_bound;
        if (bound_type min = _current_page_stats.min()) {
            if constexpr (std::is_same_v<value_type, byte_array_value>) {
                auto [val, is_exact] = truncate_min(
                  encode_for_stats(*min),
                  _opts.max_stats_truncate_length,
                  _opts.is_utf8_string);
                min_bound.emplace(std::move(val), is_exact);
            } else {
                min_bound.emplace(encode_for_stats(*min), true);
            }
        }
        _flushed_stats.merge(_current_page_stats);
        page_header header{
          .uncompressed_page_size = static_cast<int32_t>(uncompressed_page_size),
          .compressed_page_size = static_cast<int32_t>(compressed_page_size),
          .crc = compute_crc32(encoded_rep_levels, encoded_def_levels, encoded_data),
          .type = data_page_header{
            .num_values = std::exchange(_num_values, 0),
            .num_nulls = static_cast<int32_t>(_current_page_stats.null_count()),
            .num_rows = std::exchange(_num_rows, 0),
            .data_encoding = encoding::plain,
            .definition_levels_byte_length = static_cast<int32_t>(encoded_def_levels.size_bytes()),
            .repetition_levels_byte_length = static_cast<int32_t>(encoded_rep_levels.size_bytes()),
            .is_compressed = _opts.compress,
            .stats = statistics{
              .null_count = _current_page_stats.null_count(),
              .max = std::move(max_bound),
              .min = std::move(min_bound),
            },
          },
        };
        iobuf full_page_data = encode(header);
        auto header_size = static_cast<int64_t>(full_page_data.size_bytes());
        full_page_data.append(std::move(encoded_rep_levels));
        full_page_data.append(std::move(encoded_def_levels));
        full_page_data.append(std::move(encoded_data));
        _current_page_stats.reset();
        _total_memory_usage += static_cast<int32_t>(
          full_page_data.size_bytes());
        _flushed_pages.push_back(
          data_page{
            .header = std::move(header),
            .serialized_header_size = header_size,
            .serialized = std::move(full_page_data),
          });
    }

    int64_t memory_usage() const override {
        return _total_memory_usage + current_page_memory_usage();
    }

    int64_t current_page_memory_usage() const override {
        // NOTE: This does account for the underlying buffer memory
        // but we don't want to account for the capacity here, ideally we
        // always use the full capacity in our value buffer, and eagerly
        // accounting that usage might cause callers to overagressively
        // flush pages/row groups.
        return _value_buffer.size_bytes()
               + (_rep_levels.size() * sizeof(_rep_levels[0]))
               + (_def_levels.size() * sizeof(_def_levels[0]));
    }

    ss::future<> next_page() override { return flush_page(); }

    ss::future<flushed_pages> flush_pages() override {
        if (_num_values > 0) {
            co_await flush_page();
        }
        _file_stats.merge(_flushed_stats);
        auto full_stats = build_statistics(_flushed_stats);
        _flushed_stats.reset();
        _total_memory_usage = 0;
        iobuf bf;
        if (_bloom_filter) {
            // Discard the filter if it is too full: FPP ≈ fill_ratio()^8,
            // so the default threshold of 0.75 corresponds to ~10% FPP,
            // at which point the filter is unlikely to be useful for
            // skipping row groups.
            if (
              _bloom_filter->fill_ratio()
              <= _opts.bloom_filter_max_fill_ratio) {
                _bloom_filter->serialize(bf);
            }
            _bloom_filter->reset();
        }
        co_return flushed_pages{
          .pages = std::exchange(_flushed_pages, {}),
          .stats = std::move(full_stats),
          .bloom_filter = std::move(bf),
        };
    }

    statistics file_column_stats() override {
        return build_statistics(_file_stats);
    }

private:
    using collector = column_stats_collector<value_type, comparator>;

    statistics build_statistics(collector& c) {
        statistics result{.null_count = c.null_count()};
        using bound_type = typename collector::bound_ref_type;
        if (bound_type max = c.max()) {
            if constexpr (std::is_same_v<value_type, byte_array_value>) {
                if (
                  auto truncated = truncate_max(
                    encode_for_stats(*max),
                    _opts.max_stats_truncate_length,
                    _opts.is_utf8_string)) {
                    auto [val, is_exact] = std::move(*truncated);
                    result.max.emplace(std::move(val), is_exact);
                }
            } else {
                result.max.emplace(encode_for_stats(*max), true);
            }
        }
        if (bound_type min = c.min()) {
            if constexpr (std::is_same_v<value_type, byte_array_value>) {
                auto [val, is_exact] = truncate_min(
                  encode_for_stats(*min),
                  _opts.max_stats_truncate_length,
                  _opts.is_utf8_string);
                result.min.emplace(std::move(val), is_exact);
            } else {
                result.min.emplace(encode_for_stats(*min), true);
            }
        }
        return result;
    }

    column_stats_collector<value_type, comparator> _current_page_stats;
    column_stats_collector<value_type, comparator> _flushed_stats;
    column_stats_collector<value_type, comparator> _file_stats;
    std::optional<bloom_filter> _bloom_filter;
    int64_t _total_memory_usage = 0;
    plain_encoder<value_type> _value_buffer;
    chunked_vector<def_level> _def_levels;
    chunked_vector<rep_level> _rep_levels;
    chunked_vector<data_page> _flushed_pages;
    int32_t _num_rows = 0;
    int32_t _num_values = 0;
    rep_level _max_rep_level;
    def_level _max_def_level;
    options _opts;
};

template class buffered_column_writer<boolean_value, ordering::boolean>;
template class buffered_column_writer<int32_value, ordering::int32>;
template class buffered_column_writer<int32_value, ordering::uint32>;
template class buffered_column_writer<int64_value, ordering::int64>;
template class buffered_column_writer<int64_value, ordering::uint64>;
template class buffered_column_writer<float32_value, ordering::float32>;
template class buffered_column_writer<float64_value, ordering::float64>;
template class buffered_column_writer<byte_array_value, ordering::byte_array>;
template class buffered_column_writer<
  fixed_byte_array_value,
  ordering::fixed_byte_array>;
template class buffered_column_writer<
  fixed_byte_array_value,
  ordering::int128_be>;

std::unique_ptr<column_writer::impl>
make_impl(const schema_element&, std::monostate, options) {
    throw std::runtime_error(
      "invariant error: cannot make a column writer "
      "from an intermediate value");
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, bool_type, options opts) {
    return std::make_unique<
      buffered_column_writer<boolean_value, ordering::boolean>>(e, opts);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, i32_type, options opts) {
    auto integer_type = std::get_if<int_type>(&e.logical_type);
    if (integer_type && !integer_type->is_signed) {
        return std::make_unique<
          buffered_column_writer<int32_value, ordering::uint32>>(e, opts);
    }
    return std::make_unique<
      buffered_column_writer<int32_value, ordering::int32>>(e, opts);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, i64_type, options opts) {
    auto integer_type = std::get_if<int_type>(&e.logical_type);
    if (integer_type && !integer_type->is_signed) {
        return std::make_unique<
          buffered_column_writer<int64_value, ordering::uint64>>(e, opts);
    }
    return std::make_unique<
      buffered_column_writer<int64_value, ordering::int64>>(e, opts);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, f32_type, options opts) {
    return std::make_unique<
      buffered_column_writer<float32_value, ordering::float32>>(e, opts);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, f64_type, options opts) {
    return std::make_unique<
      buffered_column_writer<float64_value, ordering::float64>>(e, opts);
}
std::unique_ptr<column_writer::impl>
make_impl(const schema_element& e, byte_array_type t, options opts) {
    if (t.fixed_length.has_value()) {
        if (
          t.fixed_length == sizeof(absl::int128)
          && std::holds_alternative<decimal_type>(e.logical_type)) {
            return std::make_unique<buffered_column_writer<
              fixed_byte_array_value,
              ordering::int128_be>>(e, opts);
        }
        return std::make_unique<buffered_column_writer<
          fixed_byte_array_value,
          ordering::fixed_byte_array>>(e, opts);
    }
    opts.is_utf8_string = std::holds_alternative<string_type>(e.logical_type)
                          || std::holds_alternative<enum_type>(e.logical_type);
    return std::make_unique<
      buffered_column_writer<byte_array_value, ordering::byte_array>>(e, opts);
}

} // namespace

column_writer::column_writer(const schema_element& col, options opts)
  : _impl(
      std::visit(
        [&col, opts](auto x) { return make_impl(col, x, opts); }, col.type)) {}

column_writer::column_writer(column_writer&&) noexcept = default;
column_writer& column_writer::operator=(column_writer&&) noexcept = default;
column_writer::~column_writer() noexcept = default;

void column_writer::add(value val, rep_level rep_level, def_level def_level) {
    _impl->add(std::move(val), rep_level, def_level);
}

int64_t column_writer::memory_usage() const { return _impl->memory_usage(); }
int64_t column_writer::current_page_memory_usage() const {
    return _impl->current_page_memory_usage();
}

ss::future<> column_writer::next_page() { return _impl->next_page(); }

ss::future<flushed_pages> column_writer::flush_pages() {
    return _impl->flush_pages();
}

statistics column_writer::file_column_stats() {
    return _impl->file_column_stats();
}

} // namespace serde::parquet
