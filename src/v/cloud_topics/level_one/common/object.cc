/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object.h"

#include "base/vassert.h"
#include "bytes/iostream.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "reflection/type_traits.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <fmt/format.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <ranges>
#include <type_traits>
#include <utility>

namespace experimental::cloud_topics::l1 {

namespace {

// the delimiter for what kind of data is next in the object.
enum class data_type : uint8_t {
    kafka_batch = 0,
    partition_marker = 1,
    footer = 2,
};

// NOTE: we currently don't use serde for this serialization as to not pay the
// tax of creating an iobuf and iobuf_parser just to read a byte for the
// data_type or read 4 bytes for the size of a serde serialized object.

template<typename T>
requires std::is_integral_v<T> || std::is_scoped_enum_v<T>
         || reflection::is_rp_named_type<T>
         || std::is_same_v<model::timestamp, T>
         || std::is_same_v<model::record_batch_attributes, T>
constexpr auto as_bytes(T value) {
    if constexpr (std::is_scoped_enum_v<T>) {
        return std::bit_cast<
          std::array<char, sizeof(std::underlying_type_t<T>)>,
          std::underlying_type_t<T>>(ss::cpu_to_le(std::to_underlying(value)));
    } else if constexpr (
      reflection::is_rp_named_type<T> || std::is_same_v<model::timestamp, T>) {
        return std::bit_cast<
          std::array<char, sizeof(typename T::type)>,
          typename T::type>(ss::cpu_to_le(value()));
    } else if constexpr (std::is_same_v<model::record_batch_attributes, T>) {
        return std::bit_cast<
          std::array<char, sizeof(typename T::type)>,
          typename T::type>(ss::cpu_to_le(value.value()));
    } else {
        return std::bit_cast<std::array<char, sizeof(T)>, T>(
          ss::cpu_to_le(value));
    }
}

template<typename T>
requires std::is_integral_v<T> || std::is_scoped_enum_v<T>
         || reflection::is_rp_named_type<T>
         || std::is_same_v<model::timestamp, T>
         || std::is_same_v<model::record_batch_attributes, T>
constexpr T from_bytes(const void* ptr) {
    if constexpr (std::is_scoped_enum_v<T>) {
        std::underlying_type_t<T> value;
        std::memcpy(&value, ptr, sizeof(value));
        return static_cast<T>(ss::le_to_cpu(value));
    } else if constexpr (
      reflection::is_rp_named_type<T> || std::is_same_v<model::timestamp, T>
      || std::is_same_v<model::record_batch_attributes, T>) {
        typename T::type value;
        std::memcpy(&value, ptr, sizeof(value));
        return T(ss::le_to_cpu(value));
    } else {
        T value;
        std::memcpy(&value, ptr, sizeof(value));
        return ss::le_to_cpu(value);
    }
}

constexpr void
for_each_batch_header_field(model::record_batch_header& hdr, auto func) {
    func(hdr.header_crc);
    func(hdr.size_bytes);
    func(hdr.type);
    func(hdr.crc);
    func(hdr.attrs);
    func(hdr.base_offset);
    func(hdr.last_offset_delta);
    func(hdr.first_timestamp);
    func(hdr.max_timestamp);
    func(hdr.producer_id);
    func(hdr.producer_epoch);
    func(hdr.base_sequence);
    func(hdr.record_count);
    // The difference between this and the packed header in model/local storage
    // is that we serialize the term here.
    func(hdr.ctx.term);
}

consteval size_t compute_batch_header_size() noexcept {
    size_t size = 0;
    model::record_batch_header hdr{};
    for_each_batch_header_field(
      hdr, [&size](const auto& field) { size += sizeof(as_bytes(field)); });
    return size;
}

constinit const static size_t batch_header_size = compute_batch_header_size();

} // namespace

footer::partition footer::partition::copy() const {
    return {
      .file_position = file_position,
      .length = length,
      .indexes = indexes.copy(),
      .first_offset = first_offset,
      .last_offset = last_offset,
      .max_timestamp = max_timestamp,
    };
}
ss::future<>
footer::partition::serde_async_read(iobuf_parser& p, serde::header h) {
    using serde::read_nested;
    file_position = read_nested<size_t>(p, h._bytes_left_limit);
    length = read_nested<size_t>(p, h._bytes_left_limit);
    first_offset = read_nested<kafka::offset>(p, h._bytes_left_limit);
    last_offset = read_nested<kafka::offset>(p, h._bytes_left_limit);
    max_timestamp = read_nested<model::timestamp>(p, h._bytes_left_limit);
    auto size = read_nested<size_t>(p, h._bytes_left_limit);
    indexes.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        indexes.push_back(read_nested<index_entry>(p, h._bytes_left_limit));
        co_await ss::coroutine::maybe_yield();
    }
}

ss::future<> footer::partition::serde_async_write(iobuf& buf) const {
    using serde::write;
    write(buf, file_position);
    write(buf, length);
    write(buf, first_offset);
    write(buf, last_offset);
    write(buf, max_timestamp.value());
    write(buf, indexes.size());
    for (const auto& entry : indexes) {
        write(buf, entry);
        co_await ss::coroutine::maybe_yield();
    }
}

ss::future<> footer::serde_async_read(iobuf_parser& p, serde::header h) {
    using serde::read_async_nested;
    using serde::read_nested;
    auto size = read_nested<size_t>(p, h._bytes_left_limit);
    for (size_t i = 0; i < size; ++i) {
        auto ntp = co_await read_async_nested<model::ntp>(
          p, h._bytes_left_limit);
        auto partition = co_await read_async_nested<footer::partition>(
          p, h._bytes_left_limit);
        partitions.emplace_hint(
          partitions.end(), std::move(ntp), std::move(partition));
    }
}

ss::future<> footer::serde_async_write(iobuf& buf) const {
    using serde::write;
    using serde::write_async;
    write(buf, partitions.size());
    for (const auto& [ntp, p] : partitions) {
        co_await write_async(buf, ntp);
        co_await write_async(buf, p.copy());
    }
}

size_t footer::file_position_before_kafka_offset(
  const model::ntp& ntp, kafka::offset target) {
    auto [it, end] = partitions.equal_range(ntp);
    auto min_partition_after_target = end;
    for (; it != end; ++it) {
        const auto& partition = it->second;
        if (target > partition.last_offset) {
            continue;
        }
        if (target < partition.first_offset) {
            if (
              min_partition_after_target == end
              || partition.first_offset
                   < min_partition_after_target->second.first_offset) {
                min_partition_after_target = it;
            }
            continue;
        }
        auto rev = std::views::reverse(partition.indexes);
        auto index_it = std::ranges::lower_bound(
          rev, target, std::greater<>{}, [](const auto& entry) {
              return entry.kafka_offset;
          });
        if (index_it == rev.end()) {
            return partition.file_position;
        }
        return index_it->file_position;
    }
    if (min_partition_after_target != end) {
        return min_partition_after_target->second.file_position;
    }
    return npos;
}

size_t footer::file_position_before_max_timestamp(
  const model::ntp& ntp, model::timestamp target) {
    auto [begin, end] = partitions.equal_range(ntp);
    auto filtered = std::views::filter(
      std::ranges::subrange{begin, end}, [&target](const auto& entry) {
          return target <= entry.second.max_timestamp;
      });
    auto min_it = std::ranges::min_element(
      filtered, std::less<>{}, [](const auto& entry) {
          return entry.second.first_offset;
      });
    if (min_it == filtered.end()) {
        return npos;
    }
    const auto& index = min_it->second.indexes;
    auto it = std::ranges::lower_bound(
      index, target, std::less<>{}, [](const auto& entry) {
          return entry.max_timestamp;
      });
    // If we're past all index entries, but still within the recorded file
    // bounds, the best we can do is start at the last well known offset (the
    // last index entry).
    if (it == index.end()) {
        return min_it->second.indexes.back().file_position;
    }
    // If at the first entry, we must start at the file beginning, because the
    // the max is inclusive of those entries.
    if (it == index.begin()) {
        return min_it->second.file_position;
    }
    return it->file_position;
}

footer footer::copy() const {
    footer copy;
    for (const auto& [ntp, p] : partitions) {
        copy.partitions.emplace_hint(copy.partitions.end(), ntp, p.copy());
    }
    return copy;
}

ss::future<std::variant<footer, size_t>> footer::read(iobuf buf) {
    if (buf.size_bytes() < sizeof(uint32_t)) {
        throw std::runtime_error(fmt::format(
          "expected at least {} bytes in footer, got: {}",
          sizeof(uint32_t),
          buf.size_bytes()));
    }
    iobuf_const_parser parser(buf);
    auto footer_size
      = iobuf_parser(buf.tail(sizeof(uint32_t))).consume_type<uint32_t>();
    if (buf.size_bytes() >= (footer_size + sizeof(uint32_t))) {
        iobuf_parser p(buf.share(
          buf.size_bytes() - sizeof(uint32_t) - footer_size, footer_size));
        auto dt = static_cast<data_type>(
          p.consume_type<std::underlying_type_t<data_type>>());
        if (dt != data_type::footer) {
            throw std::runtime_error(fmt::format(
              "expected footer data type, got: {}", std::to_underlying(dt)));
        }
        auto size = p.consume_type<uint32_t>();
        if (size != p.bytes_left()) {
            throw std::runtime_error(fmt::format(
              "expected footer size to match the remaining bytes, "
              "got: {}, expected: {}",
              p.bytes_left(),
              size));
        }
        co_return co_await serde::read_async<footer>(p);
    }
    co_return (footer_size + sizeof(uint32_t)) - buf.size_bytes();
}

namespace {
class object_builder_impl final : public object_builder {
public:
    explicit object_builder_impl(ss::output_stream<char> output, options opts)
      : _output(std::move(output))
      , _opts(opts) {}

    ss::future<> start_partition(model::ntp ntp) final {
        end_partition();
        co_await serde_write_to_stream(data_type::partition_marker, ntp);
        _current_ntp = ntp;
        _current_partition = {
          .file_position = _offset,
          .length = 0, // will be set when the partition is finished
          .indexes = {},
          .first_offset = {},
          .last_offset = {},
          .max_timestamp = model::timestamp::missing(),
        };
    }

    ss::future<> add_batch(model::record_batch batch) final {
        dassert(
          batch.header().type == model::record_batch_type::raft_data,
          "expected raft_data batches, got: {}",
          batch.header().type);
        dassert(
          _current_ntp != model::ntp{},
          "wrote a data batch without starting a partition");
        dassert(
          _current_partition.last_offset
            < model::offset_cast(batch.base_offset()),
          "wrote an offset out of order within a batch: {} < {}",
          _current_partition.last_offset,
          model::offset_cast(batch.base_offset()));
        if (_current_partition.first_offset == kafka::offset{}) {
            _current_partition.first_offset = model::offset_cast(
              batch.header().base_offset);
        }
        _current_partition.last_offset = model::offset_cast(
          batch.header().base_offset
          + model::offset_delta(batch.header().last_offset_delta));
        _current_partition.max_timestamp = std::max(
          _current_partition.max_timestamp, batch.header().max_timestamp);
        auto last_index_write_position
          = _current_partition.indexes.empty()
              ? _current_partition.file_position
              : _current_partition.indexes.back().file_position;
        if ((_offset - last_index_write_position) >= _opts.indexing_frequency) {
            _current_partition.indexes.push_back(footer::partition::index_entry{
              .file_position = _offset,
              .kafka_offset = model::offset_cast(batch.header().base_offset),
              .max_timestamp = _current_partition.max_timestamp});
        }
        co_await write_batch_to_stream(std::move(batch));
    }

    ss::future<object_info> finish() final {
        end_partition();
        auto footer_start = _offset;
        co_await serde_write_to_stream(data_type::footer, _index.copy());
        object_info info{
          .index = std::move(_index),
          .footer_offset = footer_start,
          .size_bytes = _offset + sizeof(uint32_t),
        };
        auto footer_size
          = std::bit_cast<std::array<char, sizeof(uint32_t)>, uint32_t>(
            _offset - footer_start);
        co_await _output.write(footer_size.data(), footer_size.size());
        co_return info;
    }

    ss::future<> close() final { return _output.close(); }

private:
    void end_partition() {
        if (_current_ntp == model::ntp{}) {
            return;
        }
        _current_partition.length = _offset - _current_partition.file_position;
        _index.partitions.emplace(
          std::exchange(_current_ntp, {}),
          std::exchange(_current_partition, {}));
    }

    template<typename T>
    ss::future<> serde_write_to_stream(data_type dt, T data) {
        iobuf b;
        b.append(as_bytes(dt));
        iobuf serialized;
        co_await serde::write_async(serialized, std::move(data));
        b.append(as_bytes<uint32_t>(serialized.size_bytes()));
        b.append(std::move(serialized));
        _offset += b.size_bytes();
        co_await write_iobuf_to_output_stream(std::move(b), _output);
    }

    ss::future<> write_batch_to_stream(model::record_batch batch) {
        iobuf b;
        b.append(as_bytes(data_type::kafka_batch));
        for_each_batch_header_field(
          batch.header(), [&b](auto arg) { b.append(as_bytes(arg)); });
        b.append(std::move(batch).release_data());
        _offset += b.size_bytes();
        return write_iobuf_to_output_stream(std::move(b), _output);
    }

    size_t _offset = 0;
    ss::output_stream<char> _output;
    footer _index;
    model::ntp _current_ntp;
    footer::partition _current_partition;
    options _opts;
};

} // namespace

std::unique_ptr<object_builder>
object_builder::create(ss::output_stream<char> output, options opts) {
    return std::make_unique<object_builder_impl>(std::move(output), opts);
}

namespace {

class object_reader_impl : public object_reader {
public:
    explicit object_reader_impl(ss::input_stream<char> input)
      : _input(std::move(input)) {}

    ss::future<result> read_next() final {
        if (_saw_footer) {
            // After the footer we have 4 bytes for the size of the footer, so
            // we need to make sure that we don't try and interpret those bytes
            // as a `data_type`.
            co_return eof{};
        }
        auto dt_buf = co_await _input.read_exactly(sizeof(data_type));
        if (dt_buf.empty() && _input.eof()) {
            co_return eof{};
        }
        if (dt_buf.size() != sizeof(data_type)) {
            throw std::runtime_error(fmt::format(
              "expected {} bytes for data type, got: {}",
              sizeof(data_type),
              dt_buf.size()));
        }
        auto dt = from_bytes<data_type>(dt_buf.get());
        switch (dt) {
        case data_type::kafka_batch:
            co_return co_await read_next_batch();
        case data_type::partition_marker:
            co_return co_await read_next_serde<model::ntp>();
        case data_type::footer:
            _saw_footer = true;
            co_return co_await read_next_serde<footer>();
        }
        throw std::runtime_error(fmt::format(
          "unknown data type in object: {}", std::to_underlying(dt)));
    }

    ss::future<> close() final { return _input.close(); }

private:
    template<typename T>
    ss::future<T> read_next_serde() {
        ss::temporary_buffer<char> size_prefix_buf
          = co_await _input.read_exactly(sizeof(uint32_t));
        if (size_prefix_buf.size() != sizeof(uint32_t)) {
            throw std::runtime_error(fmt::format(
              "expected {} bytes, got {}",
              sizeof(uint32_t),
              size_prefix_buf.size()));
        }
        auto size = from_bytes<uint32_t>(size_prefix_buf.get());
        auto buf = co_await read_iobuf_exactly(_input, size);
        auto parser = iobuf_parser(std::move(buf));
        co_return co_await serde::read_async<T>(parser);
    }

    ss::future<model::record_batch> read_next_batch() {
        ss::temporary_buffer<char> hdr_buf = co_await _input.read_exactly(
          batch_header_size);
        if (hdr_buf.size() != batch_header_size) {
            throw std::runtime_error(fmt::format(
              "expected {} bytes, got {}", batch_header_size, hdr_buf.size()));
        }
        model::record_batch_header hdr;
        for_each_batch_header_field(hdr, [&hdr_buf](auto& field) {
            constinit static size_t field_size = sizeof(as_bytes(field));
            using T = std::remove_reference_t<decltype(field)>;
            field = from_bytes<T>(hdr_buf.get());
            hdr_buf.trim_front(field_size);
        });
        auto records = co_await read_iobuf_exactly(
          _input, hdr.size_bytes - model::packed_record_batch_header_size);
        co_return model::record_batch(
          hdr, std::move(records), model::record_batch::tag_ctor_ng{});
    }

    ss::input_stream<char> _input;
    bool _saw_footer = false;
};

} // namespace

std::unique_ptr<object_reader>
object_reader::create(ss::input_stream<char> input) {
    return std::make_unique<object_reader_impl>(std::move(input));
}

ss::future<std::unique_ptr<object_reader>>
object_reader::create(std::filesystem::path p, size_t offset, size_t length) {
    auto f = co_await ss::open_file_dma(p.native(), ss::open_flags::ro);
    co_return create(ss::make_file_input_stream(std::move(f), offset, length));
}

std::unique_ptr<object_reader>
object_reader::create(ss::file f, size_t offset, size_t length) {
    return create(ss::make_file_input_stream(std::move(f), offset, length));
}

} // namespace experimental::cloud_topics::l1
