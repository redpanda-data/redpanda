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
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <fmt/format.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <exception>
#include <ranges>
#include <type_traits>
#include <utility>

namespace cloud_topics::l1 {

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
        auto tidp = co_await read_async_nested<model::topic_id_partition>(
          p, h._bytes_left_limit);
        auto partition = co_await read_async_nested<footer::partition>(
          p, h._bytes_left_limit);
        partitions.emplace_hint(partitions.end(), tidp, std::move(partition));
    }
}

ss::future<> footer::serde_async_write(iobuf& buf) const {
    using serde::write;
    using serde::write_async;
    write(buf, partitions.size());
    for (const auto& [tidp, p] : partitions) {
        co_await write_async(buf, tidp);
        co_await write_async(buf, p.copy());
    }
}

footer::seek_result footer::file_position_before_kafka_offset(
  const model::topic_id_partition& tidp, kafka::offset target) const {
    auto [it, end] = partitions.equal_range(tidp);
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
            return {
              .file_position = partition.file_position,
              .length = partition.length,
            };
        }
        auto delta = index_it->file_position - partition.file_position;
        return {
          .file_position = index_it->file_position,
          .length = partition.length - delta,
        };
    }
    if (min_partition_after_target != end) {
        const auto& partition = min_partition_after_target->second;
        return {
          .file_position = partition.file_position,
          .length = partition.length,
        };
    }
    return npos;
}

footer::seek_result footer::file_position_before_max_timestamp(
  const model::topic_id_partition& tidp, model::timestamp target) {
    auto [begin, end] = partitions.equal_range(tidp);
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
    const auto& partition = min_it->second;
    const auto& index = partition.indexes;
    auto it = std::ranges::lower_bound(
      index, target, std::less<>{}, [](const auto& entry) {
          return entry.max_timestamp;
      });
    // If we're past all index entries, but still within the recorded file
    // bounds, the best we can do is start at the last well known offset (the
    // last index entry).
    if (it == index.end()) {
        --it;
    }
    // If at the first entry, we must start at the file beginning, because the
    // the max is inclusive of those entries.
    if (it == index.begin()) {
        return {
          .file_position = partition.file_position,
          .length = partition.length,
        };
    }
    auto delta = it->file_position - partition.file_position;
    return {
      .file_position = it->file_position,
      .length = partition.length - delta,
    };
}

footer footer::copy() const {
    footer copy;
    for (const auto& [tidp, p] : partitions) {
        copy.partitions.emplace_hint(copy.partitions.end(), tidp, p.copy());
    }
    return copy;
}

ss::future<std::variant<footer, size_t>> footer::read(iobuf buf) {
    if (buf.size_bytes() < sizeof(uint32_t)) {
        throw std::runtime_error(
          fmt::format(
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
            throw std::runtime_error(
              fmt::format(
                "expected footer data type, got: {}", std::to_underlying(dt)));
        }
        auto size = p.consume_type<uint32_t>();
        if (size != p.bytes_left()) {
            throw std::runtime_error(
              fmt::format(
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

    ~object_builder_impl() override {
        vassert(_closed, "L1 object builders must be closed unconditionally");
    }

    ss::future<> start_partition(model::topic_id_partition tidp) final {
        end_partition();
        co_await serde_write_to_stream(data_type::partition_marker, tidp);
        _current_tidp = tidp;
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
          _current_tidp != model::topic_id_partition{},
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
        if ((_offset - last_index_write_position) >= _opts.indexing_interval) {
            _current_partition.indexes.push_back(
              footer::partition::index_entry{
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
        auto footer_size = as_bytes<uint32_t>(_offset - footer_start);
        co_await _output.write(footer_size.data(), footer_size.size());
        _offset += footer_size.size();
        co_return info;
    }

    ss::future<> close() final {
        _closed = true;
        return _output.close();
    }

    size_t file_size() const final { return _offset; }

    template<typename T>
    static ss::future<size_t> serde_write_to_stream(
      data_type dt, T data, ss::output_stream<char>* output) {
        iobuf b;
        b.append(as_bytes(dt));
        iobuf serialized;
        co_await serde::write_async(serialized, std::move(data));
        b.append(as_bytes<uint32_t>(serialized.size_bytes()));
        b.append(std::move(serialized));
        auto size = b.size_bytes();
        co_await write_iobuf_to_output_stream(std::move(b), *output);
        co_return size;
    }

private:
    void end_partition() {
        if (_current_tidp == model::topic_id_partition{}) {
            return;
        }
        _current_partition.length = _offset - _current_partition.file_position;
        _index.partitions.emplace(
          std::exchange(_current_tidp, {}),
          std::exchange(_current_partition, {}));
    }

    template<typename T>
    ss::future<> serde_write_to_stream(data_type dt, T data) {
        _offset += co_await object_builder_impl::serde_write_to_stream(
          dt, std::move(data), &_output);
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
    model::topic_id_partition _current_tidp{};
    footer::partition _current_partition;
    options _opts;
    bool _closed = false;
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

    ~object_reader_impl() override {
        vassert(_closed, "L1 object readers must be closed unconditionally");
    }

    ss::future<peek_result> peek() final {
        if (_peeked) {
            co_return *_peeked;
        }
        if (_saw_footer) {
            // _saw_footer is set when peek() encounters the footer tag but
            // peek() will reach here only after the footer is read and _peeked
            // is cleared.
            _peeked = eof{};
            co_return *_peeked;
        }
        auto dt_buf = co_await _input.read_exactly(sizeof(data_type));
        if (dt_buf.empty() && _input.eof()) {
            _peeked = eof{};
            co_return *_peeked;
        }
        if (dt_buf.size() != sizeof(data_type)) {
            throw std::runtime_error(
              fmt::format(
                "expected {} bytes for data type, got: {}",
                sizeof(data_type),
                dt_buf.size()));
        }
        auto dt = from_bytes<data_type>(dt_buf.get());
        switch (dt) {
        case data_type::kafka_batch: {
            _peeked = co_await read_batch_header();
            co_return *_peeked;
        }
        case data_type::partition_marker:
            _peeked = partition_tag{};
            co_return *_peeked;
        case data_type::footer:
            _saw_footer = true;
            _peeked = footer_tag{};
            co_return *_peeked;
        }
        throw std::runtime_error(
          fmt::format(
            "unknown data type in object: {}", std::to_underlying(dt)));
    }

    ss::future<result> read_next() final {
        co_await peek();
        auto peeked = *_peeked;
        _peeked.reset();
        co_return co_await ss::visit(
          peeked,
          [this](const model::record_batch_header& hdr) {
              auto expected_size = hdr.size_bytes
                                   - model::packed_record_batch_header_size;
              return read_iobuf_exactly(_input, expected_size)
                .then([hdr, expected_size](auto records) {
                    if (records.size_bytes() != expected_size) {
                        return ss::make_exception_future<result>(
                          std::runtime_error(
                            fmt::format(
                              "expected {} bytes of record data, got {}",
                              expected_size,
                              records.size_bytes())));
                    }
                    return ss::make_ready_future<result>(result(
                      model::record_batch(
                        hdr,
                        std::move(records),
                        model::record_batch::tag_ctor_ng{})));
                });
          },
          [this](const partition_tag&) {
              return read_next_serde<model::topic_id_partition>().then(
                [](auto v) {
                    return ss::make_ready_future<result>(std::move(v));
                });
          },
          [this](const footer_tag&) {
              return read_next_serde<footer>().then([](auto v) {
                  return ss::make_ready_future<result>(std::move(v));
              });
          },
          [](const eof&) {
              return ss::make_ready_future<result>(result(eof{}));
          });
    }

    ss::future<> close() final {
        _closed = true;
        return _input.close();
    }

private:
    template<typename T>
    ss::future<T> read_next_serde() {
        ss::temporary_buffer<char> size_prefix_buf
          = co_await _input.read_exactly(sizeof(uint32_t));
        if (size_prefix_buf.size() != sizeof(uint32_t)) {
            throw std::runtime_error(
              fmt::format(
                "expected {} bytes, got {}",
                sizeof(uint32_t),
                size_prefix_buf.size()));
        }
        auto size = from_bytes<uint32_t>(size_prefix_buf.get());
        auto buf = co_await read_iobuf_exactly(_input, size);
        auto parser = iobuf_parser(std::move(buf));
        co_return co_await serde::read_async<T>(parser);
    }

    ss::future<model::record_batch_header> read_batch_header() {
        ss::temporary_buffer<char> hdr_buf = co_await _input.read_exactly(
          batch_header_size);
        if (hdr_buf.size() != batch_header_size) {
            throw std::runtime_error(
              fmt::format(
                "expected {} bytes, got {}",
                batch_header_size,
                hdr_buf.size()));
        }
        model::record_batch_header hdr;
        for_each_batch_header_field(hdr, [&hdr_buf](auto& field) {
            constinit static size_t field_size = sizeof(as_bytes(field));
            using T = std::remove_reference_t<decltype(field)>;
            field = from_bytes<T>(hdr_buf.get());
            hdr_buf.trim_front(field_size);
        });
        co_return hdr;
    }

    ss::input_stream<char> _input;
    std::optional<peek_result> _peeked;
    bool _saw_footer = false;
    bool _closed = false;
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

namespace {

// Copy upto N bytes to the output stream from the input stream
ss::future<> copy_n(
  ss::input_stream<char>* input, ss::output_stream<char>* output, size_t n) {
    struct limited_copier {
        size_t n = 0;
        ss::output_stream<char>* os;

        ss::future<ss::consumption_result<char>>
        operator()(ss::temporary_buffer<char> data) {
            if (data.empty() || n == 0) {
                co_return ss::stop_consuming<char>(std::move(data));
            }
            size_t amt = std::min(data.size(), n);
            data.trim(amt);
            co_await os->write(std::move(data));
            n -= amt;
            co_return ss::continue_consuming();
        }
    };
    limited_copier copier{.n = n, .os = output};
    co_await input->consume(copier);
}

ss::future<>
close_all_streams(combine_objects_parameters parameters, bool quiet) {
    std::exception_ptr ex;
    for (auto& input : parameters.inputs) {
        auto fut = co_await ss::coroutine::as_future(input.stream.close());
        if (fut.failed()) {
            ex = fut.get_exception();
        }
    }
    auto fut = co_await ss::coroutine::as_future(parameters.output.close());
    if (fut.failed()) {
        ex = fut.get_exception();
    }
    if (ex && !quiet) {
        std::rethrow_exception(ex);
    }
    std::ignore = ex;
}

} // namespace

ss::future<object_builder::object_info>
combine_objects(combine_objects_parameters parameters) {
    footer index;
    size_t written = 0;
    for (auto& input : parameters.inputs) {
        for (const auto& [tidp, part] : input.info.index.partitions) {
            // As we copy over our partition and index entries, we also
            // need to update the offsets.
            footer::partition updated = part.copy();
            updated.file_position += written;
            for (auto& index_entry : updated.indexes) {
                index_entry.file_position += written;
            }
            index.partitions.emplace(tidp, std::move(updated));
        }
        auto n = input.info.footer_offset;
        auto fut = co_await ss::coroutine::as_future(
          copy_n(&input.stream, &parameters.output, n));
        if (fut.failed()) {
            auto ex = fut.get_exception();
            co_await close_all_streams(
              std::move(parameters),
              /*quiet=*/true);
            std::rethrow_exception(ex);
        }
        written += n;
    }
    auto footer_offset = written;
    auto footer_write_fut = co_await ss::coroutine::as_future(
      object_builder_impl::serde_write_to_stream(
        data_type::footer, index.copy(), &parameters.output));
    if (footer_write_fut.failed()) {
        auto ex = footer_write_fut.get_exception();
        co_await close_all_streams(std::move(parameters), /*quiet=*/true);
        std::rethrow_exception(ex);
    }
    auto footer_size = footer_write_fut.get();
    auto footer_size_data = as_bytes<uint32_t>(footer_size);
    auto fut = co_await ss::coroutine::as_future(parameters.output.write(
      footer_size_data.data(), footer_size_data.size()));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_await close_all_streams(std::move(parameters), /*quiet=*/true);
        std::rethrow_exception(ex);
    }
    co_await close_all_streams(std::move(parameters), /*quiet=*/false);
    co_return object_builder::object_info{
      .index = std::move(index),
      .footer_offset = footer_offset,
      .size_bytes = footer_offset + footer_size + sizeof(uint32_t),
    };
}

fmt::iterator
footer::partition::index_entry::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{file_position: {}, kafka_offset: {}, max_timestamp: {}}}",
      file_position,
      kafka_offset,
      max_timestamp);
}

fmt::iterator footer::partition::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{file_position: {}, length: {}, first_offset: {}, last_offset: {}, "
      "max_timestamp: {}, indexes: [{}]}}",
      file_position,
      length,
      first_offset,
      last_offset,
      max_timestamp,
      fmt::join(indexes, ", "));
}

fmt::iterator footer::format_to(fmt::iterator it) const {
    auto out = fmt::format_to(it, "{{partitions: [");
    for (const auto& [tidp, partition] : partitions) {
        out = fmt::format_to(
          out, "{{tidp: {}, partition: {}}}, ", tidp, partition);
    }
    return fmt::format_to(out, "]}}");
}

fmt::iterator footer::seek_result::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{file_position:{},length:{}}}", file_position, length);
}

} // namespace cloud_topics::l1
