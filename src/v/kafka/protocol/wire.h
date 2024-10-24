/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/likely.h"
#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/types.h"
#include "strings/utf8.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <limits>
#include <optional>
#include <type_traits>

namespace seastar {
template<typename T>
class input_stream;
}

namespace kafka::protocol {

namespace detail {

template<typename Container>
concept push_backable = requires(Container c) {
    typename Container::value_type;
    c.push_back(std::declval<typename Container::value_type>());
};

template<typename Container>
concept reserveable = requires(Container c) {
    typename Container::value_type;
    c.reserve(size_t{});
};

} // namespace detail

class decoder {
public:
    explicit decoder(iobuf io) noexcept
      : _parser(std::move(io)) {}

    size_t bytes_left() const { return _parser.bytes_left(); }
    size_t bytes_consumed() const { return _parser.bytes_consumed(); }
    bool read_bool() { return _parser.read_bool(); }
    int8_t read_int8() { return _parser.consume_type<int8_t>(); }
    int16_t read_int16() { return _parser.consume_be_type<int16_t>(); }
    int32_t read_int32() { return _parser.consume_be_type<int32_t>(); }
    int64_t read_int64() { return _parser.consume_be_type<int64_t>(); }
    int32_t read_varint() { return static_cast<int32_t>(read_varlong()); }
    int64_t read_varlong() {
        auto [i, _] = _parser.read_varlong();
        return i;
    }
    uint32_t read_unsigned_varint() {
        auto [i, _] = _parser.read_unsigned_varint();
        return i;
    }
    float64_t read_float64() {
        static_assert(
          std::numeric_limits<float64_t>::is_iec559,
          "Kafka float64 type should be IEEE 754 (IEC599)");
        return std::bit_cast<float64_t>(read_int64());
    }

    static ss::sstring apply_control_validation(ss::sstring val) {
        validate_no_control(val);
        return val;
    }

    ss::sstring read_string() { return do_read_string(read_int16()); }

    ss::sstring read_string_with_control_check() {
        return apply_control_validation(do_read_string(read_int16()));
    }

    ss::sstring read_string_unchecked(int16_t len) {
        return do_read_string(len);
    }

    ss::sstring read_string_unchecked_with_control_check(int16_t len) {
        return apply_control_validation(do_read_string(len));
    }

    ss::sstring read_flex_string() {
        return do_read_flex_string(read_unsigned_varint());
    }

    ss::sstring read_flex_string_with_control_check() {
        return apply_control_validation(
          do_read_flex_string(read_unsigned_varint()));
    }

    std::optional<ss::sstring> read_nullable_string() {
        auto n = read_int16();
        if (n < 0) {
            return std::nullopt;
        }
        return {do_read_string(n)};
    }

    std::optional<ss::sstring> read_nullable_string_with_control_check() {
        auto n = read_int16();
        if (n < 0) {
            return std::nullopt;
        }
        return {apply_control_validation(do_read_string(n))};
    }

    std::optional<ss::sstring> read_nullable_flex_string() {
        auto n = read_unsigned_varint();
        if (n == 0) {
            return std::nullopt;
        }
        return {do_read_flex_string(n)};
    }

    std::optional<ss::sstring> read_nullable_flex_string_with_control_check() {
        auto n = read_unsigned_varint();
        if (n == 0) {
            return std::nullopt;
        }
        return {apply_control_validation(do_read_flex_string(n))};
    }

    uuid read_uuid() {
        return uuid(_parser.consume_type<uuid::underlying_t>());
    }

    bytes read_bytes() { return _parser.read_bytes(read_int32()); }

    bytes read_flex_bytes() {
        auto n = read_unsigned_varint();
        if (unlikely(n == 0)) {
            throw std::out_of_range("Asked to read a negative byte string");
        }
        return _parser.read_bytes(n - 1);
    }

    // Stronly suggested to use read_nullable_iobuf
    std::optional<iobuf> read_fragmented_nullable_bytes() {
        auto [io, count] = read_nullable_iobuf();
        if (count < 0) {
            return std::nullopt;
        }
        return std::move(io);
    }

    std::optional<iobuf> read_fragmented_nullable_flex_bytes() {
        auto len = read_unsigned_varint();
        if (len == 0) {
            return std::nullopt;
        }
        auto ret = _parser.share(len - 1);
        return iobuf{std::move(ret)};
    }

    std::pair<iobuf, int32_t> read_nullable_iobuf() {
        auto len = read_int32();
        if (len < 0) {
            return {iobuf(), len};
        }
        auto ret = _parser.share(len);
        return {std::move(ret), len};
    }

    std::optional<batch_reader> read_nullable_batch_reader() {
        auto io = read_fragmented_nullable_bytes();
        if (!io) {
            return std::nullopt;
        }
        return batch_reader(std::move(*io));
    }

    std::optional<batch_reader> read_nullable_flex_batch_reader() {
        auto io = read_fragmented_nullable_flex_bytes();
        if (!io) {
            return std::nullopt;
        }
        return batch_reader(std::move(*io));
    }

    template<
      template<typename...> typename Container = std::vector,
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, decoder&>>
    requires detail::push_backable<Container<T>>
    Container<T> read_array(ElementParser&& parser) {
        auto len = read_int32();
        if (len < 0) {
            throw std::out_of_range(
              "Attempt to read array with negative length");
        }
        return do_read_array<Container>(
          len, std::forward<ElementParser>(parser));
    }

    template<
      template<typename...> typename Container = std::vector,
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, decoder&>>
    requires detail::push_backable<Container<T>>
    Container<T> read_flex_array(ElementParser&& parser) {
        auto len = read_unsigned_varint();
        if (len == 0) {
            throw std::out_of_range(
              "Attempt to read non-null flex array with 0 length");
        }
        return do_read_array<Container>(
          len - 1, std::forward<ElementParser>(parser));
    }

    template<
      template<typename...> typename Container = std::vector,
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, decoder&>>
    requires detail::push_backable<Container<T>>
    std::optional<Container<T>> read_nullable_array(ElementParser&& parser) {
        auto len = read_int32();
        if (len < 0) {
            return std::nullopt;
        }
        return do_read_array<Container>(
          len, std::forward<ElementParser>(parser));
    }

    template<
      template<typename...> typename Container = std::vector,
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, decoder&>>
    requires detail::push_backable<Container<T>>
    std::optional<Container<T>>
    read_nullable_flex_array(ElementParser&& parser) {
        auto len = read_unsigned_varint();
        if (len == 0) {
            return std::nullopt;
        }
        return do_read_array<Container>(
          len - 1, std::forward<ElementParser>(parser));
    }

    // Only relevent when reading flex requests
    tagged_fields read_tags() {
        tagged_fields::type tags;
        auto num_tags = read_unsigned_varint(); // consume total num of tags
        int64_t prev_tag_id = -1;
        while (num_tags-- > 0) {
            auto id = read_unsigned_varint(); // consume tag id
            if (id <= prev_tag_id) {
                throw std::out_of_range(fmt::format(
                  "Protocol error encountered when parsing tags, tags must be "
                  "serialized in ascending order with no duplicates, tag: {}",
                  id));
            }
            prev_tag_id = id;
            auto size = read_unsigned_varint(); // consume size in bytes
            tags.emplace(
              tag_id(id), iobuf_to_bytes(_parser.share(size))); // consume tag
        }
        return tagged_fields(std::move(tags));
    }

    void consume_unknown_tag(tagged_fields& fields, uint32_t id, size_t n) {
        tagged_fields::type fs(std::move(fields));
        auto [_, succeded] = fs.emplace(tag_id(id), _parser.read_bytes(n));
        if (!succeded) {
            throw std::out_of_range(fmt::format(
              "Protocol error encountered when parsing unknown tags, duplicate "
              "tag id detected: {}",
              id));
        }
        fields = tagged_fields(std::move(fs));
    }

private:
    ss::sstring do_read_string(int16_t n) {
        if (unlikely(n < 0)) {
            throw std::out_of_range("Asked to read a negative byte string");
        }
        return _parser.read_string(n);
    }

    ss::sstring do_read_flex_string(uint32_t n) {
        if (unlikely(n == 0)) {
            throw std::out_of_range("Asked to read a 0 byte flex string");
        }
        return _parser.read_string(n - 1);
    }

    template<
      template<typename...> typename Container = std::vector,
      typename ElementParser,
      typename T = std::invoke_result_t<ElementParser, decoder&>>
    requires requires(ElementParser parser, decoder& rr) {
        { parser(rr) } -> std::same_as<T>;
        detail::push_backable<Container<T>>;
    }
    Container<T> do_read_array(int32_t len, ElementParser&& parser) {
        if (len < 0) {
            throw std::out_of_range("Attempt to parse array w/ negative len");
        }
        Container<T> res;
        if constexpr (detail::reserveable<Container<T>>) {
            res.reserve(len);
        }
        while (len-- > 0) {
            res.push_back(parser(*this));
        }
        return res;
    }

    iobuf_parser _parser;
};

ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>&);

/**
 * Concept for the containers that write_array and friends accept.
 *
 * Basically, must have a size() and be iterable.
 */
template<typename C>
concept SizedContainer = requires(C c, const C cc) {
    typename C::value_type;
    requires std::forward_iterator<typename C::iterator>;
    { c.size() } -> std::same_as<size_t>;
    { c.begin() } -> std::same_as<typename C::iterator>;
    { c.end() } -> std::same_as<typename C::iterator>;
    { cc.begin() } -> std::same_as<typename C::const_iterator>;
    { cc.end() } -> std::same_as<typename C::const_iterator>;
};

class encoder;
void writer_serialize_batch(encoder& w, model::record_batch&& batch);

class encoder {
    template<typename ExplicitIntegerType, typename IntegerType>
    // clang-format off
    requires std::is_integral<ExplicitIntegerType>::value
             && std::is_integral<IntegerType>::value
    // clang-format on
    uint32_t serialize_int(IntegerType val) {
        auto nval = ss::cpu_to_be(ExplicitIntegerType(val));
        _out->append(reinterpret_cast<const char*>(&nval), sizeof(nval));
        return sizeof(nval);
    }

    uint32_t serialize_vint(int64_t val) {
        auto x = vint::to_bytes(val);
        _out->append(x.data(), x.size());
        return x.size();
    }

    uint32_t serialize_unsigned_vint(uint32_t val) {
        auto x = unsigned_vint::to_bytes(val);
        _out->append(x.data(), x.size());
        return x.size();
    }

public:
    explicit encoder(iobuf& out) noexcept
      : _out(&out) {}

    uint32_t write(bool v) { return serialize_int<int8_t>(v); }

    uint32_t write(int8_t v) { return serialize_int<int8_t>(v); }

    uint32_t write(int16_t v) { return serialize_int<int16_t>(v); }

    uint32_t write(int32_t v) { return serialize_int<int32_t>(v); }

    uint32_t write(int64_t v) { return serialize_int<int64_t>(v); }

    uint32_t write(uint32_t v) { return serialize_int<uint32_t>(v); }

    template<typename T, typename = std::enable_if_t<std::is_enum_v<T>>>
    uint32_t write(T v) {
        using underlying = std::underlying_type_t<T>;
        return serialize_int<underlying>(static_cast<underlying>(v));
    }

    uint32_t write(const model::timestamp ts) { return write(ts()); }

    uint32_t write_unsigned_varint(uint32_t v) {
        return serialize_unsigned_vint(v);
    }

    uint32_t write_varint(int32_t v) { return serialize_vint(v); }

    uint32_t write_varlong(int64_t v) { return serialize_vint(v); }

    uint32_t write(std::string_view v) {
        auto size = serialize_int<int16_t>(v.size()) + v.size();
        _out->append(v.data(), v.size());
        return size;
    }

    uint32_t write_flex(std::string_view v) {
        auto size = serialize_unsigned_vint(v.size() + 1) + v.size();
        _out->append(v.data(), v.size());
        return size;
    }

    uint32_t write(const ss::sstring& v) { return write(std::string_view(v)); }

    uint32_t write_flex(const ss::sstring& v) {
        return write_flex(std::string_view(v));
    }

    uint32_t write_flex(std::optional<std::string_view> v) {
        if (!v) {
            return write_unsigned_varint(0);
        }
        return write_flex(*v);
    }

    uint32_t write_flex(const std::optional<ss::sstring>& v) {
        if (!v) {
            return write_unsigned_varint(0);
        }
        return write_flex(std::string_view(*v));
    }

    uint32_t write(std::optional<std::string_view> v) {
        if (!v) {
            return serialize_int<int16_t>(-1);
        }
        return write(*v);
    }

    uint32_t write(const std::optional<ss::sstring>& v) {
        if (!v) {
            return serialize_int<int16_t>(-1);
        }
        return write(std::string_view(*v));
    }

    uint32_t write(uuid uuid) {
        /// This type is not prepended with its size
        _out->append(uuid.view().data(), uuid::length);
        return uuid::length;
    }

    uint32_t write(float64_t v) {
        static_assert(
          std::numeric_limits<float64_t>::is_iec559,
          "Kafka float64 type should be IEEE 754 (IEC599)");
        return serialize_int<int64_t>(std::bit_cast<int64_t>(v));
    }

    uint32_t write(bytes_view bv) {
        auto size = serialize_int<int32_t>(bv.size()) + bv.size();
        _out->append(reinterpret_cast<const char*>(bv.data()), bv.size());
        return size;
    }

    uint32_t write_flex(bytes_view bv) {
        auto size = write_unsigned_varint(bv.size() + 1) + bv.size();
        _out->append(reinterpret_cast<const char*>(bv.data()), bv.size());
        return size;
    }

    uint32_t write(const model::topic& topic) { return write(topic()); }

    uint32_t write(std::optional<iobuf>&& data) {
        if (!data) {
            return serialize_int<int32_t>(-1);
        }
        auto size = serialize_int<int32_t>(data->size_bytes())
                    + data->size_bytes();
        _out->append(std::move(*data));
        return size;
    }

    uint32_t write_flex(std::optional<iobuf>&& data) {
        if (!data) {
            return write_unsigned_varint(0);
        }
        auto size = write_unsigned_varint(data->size_bytes() + 1)
                    + data->size_bytes();
        _out->append(std::move(*data));
        return size;
    }

    uint32_t write(std::optional<batch_reader>&& rdr) {
        if (!rdr) {
            return write(std::optional<iobuf>());
        }
        return write(std::move(*rdr).release());
    }

    uint32_t write(std::optional<batch_reader>& rdr) {
        if (!rdr) {
            return write(std::optional<iobuf>());
        }
        return write(std::move(*rdr).release());
    }

    uint32_t write_flex(std::optional<batch_reader>&& rdr) {
        if (!rdr) {
            return write_flex(std::optional<iobuf>());
        }
        return write_flex(std::move(*rdr).release());
    }

    uint32_t write_flex(std::optional<batch_reader>& rdr) {
        if (!rdr) {
            return write_flex(std::optional<iobuf>());
        }
        return write_flex(std::move(*rdr).release());
    }

    // write bytes directly to output without a length prefix
    uint32_t write_direct(iobuf&& f) {
        auto size = f.size_bytes();
        _out->append(std::move(f));
        return size;
    }

    template<typename T, typename Tag>
    uint32_t write(const named_type<T, Tag>& t) {
        return write(t());
    }

    template<typename T, typename Tag>
    uint32_t write_flex(const named_type<T, Tag>& t) {
        return write_flex(t());
    }

    template<typename Rep, typename Period>
    uint32_t write(const std::chrono::duration<Rep, Period>& d) {
        return write(int32_t(d.count()));
    }

    template<typename C, typename ElementWriter>
    requires requires(
      ElementWriter writer, encoder& rw, const typename C::value_type& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_array(const C& v, ElementWriter&& writer) {
        auto start_size = uint32_t(_out->size_bytes());
        write(int32_t(v.size()));
        for (auto& elem : v) {
            writer(elem, *this);
        }
        return _out->size_bytes() - start_size;
    }
    template<typename C, typename ElementWriter>
    requires requires(
      ElementWriter writer, encoder& rw, typename C::value_type& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_array(C& v, ElementWriter&& writer) {
        auto start_size = uint32_t(_out->size_bytes());
        write(int32_t(v.size()));
        for (auto& elem : v) {
            writer(elem, *this);
        }
        return _out->size_bytes() - start_size;
    }

    template<typename C, typename ElementWriter>
    requires requires(
      ElementWriter writer, encoder& rw, typename C::value_type& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_nullable_array(std::optional<C>& v, ElementWriter&& writer) {
        if (!v) {
            return write(int32_t(-1));
        }
        return write_array(*v, std::forward<ElementWriter>(writer));
    }

    template<typename C, typename ElementWriter>
    requires requires(
      ElementWriter writer, encoder& rw, typename C::value_type& elem) {
        requires SizedContainer<C>;
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_flex_array(C& v, ElementWriter&& writer) {
        auto start_size = uint32_t(_out->size_bytes());
        write_unsigned_varint(v.size() + 1);
        for (auto& elem : v) {
            writer(elem, *this);
        }
        return _out->size_bytes() - start_size;
    }

    template<typename C, typename ElementWriter>
    requires requires(
      ElementWriter writer, encoder& rw, typename C::value_type& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t
    write_nullable_flex_array(std::optional<C>& v, ElementWriter&& writer) {
        if (!v) {
            return write_unsigned_varint(0);
        }
        return write_flex_array(*v, std::forward<ElementWriter>(writer));
    }

    // wrap a writer in a kafka bytes array object. the writer should return
    // true if writing no bytes should result in the encoding as nullable bytes,
    // and false otherwise.
    template<typename ElementWriter>
    requires requires(ElementWriter writer, encoder& rw) {
        { writer(rw) } -> std::same_as<bool>;
    }
    uint32_t write_bytes_wrapped(ElementWriter&& writer) {
        auto ph = _out->reserve(sizeof(int32_t));
        auto start_size = uint32_t(_out->size_bytes());
        auto zero_len_is_null = writer(*this);
        int32_t real_size = _out->size_bytes() - start_size;
        // enc_size: the size prefix in the serialization
        int32_t enc_size = real_size > 0 ? real_size
                                         : (zero_len_is_null ? -1 : 0);
        auto be_size = ss::cpu_to_be(enc_size);
        auto* in = reinterpret_cast<const char*>(&be_size);
        ph.write(in, sizeof(be_size));
        return real_size + sizeof(be_size);
    }

    uint32_t write(std::optional<produce_request_record_data>& data) {
        if (!data) {
            return write(int32_t(-1));
        }
        auto start_size = uint32_t(_out->size_bytes());
        write(data->adapter.batch->size_bytes());
        writer_serialize_batch(*this, std::move(data->adapter.batch.value()));
        return _out->size_bytes() - start_size;
    }

    uint32_t write_flex(std::optional<produce_request_record_data>& data) {
        if (!data) {
            return write_unsigned_varint(0);
        }
        auto start_size = uint32_t(_out->size_bytes());
        write_unsigned_varint(data->adapter.batch->size_bytes() + 1);
        writer_serialize_batch(*this, std::move(data->adapter.batch.value()));
        return _out->size_bytes() - start_size;
    }

    // Only relevent when writing flex responses
    uint32_t write_tags(tagged_fields&& tags) {
        auto start_size = uint32_t(_out->size_bytes());
        const auto n = tags().size();
        write_unsigned_varint(n); // write total number of tags
        for (auto& [id, tag] : tags()) {
            // write tag id +  size in bytes + tag itself
            write_unsigned_varint(id);
            write_size_prepended(bytes_to_iobuf(tag));
        }
        return _out->size_bytes() - start_size;
    }

    // Currently used within our generator where we don't support writing any
    // custom tag fields but must encode at least a 0 byte to be protocol
    // compliant
    uint32_t write_tags() { return write_unsigned_varint(0); }

    /// Used when a size (in bytes) must be prepended before serializing the
    /// object itself. So far only used for encoding tag headers.
    uint32_t write_size_prepended(iobuf&& buf) {
        const auto size = write_unsigned_varint(buf.size_bytes())
                          + buf.size_bytes();
        _out->append(std::move(buf));
        return size;
    }

private:
    iobuf* _out;
};

inline void writer_serialize_batch(encoder& w, model::record_batch&& batch) {
    /*
     * calculate batch size expected by kafka client.
     *
     * 1. records_size = batch.size_bytes() - RP header size;
     * 2. kafka_total = records_size + kafka header size;
     * 3. batch_size = kafka_total - sizeof(offset) - sizeof(length);
     *
     * The records size in (1) is computed correctly because RP batch size
     * is defined as the RP header size plus the size of the records. Unlike
     * the kafka batch size described below, RP batch size includes the size
     * of the length field itself.
     *
     * The adjustment in (3) is because the batch size given in the kafka
     * header does not include the offset preceeding the length field nor
     * the size of the length field itself.
     */
    auto size = batch.size_bytes() - model::packed_record_batch_header_size
                + internal::kafka_header_size - sizeof(int64_t)
                - sizeof(int32_t);

    w.write(int64_t(batch.base_offset()));
    w.write(int32_t(size)); // batch length
    w.write(
      int32_t(leader_epoch_from_term(batch.term()))); // partition leader epoch
    w.write(int8_t(2));                               // magic
    w.write(batch.header().crc);
    w.write(int16_t(batch.header().attrs.value()));
    w.write(int32_t(batch.header().last_offset_delta));
    w.write(int64_t(batch.header().first_timestamp.value()));
    w.write(int64_t(batch.header().max_timestamp.value()));
    w.write(int64_t(batch.header().producer_id));
    w.write(int16_t(batch.header().producer_epoch));
    w.write(int32_t(batch.header().base_sequence));
    w.write(int32_t(batch.record_count()));
    w.write_direct(std::move(batch).release_data());
}

} // namespace kafka::protocol
