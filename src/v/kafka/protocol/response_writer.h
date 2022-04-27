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

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/errors.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>

#include <boost/range/numeric.hpp>

#include <optional>
#include <string_view>

namespace kafka {

class response_writer;
void writer_serialize_batch(response_writer& w, model::record_batch&& batch);

class response_writer {
    template<typename ExplicitIntegerType, typename IntegerType>
    // clang-format off
    requires std::is_integral<ExplicitIntegerType>::value
             && std::is_integral<IntegerType>::value
      // clang-format on
      uint32_t
      serialize_int(IntegerType val) {
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
    explicit response_writer(iobuf& out) noexcept
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

    template<typename T, typename ElementWriter>
    requires requires(
      ElementWriter writer, response_writer& rw, const T& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_array(const std::vector<T>& v, ElementWriter&& writer) {
        auto start_size = uint32_t(_out->size_bytes());
        write(int32_t(v.size()));
        for (auto& elem : v) {
            writer(elem, *this);
        }
        return _out->size_bytes() - start_size;
    }
    template<typename T, typename ElementWriter>
    requires requires(ElementWriter writer, response_writer& rw, T& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_array(std::vector<T>& v, ElementWriter&& writer) {
        auto start_size = uint32_t(_out->size_bytes());
        write(int32_t(v.size()));
        for (auto& elem : v) {
            writer(elem, *this);
        }
        return _out->size_bytes() - start_size;
    }

    template<typename T, typename ElementWriter>
    requires requires(ElementWriter writer, response_writer& rw, T& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_nullable_array(
      std::optional<std::vector<T>>& v, ElementWriter&& writer) {
        if (!v) {
            return write(int32_t(-1));
        }
        return write_array(*v, std::forward<ElementWriter>(writer));
    }

    template<typename T, typename ElementWriter>
    requires requires(ElementWriter writer, response_writer& rw, T& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_flex_array(std::vector<T>& v, ElementWriter&& writer) {
        auto start_size = uint32_t(_out->size_bytes());
        write_unsigned_varint(v.size() + 1);
        for (auto& elem : v) {
            writer(elem, *this);
        }
        return _out->size_bytes() - start_size;
    }

    template<typename T, typename ElementWriter>
    requires requires(ElementWriter writer, response_writer& rw, T& elem) {
        { writer(elem, rw) } -> std::same_as<void>;
    }
    uint32_t write_nullable_flex_array(
      std::optional<std::vector<T>>& v, ElementWriter&& writer) {
        if (!v) {
            return write_unsigned_varint(0);
        }
        return write_flex_array(*v, std::forward<ElementWriter>(writer));
    }

    // wrap a writer in a kafka bytes array object. the writer should return
    // true if writing no bytes should result in the encoding as nullable bytes,
    // and false otherwise.
    template<typename ElementWriter>
    requires requires(ElementWriter writer, response_writer& rw) {
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

    uint32_t write_tags() { return write_unsigned_varint(0); }

private:
    iobuf* _out;
};

inline void
writer_serialize_batch(response_writer& w, model::record_batch&& batch) {
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

} // namespace kafka
