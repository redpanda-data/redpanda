#pragma once

#include "bytes/bytes.h"
#include "bytes/bytes_ostream.h"
#include "model/fundamental.h"
#include "redpanda/kafka/errors/errors.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"
#include "utils/fragbuf.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>

#include <boost/range/numeric.hpp>

#include <optional>
#include <string_view>

namespace kafka::requests {

class response_writer {
    template<typename ExplicitIntegerType, typename IntegerType>
    // clang-format off
    CONCEPT(requires std::is_integral<ExplicitIntegerType>::value
            && std::is_integral<IntegerType>::value)
      // clang-format on
      uint32_t serialize_int(IntegerType val) {
        auto nval = cpu_to_be(ExplicitIntegerType(val));
        _out->write(reinterpret_cast<const char*>(&nval), sizeof(nval));
        return sizeof(nval);
    }

    template<typename VintType>
    uint32_t serialize_vint(typename VintType::value_type val) {
        std::array<bytes::value_type, vint::max_length> encoding_buffer;
        const auto size = VintType::serialize(val, encoding_buffer.begin());
        _out->write(
          reinterpret_cast<const char*>(encoding_buffer.data()), size);
        return size;
    }

public:
    explicit response_writer(bytes_ostream& out) noexcept
      : _out(&out) {
    }

    uint32_t write(bool v) {
        return serialize_int<int8_t>(v);
    }

    uint32_t write(int8_t v) {
        return serialize_int<int8_t>(v);
    }

    uint32_t write(int16_t v) {
        return serialize_int<int16_t>(v);
    }

    uint32_t write(int32_t v) {
        return serialize_int<int32_t>(v);
    }

    uint32_t write(int64_t v) {
        return serialize_int<int64_t>(v);
    }

    uint32_t write(uint32_t v) {
        return serialize_int<uint32_t>(v);
    }

    uint32_t write(kafka::errors::error_code v) {
        using underlying = std::underlying_type_t<kafka::errors::error_code>;
        return serialize_int<underlying>(static_cast<underlying>(v));
    }

    uint32_t write_varint(int32_t v) {
        return serialize_vint<vint>(v);
    }

    uint32_t write_varlong(int64_t v) {
        return serialize_vint<vint>(v);
    }

    uint32_t write(std::string_view v) {
        auto size = serialize_int<int16_t>(v.size()) + v.size();
        _out->write(reinterpret_cast<const char*>(v.data()), v.size());
        return size;
    }

    uint32_t write(const sstring& v) {
        return write(std::string_view(v));
    }

    uint32_t write(std::optional<std::string_view> v) {
        if (!v) {
            return serialize_int<int16_t>(-1);
        }
        return write(*v);
    }

    uint32_t write(const std::optional<sstring>& v) {
        if (!v) {
            return serialize_int<int16_t>(-1);
        }
        return write(std::string_view(*v));
    }

    uint32_t write(bytes_view bv) {
        auto size = serialize_int<int32_t>(bv.size()) + bv.size();
        _out->write(std::move(bv));
        return size;
    }

    uint32_t write(const bytes_opt& bv) {
        if (!bv) {
            return serialize_int<int32_t>(-1);
        }
        return write(bytes_view(*bv));
    }

    uint32_t write(const model::topic& topic) {
        return write(topic.name);
    }

    // write bytes directly to output without a length prefix
    uint32_t write_direct(fragbuf&& f) {
      auto size = f.size_bytes();
      auto bufs = std::move(f).release();
      for (auto& b : bufs) {
	_out->write(std::move(b));
      }
      return size;
    }

    // write bytes_ostream directly to output without a length prefix
    uint32_t write_direct(bytes_ostream&& buf) {
        // TODO: this should probably be an bytes_ostream interface
        auto size = buf.size_bytes();
        auto bufs = std::move(buf).release();
        for (auto& b : bufs) {
            _out->write(std::move(b).release());
        }
        return size;
    }

    // clang-format off
    template<typename T, typename ElementWriter>
    CONCEPT(requires requires (ElementWriter writer,
                               response_writer& rw,
                               const T& elem) {
        { writer(elem, rw) } -> void;
    })
    // clang-format on
    uint32_t write_array(const std::vector<T>& v, ElementWriter&& writer) {
        auto start_size = uint32_t(_out->size_bytes());
        write(int32_t(v.size()));
        for (auto& elem : v) {
            writer(elem, *this);
        }
        return _out->size_bytes() - start_size;
    }

    // wrap a writer in a kafka bytes array object. the writer should return
    // true if writing no bytes should result in the encoding as nullable bytes,
    // and false otherwise.
    //
    // clang-format off
    template<typename ElementWriter>
    CONCEPT(requires requires (ElementWriter writer,
                               response_writer& rw) {
        { writer(rw) } -> bool;
    })
    // clang-format on
    uint32_t write_bytes_wrapped(ElementWriter&& writer) {
        auto* size_place_holder = _out->write_place_holder(sizeof(int32_t));
        auto start_size = uint32_t(_out->size_bytes());
        auto zero_len_is_null = writer(*this);
        int32_t real_size = _out->size_bytes() - start_size;
        // enc_size: the size prefix in the serialization
        int32_t enc_size = real_size > 0 ? real_size
                                         : (zero_len_is_null ? -1 : 0);
        auto be_size = cpu_to_be(enc_size);
        auto* in = reinterpret_cast<const bytes_ostream::value_type*>(&be_size);
        std::copy_n(in, sizeof(be_size), size_place_holder);
        return real_size + sizeof(be_size);
    }

private:
    bytes_ostream* _out;
};

} // namespace kafka::requests
