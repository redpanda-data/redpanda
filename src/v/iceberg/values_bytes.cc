// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/values_bytes.h"

#include "bytes/iobuf_parser.h"

#include <variant>

namespace iceberg {

namespace {

struct value_to_bytes_visitor {
    template<typename T>
    requires(std::is_arithmetic_v<std::decay_t<decltype(T::val)>>)
    bytes operator()(const T& v) {
        iobuf ret;
        if constexpr (sizeof(T::val) == 1) {
            ret.append(reinterpret_cast<const char*>(&v.val), 1);
        } else if constexpr (std::is_same_v<decltype(T::val), float>) {
            const auto le_t = htole32(std::bit_cast<std::uint32_t>(v.val));
            ret.append(reinterpret_cast<const char*>(&le_t), sizeof(le_t));
        } else if constexpr (std::is_same_v<decltype(T::val), double>) {
            const auto le_t = htole64(std::bit_cast<std::uint64_t>(v.val));
            ret.append(reinterpret_cast<const char*>(&le_t), sizeof(le_t));
        } else {
            const auto le_t = ss::cpu_to_le(v.val);
            static_assert(sizeof(le_t) == sizeof(T::val));
            ret.append(reinterpret_cast<const char*>(&le_t), sizeof(le_t));
        }
        return iobuf_to_bytes(ret);
    }
    bytes operator()(const string_value& v) { return iobuf_to_bytes(v.val); }
    bytes operator()(const uuid_value& v) {
        iobuf ret;
        ret.append(v.val.to_vector().data(), uuid_t::length);
        return iobuf_to_bytes(ret);
    }
    bytes operator()(const fixed_value& v) { return iobuf_to_bytes(v.val); }
    bytes operator()(const binary_value& v) { return iobuf_to_bytes(v.val); }
    bytes operator()(const decimal_value&) {
        throw std::invalid_argument(
          "XXX: decimals as bytes not implemented yet!");
    }
};

struct value_from_bytes_visitor {
    explicit value_from_bytes_visitor(const bytes& b)
      : p(bytes_to_iobuf(b)) {}
    iobuf_parser p;

    void check_size(size_t expected_size) {
        if (expected_size != p.bytes_left()) {
            throw std::invalid_argument(fmt::format(
              "Expected {} bytes, got {}", expected_size, p.bytes_left()));
        }
    }
    template<typename T, typename V>
    value make_int_val() {
        check_size(sizeof(int32_t));
        return V{ss::le_to_cpu(p.consume_type<int32_t>())};
    }
    template<typename T, typename V>
    value make_long_val() {
        check_size(sizeof(int64_t));
        return V{ss::le_to_cpu(p.consume_type<int64_t>())};
    }
    template<typename T, typename V>
    value make_iobuf_val() {
        return V{p.copy(p.bytes_left())};
    }

    value operator()(const boolean_type&) {
        check_size(sizeof(bool));
        return boolean_value{p.consume_type<bool>()};
    }
    value operator()(const int_type&) {
        return make_int_val<int_type, int_value>();
    }
    value operator()(const long_type&) {
        return make_long_val<long_type, long_value>();
    }
    value operator()(const float_type&) {
        check_size(sizeof(float));
        return float_value{
          std::bit_cast<float>(le32toh(p.consume_type<std::uint32_t>()))};
    }
    value operator()(const double_type&) {
        check_size(sizeof(double));
        return double_value{
          std::bit_cast<double>(le64toh(p.consume_type<std::uint64_t>()))};
    }
    value operator()(const date_type&) {
        return make_int_val<date_type, date_value>();
    }
    value operator()(const time_type&) {
        return make_long_val<time_type, time_value>();
    }
    value operator()(const timestamp_type&) {
        return make_long_val<timestamp_type, timestamp_value>();
    }
    value operator()(const timestamptz_type&) {
        return make_long_val<timestamptz_type, timestamptz_value>();
    }
    value operator()(const decimal_type&) {
        throw std::invalid_argument(
          "XXX: decimals from bytes not implemented yet!");
    }
    value operator()(const uuid_type&) {
        check_size(uuid_t::length);
        auto bytes = p.peek_bytes(uuid_t::length);
        std::vector<uint8_t> v;
        v.reserve(bytes.size());
        for (const auto b : bytes) {
            v.emplace_back(b);
        }
        return uuid_value{uuid_t{v}};
    }
    value operator()(const string_type&) {
        return make_iobuf_val<string_type, string_value>();
    }
    value operator()(const fixed_type&) {
        return make_iobuf_val<fixed_type, fixed_value>();
    }
    value operator()(const binary_type&) {
        return make_iobuf_val<binary_type, binary_value>();
    }
};

} // namespace

bytes value_to_bytes(const value& v) {
    // Only primitive types are expected to be serialized as bytes, e.g. as an
    // upper/lower bound in a manifest.
    if (!std::holds_alternative<primitive_value>(v)) {
        throw std::invalid_argument(fmt::format(
          "Can only translate primitive values to bytes, got {}", v));
    }
    return std::visit(value_to_bytes_visitor{}, std::get<primitive_value>(v));
}

value value_from_bytes(const field_type& type, const bytes& b) {
    if (!std::holds_alternative<primitive_type>(type)) {
        throw std::invalid_argument(fmt::format(
          "Can only translate primitive values from bytes, got {}", type));
    }
    const auto& prim_type = std::get<primitive_type>(type);
    return std::visit(value_from_bytes_visitor{b}, prim_type);
}

} // namespace iceberg
