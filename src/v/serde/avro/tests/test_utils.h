/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "serde/avro/parser.h"

#include <avro/Generic.hh>

#include <memory>

namespace serde::avro::testing {

/// Convert a parsed message tree back to an Avro GenericDatum for comparison.
void parsed_to_avro(
  ::avro::GenericDatum& datum, const std::unique_ptr<parsed::message>& msg);

namespace detail {

struct primitive_visitor {
    void operator()(int32_t v) { datum->value<int32_t>() = v; }
    void operator()(int64_t v) {
        if (datum->type() == ::avro::Type::AVRO_ENUM) {
            datum->value<::avro::GenericEnum>().set(v);
        } else {
            datum->value<int64_t>() = v;
        }
    }
    void operator()(bool v) { datum->value<bool>() = v; }
    void operator()(parsed::avro_null) {}
    void operator()(double v) { datum->value<double>() = v; }
    void operator()(float v) { datum->value<float>() = v; }
    void operator()(const iobuf& buffer) {
        if (datum->type() == ::avro::Type::AVRO_FIXED) {
            auto& avro_fixed = datum->value<::avro::GenericFixed>();
            avro_fixed.value().reserve(buffer.size_bytes());
            iobuf::iterator_consumer it(buffer.cbegin(), buffer.cend());
            it.consume_to(buffer.size_bytes(), avro_fixed.value().data());
        } else if (datum->type() == ::avro::Type::AVRO_STRING) {
            auto& avro_str = datum->value<std::string>();
            iobuf_parser p(buffer.copy());
            avro_str = p.read_string(buffer.size_bytes());
        } else {
            std::vector<uint8_t> avro_bytes(buffer.size_bytes());
            iobuf::iterator_consumer it(buffer.cbegin(), buffer.cend());
            it.consume_to(buffer.size_bytes(), avro_bytes.data());
            datum->value<std::vector<uint8_t>>() = avro_bytes;
        }
    }
    ::avro::GenericDatum* datum;
};

struct parsed_msg_visitor {
    void operator()(const parsed::record& record) {
        auto& avro_record = datum->value<::avro::GenericRecord>();
        for (size_t i = 0; i < avro_record.fieldCount(); ++i) {
            auto& field_datum = avro_record.fieldAt(i);
            parsed_to_avro(field_datum, record.fields[i]);
        }
    }
    void operator()(const parsed::map& parsed_map) {
        auto& avro_map = datum->value<::avro::GenericMap>();
        for (auto& [k, v] : parsed_map.entries) {
            iobuf_const_parser p(k);
            ::avro::GenericDatum value(avro_map.schema()->leafAt(1));
            parsed_to_avro(value, v);
            auto key_str = p.read_string(k.size_bytes());
            avro_map.value().emplace_back(key_str, value);
        }
    }
    void operator()(const parsed::list& v) {
        auto& array = datum->value<::avro::GenericArray>();
        for (auto& e : v.elements) {
            ::avro::GenericDatum value(array.schema()->leafAt(0));
            parsed_to_avro(value, e);
            array.value().push_back(value);
        }
    }
    void operator()(const parsed::avro_union& v) {
        datum->selectBranch(v.branch);
        parsed_to_avro(*datum, v.message);
    }
    void operator()(const parsed::primitive& v) {
        std::visit(primitive_visitor{datum}, v);
    }
    ::avro::GenericDatum* datum;
};

} // namespace detail

inline void parsed_to_avro(
  ::avro::GenericDatum& datum, const std::unique_ptr<parsed::message>& msg) {
    std::visit(detail::parsed_msg_visitor{&datum}, *msg);
}

} // namespace serde::avro::testing
