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

#include "serde/avro/encoder.h"

#include "utils/vint.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include <boost/range/irange.hpp>

#include <cstring>
#include <stdexcept>

namespace serde::avro {

namespace {

// Maximum nesting depth, matching the parser limit.
static constexpr int max_nested_depth = 100;

struct encoder_state {
    int level{0};
};

class encoder {
public:
    explicit encoder(const ::avro::ValidSchema& schema)
      : _schema(&schema) {}

    ss::future<iobuf> encode_message(const parsed::message& msg) {
        encoder_state state;
        co_await encode_node(msg, _schema->root(), state);
        co_return std::move(_out);
    }

private:
    void write_zigzag_varint(int64_t value) {
        uint8_t buf[vint::max_length];
        auto sz = vint::serialize(value, buf);
        _out.append(buf, sz);
    }

    void write_raw_bytes(const iobuf& buf) {
        if (buf.empty()) {
            return;
        }
        // Copy the iobuf contents into the output
        for (const auto& frag : buf) {
            _out.append(frag.get(), frag.size());
        }
    }

    void write_float(float value) {
        // Avro encodes floats as 4 bytes little-endian.
        char buf[sizeof(float)];
        std::memcpy(buf, &value, sizeof(float));
        _out.append(buf, sizeof(float));
    }

    void write_double(double value) {
        // Avro encodes doubles as 8 bytes little-endian.
        char buf[sizeof(double)];
        std::memcpy(buf, &value, sizeof(double));
        _out.append(buf, sizeof(double));
    }

    void write_bool(bool value) {
        uint8_t byte = value ? 1 : 0;
        _out.append(&byte, 1);
    }

    void write_bytes_with_length(const iobuf& buf) {
        write_zigzag_varint(static_cast<int64_t>(buf.size_bytes()));
        write_raw_bytes(buf);
    }

    ss::future<> encode_primitive(
      const parsed::primitive& prim, const ::avro::NodePtr& node) {
        std::visit(
          [this, &node](auto&& val) {
              using T = std::decay_t<decltype(val)>;
              if constexpr (std::is_same_v<T, parsed::avro_null>) {
                  // null: write nothing
              } else if constexpr (std::is_same_v<T, bool>) {
                  write_bool(val);
              } else if constexpr (std::is_same_v<T, int32_t>) {
                  write_zigzag_varint(val);
              } else if constexpr (std::is_same_v<T, int64_t>) {
                  write_zigzag_varint(val);
              } else if constexpr (std::is_same_v<T, float>) {
                  write_float(val);
              } else if constexpr (std::is_same_v<T, double>) {
                  write_double(val);
              } else if constexpr (std::is_same_v<T, iobuf>) {
                  if (node->type() == ::avro::AVRO_FIXED) {
                      // Fixed: write raw bytes without length prefix
                      write_raw_bytes(val);
                  } else {
                      // String or bytes: write length-prefixed
                      write_bytes_with_length(val);
                  }
              }
          },
          prim);
        co_return;
    }

    ss::future<> encode_node(
      const parsed::message& msg,
      const ::avro::NodePtr& node,
      encoder_state& state) {
        state.level++;
        auto decrement_on_exit = ss::defer([&state] { state.level--; });
        if (state.level >= max_nested_depth) {
            throw std::invalid_argument(
              fmt::format(
                "max nested field depth of {} reached during encoding",
                max_nested_depth));
        }
        // Resolve symbolic references before dispatch.
        auto resolved = node->type() == ::avro::AVRO_SYMBOLIC
                          ? ::avro::resolveSymbol(node)
                          : node;

        co_await std::visit(
          [this, &resolved, &state](auto&& val) -> ss::future<> {
              using T = std::decay_t<decltype(val)>;
              if constexpr (std::is_same_v<T, parsed::primitive>) {
                  return encode_primitive(val, resolved);
              } else if constexpr (std::is_same_v<T, parsed::record>) {
                  return encode_record(val, resolved, state);
              } else if constexpr (std::is_same_v<T, parsed::map>) {
                  return encode_map(val, resolved, state);
              } else if constexpr (std::is_same_v<T, parsed::list>) {
                  return encode_list(val, resolved, state);
              } else if constexpr (std::is_same_v<T, parsed::avro_union>) {
                  return encode_union(val, resolved, state);
              }
          },
          msg);
    }

    ss::future<> encode_record(
      const parsed::record& rec,
      const ::avro::NodePtr& node,
      encoder_state& state) {
        for (auto i : boost::irange<size_t>(node->leaves())) {
            co_await encode_node(*rec.fields[i], node->leafAt(i), state);
        }
    }

    ss::future<> encode_map(
      const parsed::map& m, const ::avro::NodePtr& node, encoder_state& state) {
        if (!m.entries.empty()) {
            // Write block count
            write_zigzag_varint(static_cast<int64_t>(m.entries.size()));
            // Write each key-value pair
            for (const auto& [key, value] : m.entries) {
                write_bytes_with_length(key);
                co_await encode_node(*value, node->leafAt(1), state);
            }
        }
        // Write terminating zero block
        write_zigzag_varint(0);
    }

    ss::future<> encode_list(
      const parsed::list& lst,
      const ::avro::NodePtr& node,
      encoder_state& state) {
        if (!lst.elements.empty()) {
            // Write block count
            write_zigzag_varint(static_cast<int64_t>(lst.elements.size()));
            // Write each element
            for (const auto& elem : lst.elements) {
                co_await encode_node(*elem, node->leafAt(0), state);
            }
        }
        // Write terminating zero block
        write_zigzag_varint(0);
    }

    ss::future<> encode_union(
      const parsed::avro_union& u,
      const ::avro::NodePtr& node,
      encoder_state& state) {
        write_zigzag_varint(static_cast<int64_t>(u.branch));
        co_await encode_node(*u.message, node->leafAt(u.branch), state);
    }

    iobuf _out;
    const ::avro::ValidSchema* _schema;
};

} // namespace

ss::future<iobuf>
encode(const parsed::message& msg, const ::avro::ValidSchema& schema) {
    encoder enc(schema);
    co_return co_await enc.encode_message(msg);
}

} // namespace serde::avro
