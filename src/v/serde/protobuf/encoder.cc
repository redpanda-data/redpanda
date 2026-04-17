/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/protobuf/encoder.h"

#include "serde/protobuf/wire_format.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/variant_utils.hh>

#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <variant>

namespace serde::pb {

namespace pb = google::protobuf;

namespace {

wire_type wire_type_for_field(const pb::FieldDescriptor& field) {
    switch (field.type()) {
    case pb::FieldDescriptor::TYPE_BOOL:
    case pb::FieldDescriptor::TYPE_ENUM:
    case pb::FieldDescriptor::TYPE_INT32:
    case pb::FieldDescriptor::TYPE_SINT32:
    case pb::FieldDescriptor::TYPE_UINT32:
    case pb::FieldDescriptor::TYPE_INT64:
    case pb::FieldDescriptor::TYPE_SINT64:
    case pb::FieldDescriptor::TYPE_UINT64:
        return wire_type::varint;
    case pb::FieldDescriptor::TYPE_FIXED64:
    case pb::FieldDescriptor::TYPE_SFIXED64:
    case pb::FieldDescriptor::TYPE_DOUBLE:
        return wire_type::i64;
    case pb::FieldDescriptor::TYPE_STRING:
    case pb::FieldDescriptor::TYPE_BYTES:
    case pb::FieldDescriptor::TYPE_MESSAGE:
        return wire_type::length;
    case pb::FieldDescriptor::TYPE_FIXED32:
    case pb::FieldDescriptor::TYPE_SFIXED32:
    case pb::FieldDescriptor::TYPE_FLOAT:
        return wire_type::i32;
    case pb::FieldDescriptor::TYPE_GROUP:
        throw std::runtime_error("legacy proto2 groups not supported");
    }
    __builtin_unreachable();
}

void write_tag_for_field(const pb::FieldDescriptor& field, iobuf* out) {
    tag::write(
      {.wire_type = wire_type_for_field(field), .field_number = field.number()},
      out);
}

// Write a fixed-size integer in little-endian byte order.
template<typename T>
requires std::is_integral_v<T>
void write_fixed(T val, iobuf* out) {
    T le_val = ss::cpu_to_le(val);
    char buf[sizeof(T)];
    std::memcpy(buf, &le_val, sizeof(T));
    out->append(buf, sizeof(T));
}

// Write a float/double in IEEE 754 little-endian byte order.
// ss::cpu_to_le does not support floating point types, so we use memcpy.
void write_fixed(float val, iobuf* out) {
    // IEEE 754 float is 4 bytes; protobuf stores it as-is in little-endian.
    static_assert(sizeof(float) == sizeof(uint32_t));
    uint32_t bits;
    std::memcpy(&bits, &val, sizeof(bits));
    write_fixed(bits, out);
}

void write_fixed(double val, iobuf* out) {
    static_assert(sizeof(double) == sizeof(uint64_t));
    uint64_t bits;
    std::memcpy(&bits, &val, sizeof(bits));
    write_fixed(bits, out);
}

void write_scalar_varint(
  const pb::FieldDescriptor& field, int32_t val, iobuf* out) {
    switch (field.type()) {
    case pb::FieldDescriptor::TYPE_SINT32:
        write_varint<int32_t, zigzag::yes>(val, out);
        break;
    case pb::FieldDescriptor::TYPE_INT32:
    case pb::FieldDescriptor::TYPE_ENUM:
        write_varint<int32_t, zigzag::no>(val, out);
        break;
    default:
        write_varint<int32_t, zigzag::no>(val, out);
        break;
    }
}

void write_scalar_varint(
  const pb::FieldDescriptor& field, int64_t val, iobuf* out) {
    switch (field.type()) {
    case pb::FieldDescriptor::TYPE_SINT64:
        write_varint<int64_t, zigzag::yes>(val, out);
        break;
    case pb::FieldDescriptor::TYPE_INT64:
        write_varint<int64_t, zigzag::no>(val, out);
        break;
    default:
        write_varint<int64_t, zigzag::no>(val, out);
        break;
    }
}

void write_scalar_varint(
  const pb::FieldDescriptor& /*field*/, uint32_t val, iobuf* out) {
    write_varint<uint32_t>(val, out);
}

void write_scalar_varint(
  const pb::FieldDescriptor& /*field*/, uint64_t val, iobuf* out) {
    write_varint<uint64_t>(val, out);
}

void write_scalar_varint(
  const pb::FieldDescriptor& /*field*/, bool val, iobuf* out) {
    write_varint<uint64_t>(val ? 1 : 0, out);
}

// Forward declaration for recursive encoding.
void encode_message(
  const parsed::message& msg, const pb::Descriptor& desc, iobuf* out);

void encode_field_value(
  const pb::FieldDescriptor& field,
  const parsed::message::field& value,
  iobuf* out);

void write_length_delimited_iobuf(const iobuf& buf, iobuf* out) {
    write_length(static_cast<int32_t>(buf.size_bytes()), out);
    out->append(buf.copy());
}

void encode_singular_field(
  const pb::FieldDescriptor& field,
  const parsed::message::field& value,
  iobuf* out) {
    ss::visit(
      value,
      [&](double val) {
          write_tag_for_field(field, out);
          write_fixed(val, out);
      },
      [&](float val) {
          write_tag_for_field(field, out);
          write_fixed(val, out);
      },
      [&](int32_t val) {
          write_tag_for_field(field, out);
          switch (field.type()) {
          case pb::FieldDescriptor::TYPE_SFIXED32:
              write_fixed(val, out);
              break;
          default:
              write_scalar_varint(field, val, out);
              break;
          }
      },
      [&](int64_t val) {
          write_tag_for_field(field, out);
          switch (field.type()) {
          case pb::FieldDescriptor::TYPE_SFIXED64:
              write_fixed(val, out);
              break;
          default:
              write_scalar_varint(field, val, out);
              break;
          }
      },
      [&](uint32_t val) {
          write_tag_for_field(field, out);
          switch (field.type()) {
          case pb::FieldDescriptor::TYPE_FIXED32:
              write_fixed(val, out);
              break;
          default:
              write_scalar_varint(field, val, out);
              break;
          }
      },
      [&](uint64_t val) {
          write_tag_for_field(field, out);
          switch (field.type()) {
          case pb::FieldDescriptor::TYPE_FIXED64:
              write_fixed(val, out);
              break;
          default:
              write_scalar_varint(field, val, out);
              break;
          }
      },
      [&](bool val) {
          write_tag_for_field(field, out);
          write_scalar_varint(field, val, out);
      },
      [&](const iobuf& val) {
          write_tag_for_field(field, out);
          write_length_delimited_iobuf(val, out);
      },
      [&](const std::unique_ptr<parsed::message>& val) {
          write_tag_for_field(field, out);
          iobuf nested;
          encode_message(*val, *field.message_type(), &nested);
          write_length(static_cast<int32_t>(nested.size_bytes()), out);
          out->append(std::move(nested));
      },
      [&](const parsed::repeated&) {
          // Handled separately by encode_repeated_field.
      },
      [&](const parsed::map&) {
          // Handled separately by encode_map_field.
      });
}

// Encode packed repeated elements into a length-delimited blob.
template<typename T>
void write_packed_elements(
  const pb::FieldDescriptor& field, const chunked_vector<T>& vec, iobuf* out) {
    iobuf packed;
    for (const auto& val : vec) {
        if constexpr (std::is_same_v<T, double>) {
            write_fixed(val, &packed);
        } else if constexpr (std::is_same_v<T, float>) {
            write_fixed(val, &packed);
        } else if constexpr (std::is_same_v<T, int32_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_SFIXED32:
                write_fixed(val, &packed);
                break;
            default:
                write_scalar_varint(field, val, &packed);
                break;
            }
        } else if constexpr (std::is_same_v<T, int64_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_SFIXED64:
                write_fixed(val, &packed);
                break;
            default:
                write_scalar_varint(field, val, &packed);
                break;
            }
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_FIXED32:
                write_fixed(val, &packed);
                break;
            default:
                write_scalar_varint(field, val, &packed);
                break;
            }
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_FIXED64:
                write_fixed(val, &packed);
                break;
            default:
                write_scalar_varint(field, val, &packed);
                break;
            }
        } else if constexpr (std::is_same_v<T, bool>) {
            write_scalar_varint(field, val, &packed);
        }
    }
    tag::write(
      {.wire_type = wire_type::length, .field_number = field.number()}, out);
    write_length(static_cast<int32_t>(packed.size_bytes()), out);
    out->append(std::move(packed));
}

// Encode unpacked repeated scalar elements, one tag per element.
template<typename T>
void write_unpacked_elements(
  const pb::FieldDescriptor& field, const chunked_vector<T>& vec, iobuf* out) {
    for (const auto& val : vec) {
        write_tag_for_field(field, out);
        if constexpr (std::is_same_v<T, double>) {
            write_fixed(val, out);
        } else if constexpr (std::is_same_v<T, float>) {
            write_fixed(val, out);
        } else if constexpr (std::is_same_v<T, int32_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_SFIXED32:
                write_fixed(val, out);
                break;
            default:
                write_scalar_varint(field, val, out);
                break;
            }
        } else if constexpr (std::is_same_v<T, int64_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_SFIXED64:
                write_fixed(val, out);
                break;
            default:
                write_scalar_varint(field, val, out);
                break;
            }
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_FIXED32:
                write_fixed(val, out);
                break;
            default:
                write_scalar_varint(field, val, out);
                break;
            }
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            switch (field.type()) {
            case pb::FieldDescriptor::TYPE_FIXED64:
                write_fixed(val, out);
                break;
            default:
                write_scalar_varint(field, val, out);
                break;
            }
        } else if constexpr (std::is_same_v<T, bool>) {
            write_scalar_varint(field, val, out);
        }
    }
}

void encode_repeated_field(
  const pb::FieldDescriptor& field, const parsed::repeated& rep, iobuf* out) {
    bool packed = field.is_packed();
    std::visit(
      [&](const auto& vec) {
          using vec_t = std::decay_t<decltype(vec)>;
          using elem_t = typename vec_t::value_type;
          if constexpr (std::is_same_v<elem_t, iobuf>) {
              // String/bytes are never packed, each element gets its own tag.
              for (const auto& val : vec) {
                  write_tag_for_field(field, out);
                  write_length_delimited_iobuf(val, out);
              }
          } else if constexpr (
            std::is_same_v<elem_t, std::unique_ptr<parsed::message>>) {
              // Messages are never packed, each element gets its own tag.
              for (const auto& val : vec) {
                  write_tag_for_field(field, out);
                  iobuf nested;
                  encode_message(*val, *field.message_type(), &nested);
                  write_length(static_cast<int32_t>(nested.size_bytes()), out);
                  out->append(std::move(nested));
              }
          } else {
              // Scalar repeated fields: packed or unpacked.
              if (packed) {
                  write_packed_elements(field, vec, out);
              } else {
                  write_unpacked_elements(field, vec, out);
              }
          }
      },
      rep.elements);
}

void encode_map_entry_key(
  const pb::FieldDescriptor& key_field,
  const parsed::map::key& key,
  iobuf* entry_buf) {
    ss::visit(
      key,
      [](const std::monostate&) {
          // Default key, don't write anything.
      },
      [&](const auto& val) {
          parsed::message::field f(val);
          encode_singular_field(key_field, f, entry_buf);
      },
      [&](const iobuf& val) {
          parsed::message::field f(val.copy());
          encode_singular_field(key_field, f, entry_buf);
      });
}

void encode_map_entry_value(
  const pb::FieldDescriptor& val_field,
  const parsed::map::value& value,
  iobuf* entry_buf) {
    ss::visit(
      value,
      [](const std::monostate&) {
          // Default value, don't write anything.
      },
      [&](const auto& val) {
          parsed::message::field f(val);
          encode_singular_field(val_field, f, entry_buf);
      },
      [&](const iobuf& val) {
          parsed::message::field f(val.copy());
          encode_singular_field(val_field, f, entry_buf);
      },
      [&](const std::unique_ptr<parsed::message>& val) {
          write_tag_for_field(val_field, entry_buf);
          iobuf nested;
          encode_message(*val, *val_field.message_type(), &nested);
          write_length(static_cast<int32_t>(nested.size_bytes()), entry_buf);
          entry_buf->append(std::move(nested));
      });
}

void encode_map_field(
  const pb::FieldDescriptor& field, const parsed::map& map, iobuf* out) {
    const auto* entry_desc = field.message_type();
    const auto* key_field = entry_desc->map_key();
    const auto* val_field = entry_desc->map_value();

    for (const auto& [k, v] : map.entries) {
        iobuf entry_buf;
        encode_map_entry_key(*key_field, k, &entry_buf);
        encode_map_entry_value(*val_field, v, &entry_buf);

        // Write the map entry as a length-delimited message.
        tag::write(
          {.wire_type = wire_type::length, .field_number = field.number()},
          out);
        write_length(static_cast<int32_t>(entry_buf.size_bytes()), out);
        out->append(std::move(entry_buf));
    }
}

void encode_field_value(
  const pb::FieldDescriptor& field,
  const parsed::message::field& value,
  iobuf* out) {
    if (std::holds_alternative<parsed::map>(value)) {
        encode_map_field(field, std::get<parsed::map>(value), out);
    } else if (std::holds_alternative<parsed::repeated>(value)) {
        encode_repeated_field(field, std::get<parsed::repeated>(value), out);
    } else {
        encode_singular_field(field, value, out);
    }
}

void encode_message(
  const parsed::message& msg, const pb::Descriptor& desc, iobuf* out) {
    // Collect field numbers and sort them for deterministic output.
    chunked_vector<int32_t> field_numbers;
    field_numbers.reserve(msg.fields.size());
    for (const auto& [num, _] : msg.fields) {
        field_numbers.push_back(num);
    }
    std::sort(field_numbers.begin(), field_numbers.end());

    for (auto field_number : field_numbers) {
        const auto* field_desc = desc.FindFieldByNumber(field_number);
        if (field_desc == nullptr) {
            continue;
        }
        auto it = msg.fields.find(field_number);
        if (it == msg.fields.end()) {
            continue;
        }
        encode_field_value(*field_desc, it->second, out);
    }
}

} // namespace

ss::future<iobuf>
encode(const parsed::message& msg, const pb::Descriptor& desc) {
    iobuf out;
    encode_message(msg, desc, &out);
    co_return out;
}

} // namespace serde::pb
