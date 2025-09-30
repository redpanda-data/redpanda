/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/values_json.h"

#include "bytes/iobuf_parser.h"
#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/ir_json.h"
#include "iceberg/conversion/time_rfc3339.h"
#include "iceberg/values.h"
#include "serde/json/parser.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <memory>
#include <optional>
#include <variant>

namespace iceberg {

namespace {

std::optional<primitive_value>
convert_primitive(serde::json::parser& p, const primitive_type& ft) {
    using token = serde::json::token;

    switch (p.token()) {
    case token::value_null:
        return std::nullopt;
    case token::value_true:
        if (!std::holds_alternative<boolean_type>(ft)) {
            throw value_conversion_exception(
              fmt::format(
                "Mismatch json between json boolean value and schema type: {}",
                ft));
        }
        return iceberg::boolean_value(true);
    case token::value_false:
        if (!std::holds_alternative<boolean_type>(ft)) {
            throw value_conversion_exception(
              fmt::format(
                "Mismatch json between json boolean value and schema type: {}",
                ft));
        }
        return iceberg::boolean_value(false);
    case token::value_int:
        if (std::holds_alternative<long_type>(ft)) {
            return iceberg::long_value(p.value_int());
        } else if (std::holds_alternative<double_type>(ft)) {
            return iceberg::double_value(static_cast<double>(p.value_int()));
        } else {
            throw value_conversion_exception(
              fmt::format(
                "Mismatch json between json integer value and schema type: {}",
                ft));
        }
    case token::value_double:
        if (std::holds_alternative<double_type>(ft)) {
            return iceberg::double_value(p.value_double());
        } else if (std::holds_alternative<long_type>(ft)) {
            double source_value = p.value_double();
            // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
            double int_part;
            const bool is_integer = (std::modf(source_value, &int_part) == 0.0);
            const bool can_be_represented_as_iceberg_long
              = is_integer
                && (int_part >= static_cast<double>(std::numeric_limits<int64_t>::min()) && int_part <= static_cast<double>(std::numeric_limits<int64_t>::max()));
            if (!can_be_represented_as_iceberg_long) {
                throw value_conversion_exception(
                  fmt::format(
                    "Cannot convert non-integer double value {} to integer "
                    "without precision loss",
                    source_value));
            }
            return iceberg::long_value(static_cast<int64_t>(int_part));
        } else {
            throw value_conversion_exception(
              fmt::format(
                "Mismatch json between json double value and schema type: {}",
                ft));
        }
    case token::value_string: {
        // Trivial case for string type.
        if (std::holds_alternative<string_type>(ft)) {
            return iceberg::string_value(p.value_string());
        }

        auto linearize = [](iobuf buf, size_t max_bytes) {
            if (buf.size_bytes() > max_bytes) {
                throw value_conversion_exception(
                  fmt::format(
                    "String value exceeds maximum length of {} bytes: {}",
                    max_bytes,
                    buf.size_bytes()));
            }

            iobuf_const_parser iop(buf);
            return iop.read_string(iop.bytes_left());
        };

        // 32 bytes should be enough for most date/time formats.
        // There can be unlimited trailing digits for fractional
        // seconds but 32 should be more than enough.
        // Example: 2025-01-01T01:02:03.11111111111[snip]Z
        constexpr size_t max_date_time_format_length = 32;

        if (std::holds_alternative<timestamptz_type>(ft)) {
            auto parse_input = linearize(
              p.value_string(), max_date_time_format_length);
            return conversion::time_rfc3339::date_time_str_to_timestampz(
                     parse_input)
              .value();
        } else if (std::holds_alternative<date_type>(ft)) {
            auto parse_input = linearize(
              p.value_string(), max_date_time_format_length);
            return conversion::time_rfc3339::date_str_to_date(parse_input)
              .value();
        } else if (std::holds_alternative<time_type>(ft)) {
            auto parse_input = linearize(
              p.value_string(), max_date_time_format_length);
            return conversion::time_rfc3339::time_str_to_time(parse_input)
              .value();
        }

        throw value_conversion_exception(
          fmt::format(
            "Mismatch json between json string value and schema type: {}", ft));
    }
    default:
        throw value_conversion_exception(
          fmt::format("Unexpected JSON token type: {}", p.token()));
    }
}

ss::future<> decode_struct(
  serde::json::parser& p,
  struct_value& sv,
  const struct_type& st,
  const json_conversion_ir::struct_field_map_t& field_map);

ss::future<> decode_list(
  serde::json::parser& p,
  list_value& lv,
  const list_type& lt,
  const json_conversion_ir::struct_field_map_t& field_map);

ss::future<> decode_field(
  serde::json::parser& p,
  std::optional<iceberg::value>& v,
  const field_type& ft,
  const json_conversion_ir::struct_field_map_t& field_map) {
    struct field_decoder_visitor {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
        serde::json::parser& p;
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
        const json_conversion_ir::struct_field_map_t& field_map;

        ss::future<std::optional<value>> operator()(const primitive_type& ft) {
            co_return convert_primitive(p, ft);
        }
        ss::future<std::optional<value>> operator()(const struct_type& st) {
            auto sv = std::make_unique<struct_value>();
            co_await decode_struct(p, *sv, st, field_map);
            co_return std::move(sv);
        }
        ss::future<std::optional<value>> operator()(const list_type& lt) {
            auto lv = std::make_unique<list_value>();
            co_await decode_list(p, *lv, lt, field_map);
            co_return std::move(lv);
        }
        ss::future<std::optional<value>> operator()(const map_type&) {
            throw value_conversion_exception(
              "Map type is not supported in JSON deserialization");
        }
    };

    v = co_await std::visit(
      field_decoder_visitor{
        p,
        field_map,
      },
      ft);
}

ss::future<> decode_struct(
  serde::json::parser& p,
  struct_value& sv,
  const struct_type& st,
  const json_conversion_ir::struct_field_map_t& field_map) {
    if (p.token() != serde::json::token::start_object) {
        throw value_conversion_exception(
          fmt::format(
            "Expected start of JSON object for struct type but got {}",
            p.token()));
    }

    // Placeholder fields.
    for (size_t i = 0; i < st.fields.size(); ++i) {
        sv.fields.emplace_back();
    }

    while (true) {
        if (!co_await p.next()) {
            throw value_conversion_exception(
              "Failed to read next JSON token after start object");
        }

        if (p.token() == serde::json::token::end_object) {
            for (size_t i = 0; i < st.fields.size(); ++i) {
                if (st.fields[i]->required && !sv.fields[i].has_value()) {
                    throw value_conversion_exception(
                      fmt::format(
                        "Required field {} is missing", st.fields[i]->name));
                }
            }
            co_return;
        }

        if (p.token() != serde::json::token::key) {
            throw value_conversion_exception(
              fmt::format(
                "Expected key token in JSON object but got {}", p.token()));
        }

        auto key = p.value_string();

        auto key_len = key.size_bytes();
        iobuf_parser iop(std::move(key));
        auto key_as_string = iop.read_string(key_len);

        auto it = field_map.find(key_as_string);

        if (it == field_map.end()) {
            // Key not found in the mapping, skip it.
            // todo throw if extra keys not allowed?
            // skip the value
            co_await p.skip_value();
            continue;
        }

        // Decode the value for this key.
        if (!co_await p.next()) {
            throw value_conversion_exception(
              "Failed to read next JSON token after key");
        }

        co_await decode_field(
          p,
          sv.fields[it->second.field_pos],
          st.fields[it->second.field_pos]->type,
          it->second.nested_fields);
    }
}

ss::future<> decode_list(
  serde::json::parser& p,
  list_value& lv,
  const list_type& lt,
  const json_conversion_ir::struct_field_map_t& field_map) {
    if (p.token() != serde::json::token::start_array) {
        throw value_conversion_exception(
          "Expected start of JSON array for list type");
    }

    while (co_await p.next()) {
        if (p.token() == serde::json::token::end_array) {
            co_return;
        }

        co_await decode_field(
          p, lv.elements.emplace_back(), lt.element_field->type, field_map);
    }
}

}; // namespace

ss::future<value_outcome>
deserialize_json_impl(iobuf buf, const json_conversion_ir& ir) {
    std::optional<iceberg::value> root_value;

    serde::json::parser p(std::move(buf));

    if (!co_await p.next()) {
        throw value_conversion_exception("Failed to read next JSON token");
    }

    co_await decode_field(p, root_value, ir.root(), ir.struct_field_map());

    // Wrap the root value in a struct_value if it is not already one.
    if (
      root_value.has_value()
      && std::holds_alternative<std::unique_ptr<struct_value>>(
        root_value.value())) {
        co_return std::move(root_value.value());
    } else {
        auto sv = std::make_unique<struct_value>();
        sv->fields.push_back(std::move(root_value));
        co_return std::move(sv);
    }
}

ss::future<value_outcome>
deserialize_json(iobuf buf, const json_conversion_ir& ir) {
    try {
        // Firewall for exceptions during deserialization because the caller
        // does not expect exceptions to be thrown.
        co_return co_await deserialize_json_impl(std::move(buf), ir);
    } catch (const value_conversion_exception& e) {
        co_return e;
    } catch (std::exception& e) {
        co_return value_conversion_exception(
          fmt::format("Exception during JSON deserialization: {}", e.what()));
    }
}

} // namespace iceberg
