/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/partition_key_path.h"

#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "datalake/logger.h"
#include "http/utils.h"
#include "ssx/sformat.h"
#include "utils/base64.h"

#include <fmt/chrono.h>
namespace datalake {
namespace {
// AWS S3 path size limit is 1024 bytes, we allow a single key to be up to 64
// bytes of size.
static constexpr size_t max_key_value_length = 64;
static constexpr size_t max_path_length = 512;

size_t limited_size(const iobuf& original) {
    return std::min(original.size_bytes(), max_key_value_length);
}

std::chrono::microseconds
get_sub_seconds(const std::chrono::system_clock::time_point& tp) {
    return std::chrono::microseconds(tp.time_since_epoch().count())
           - std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::duration_cast<std::chrono::seconds>(
               tp.time_since_epoch()));
}
/**
 * Return true if the given time point can not be represented as full
 * milliseconds and it requires microsecond precision
 */
bool has_millisecond_fraction(std::chrono::microseconds sub_seconds_us) {
    // round to full milliseconds
    const auto sub_seconds_ms
      = std::chrono::duration_cast<std::chrono::milliseconds>(sub_seconds_us);

    return sub_seconds_us
             - std::chrono::duration_cast<std::chrono::microseconds>(
               sub_seconds_ms)
           != std::chrono::microseconds(0);
}

ss::sstring format_timestamp(size_t value, bool include_zone) {
    std::chrono::system_clock::time_point tp{std::chrono::microseconds(value)};
    const auto sub_seconds_us = get_sub_seconds(tp);
    // Truncate to seconds so that %S does not emit fractional seconds
    // (fmt >=10 includes sub-second digits in %S when the time_point
    // carries sub-second precision).
    const auto tp_s = std::chrono::floor<std::chrono::seconds>(tp);

    if (sub_seconds_us == std::chrono::microseconds(0)) {
        if (include_zone) {
            return ssx::sformat("{:%Y-%m-%dT%H:%M:%S}{:%z}", tp_s, tp_s);
        }
        return ssx::sformat("{:%Y-%m-%dT%H:%M:%S}Z", tp_s);
    }

    if (has_millisecond_fraction(sub_seconds_us)) {
        if (include_zone) {
            return ssx::sformat(
              "{:%Y-%m-%dT%H:%M:%S}.{:06}{:%z}",
              tp_s,
              get_sub_seconds(tp).count(),
              tp_s);
        }
        return ssx::sformat(
          "{:%Y-%m-%dT%H:%M:%S}.{:06}Z", tp_s, get_sub_seconds(tp).count());
    }

    // no millisecond fraction
    const auto sub_seconds_ms
      = std::chrono::duration_cast<std::chrono::milliseconds>(sub_seconds_us);

    if (include_zone) {
        return ssx::sformat(
          "{:%Y-%m-%dT%H:%M:%S}.{:03}{:%z}",
          tp_s,
          sub_seconds_ms.count(),
          tp_s);
    }
    return ssx::sformat(
      "{:%Y-%m-%dT%H:%M:%S}.{:03}Z", tp_s, sub_seconds_ms.count());
}
/**
 * Default formatting rules used for identity type
 */
struct primitive_formatting_visitor {
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::boolean_value& v) {
        return fmt::to_string(v.val);
    }
    checked<ss::sstring, partition_key_error> operator()(const auto& v) {
        return fmt::to_string(v.val);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::date_value& v) {
        const std::chrono::system_clock::time_point tp{
          std::chrono::days(v.val)};

        return ssx::sformat("{:%Y-%m-%d}", tp);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::time_value& v) {
        std::chrono::hh_mm_ss tp(std::chrono::microseconds(v.val));
        const auto sub_seconds_us = std::chrono::microseconds(
          tp.subseconds().count());
        if (sub_seconds_us == std::chrono::microseconds(0)) {
            return ssx::sformat(
              "{:02}:{:02}:{:02}",
              tp.hours().count(),
              tp.minutes().count(),
              tp.seconds().count());
        }

        if (has_millisecond_fraction(sub_seconds_us)) {
            return ssx::sformat(
              "{:02}:{:02}:{:02}.{:06}",
              tp.hours().count(),
              tp.minutes().count(),
              tp.seconds().count(),
              sub_seconds_us.count());
        }
        const auto sub_seconds_ms
          = std::chrono::duration_cast<std::chrono::milliseconds>(
            sub_seconds_us);

        return ssx::sformat(
          "{:02}:{:02}:{:02}.{:03}",
          tp.hours().count(),
          tp.minutes().count(),
          tp.seconds().count(),
          sub_seconds_ms.count());
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::timestamp_value& v) {
        return format_timestamp(v.val, false);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::timestamptz_value& v) {
        return format_timestamp(v.val, true);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::string_value& v) {
        iobuf_const_parser p(v.val);
        auto ret = p.read_string(limited_size(v.val));
        return ret;
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::uuid_value& v) {
        return fmt::to_string(v.val);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::fixed_value& v) {
        return iobuf_to_base64_string(v.val, limited_size(v.val));
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::binary_value& v) {
        return iobuf_to_base64_string(v.val, limited_size(v.val));
    }
};

template<typename DurationT>
checked<ss::sstring, partition_key_error>
format_time_transform_key(const iceberg::primitive_value& value) {
    // Per the Iceberg spec, the `day` transform produces a date value;
    // year/month/hour transforms produce int values. Both wrap an int32_t.
    int32_t raw_val{};
    if (std::holds_alternative<iceberg::int_value>(value)) {
        raw_val = std::get<iceberg::int_value>(value).val;
    } else if (std::holds_alternative<iceberg::date_value>(value)) {
        raw_val = std::get<iceberg::date_value>(value).val;
    } else {
        return partition_key_error(
          "time transform expects an integer or date value");
    }

    std::chrono::system_clock::time_point tp{std::chrono::milliseconds(0)};
    // offset epoch by the given duration
    tp += DurationT(raw_val);

    if constexpr (std::is_same_v<std::chrono::years, DurationT>) {
        return ssx::sformat("{:%Y}", tp);
    }

    if constexpr (std::is_same_v<std::chrono::months, DurationT>) {
        return ssx::sformat("{:%Y-%m}", tp);
    }

    if constexpr (std::is_same_v<std::chrono::days, DurationT>) {
        return ssx::sformat("{:%Y-%m-%d}", tp);
    }

    if constexpr (std::is_same_v<std::chrono::hours, DurationT>) {
        return ssx::sformat("{:%Y-%m-%d-%H}", tp);
    }
}

struct transform_value_formatting_visitor {
    explicit transform_value_formatting_visitor(
      const iceberg::primitive_value& v)
      : value(v) {}
    // This case handles:
    // - Identity transform
    // - Truncation transform
    checked<ss::sstring, partition_key_error> operator()(const auto&) {
        return std::visit(primitive_formatting_visitor{}, value);
    }

    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::year_transform&) {
        return format_time_transform_key<std::chrono::years>(value);
    }

    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::month_transform&) {
        return format_time_transform_key<std::chrono::months>(value);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::day_transform&) {
        return format_time_transform_key<std::chrono::days>(value);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::hour_transform&) {
        return format_time_transform_key<std::chrono::hours>(value);
    }
    checked<ss::sstring, partition_key_error>
    operator()(const iceberg::void_transform&) {
        return "null";
    }

    const iceberg::primitive_value& value;
};

checked<ss::sstring, partition_key_error> escape(std::string_view s) {
    if (!is_valid_utf8(s)) {
        return partition_key_error("Invalid UTF-8 string, unable to escape");
    }

    return http::uri_encode(s, http::uri_encode_slash::no);
}

checked<ss::sstring, partition_key_error> format_field_value(
  const iceberg::transform& transform,
  const std::optional<iceberg::value>& value) {
    if (!value) {
        return "null";
    }
    if (!std::holds_alternative<iceberg::primitive_value>(*value)) {
        return partition_key_error(
          "non primitive iceberg partition values are not supported");
    }
    auto res = std::visit(
      transform_value_formatting_visitor{
        std::get<iceberg::primitive_value>(*value)},
      transform);
    if (res.has_error()) {
        return res.error();
    }
    return escape(res.value());
}
} // namespace

checked<remote_path, partition_key_error> partition_key_to_path(
  const iceberg::partition_spec& spec, const iceberg::partition_key& key) {
    std::filesystem::path ret;

    if (!key.val) {
        return partition_key_error{"Partition key value is missing"};
    }

    if (spec.fields.size() != key.val->fields.size()) {
        return partition_key_error{ssx::sformat(
          "Partition key/spec mismatch.The key has {} fields, but the spec has "
          "{} fields",
          key.val->fields.size(),
          spec.fields.size())};
    }
    size_t total_length = 0;
    for (size_t i = 0; i < spec.fields.size(); i++) {
        auto& field = spec.fields[i];
        const auto& value = key.val->fields[i];
        auto formatting_res = format_field_value(field.transform, value);
        if (formatting_res.has_error()) {
            return formatting_res.error();
        }
        auto element_key = escape(field.name);
        if (element_key.has_error()) {
            return element_key.error();
        }
        auto element = ssx::sformat(
          "{}={}", element_key.value(), formatting_res.value());
        total_length += element.size() + 1;
        if (total_length > max_path_length) {
            static constexpr auto rate_limit = std::chrono::seconds(5);
            static thread_local ss::logger::rate_limit rate(rate_limit);
            vloglr(
              datalake_log,
              ss::log_level::warn,
              rate,
              "Partition key path {}, exceeds the maximum length of {} bytes, "
              "truncating",
              ret,
              max_path_length);
            break;
        }

        ret /= std::filesystem::path(std::move(element));
    }

    return remote_path(std::move(ret));
}

} // namespace datalake
