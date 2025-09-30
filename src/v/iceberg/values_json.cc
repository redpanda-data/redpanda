/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/values_json.h"

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "iceberg/json_utils.h"

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/range/irange.hpp>

#include <charconv>
#include <chrono>
#include <iomanip>
#include <ranges>
#include <stdexcept>

namespace {

using namespace iceberg;

std::string_view
parse_string_view(const json::Value& data, std::string_view context = "") {
    if (!data.IsString()) {
        throw std::invalid_argument(
          fmt::format(
            "Expected JSON string for {}, got {}", context, data.GetType()));
    }
    return std::string_view{data.GetString(), data.GetStringLength()};
}

iobuf hex_str_to_iobuf(std::string_view str) {
    if (str.size() & 0x01ul) {
        throw std::invalid_argument(
          fmt::format("Expected even length hex string, got {}", str.size()));
    }
    bytes b;
    b.reserve(str.size() / 2);
    while (!str.empty()) {
        auto sub = str.substr(0, 2);
        uint8_t x{};
        auto [ptr, ec] = std::from_chars(
          sub.data(), sub.data() + sub.size(), x, 16);

        if (ec != std::errc{}) {
            throw std::invalid_argument(
              fmt::format(
                "Failed to parse hex byte '{}' - ec: '{}'",
                sub,
                std::make_error_code(ec)));
        }

        b.push_back(static_cast<uint8_t>(x));
        str = str.substr(2);
    }
    return bytes_to_iobuf(b);
}

std::chrono::system_clock::time_point
sv_to_time_point(std::string_view fmt, std::string_view str) {
    // non copying input stream
    using ibufstream
      = boost::iostreams::stream<boost::iostreams::basic_array_source<char>>;

    ibufstream is(str.data(), str.size());
    std::tm tm{};
    is >> std::get_time(&tm, fmt.data());
    if (is.fail()) {
        throw std::invalid_argument(
          fmt::format(
            "Failed to parse date string '{}', expected format '{}'",
            str,
            fmt));
    }
    return absl::ToChronoTime(absl::FromTM(tm, absl::UTCTimeZone()));
}

std::chrono::microseconds sv_to_us(std::string_view u_str) {
    if (u_str.size() != 6) {
        throw std::invalid_argument(
          fmt::format(
            "Expected 6-digit microsecond resolution, got '{}'", u_str));
    }
    int us_raw{};
    auto [ptr, ec] = std::from_chars(
      u_str.data(), u_str.data() + u_str.size(), us_raw);

    if (ec != std::errc{}) {
        throw std::invalid_argument(
          fmt::format(
            "Failed to parse microseconds: '{}' - ec: {}",
            u_str,
            std::make_error_code(ec)));
    }
    return std::chrono::microseconds{us_raw};
}

struct primitive_value_parsing_visitor {
    explicit primitive_value_parsing_visitor(const json::Value& d)
      : data_(d) {}

    value operator()(const boolean_type&) {
        if (!data_.IsBool()) {
            throw std::invalid_argument(
              fmt::format("Expected bool value, got {}", data_.GetType()));
        }
        return boolean_value{data_.GetBool()};
    }
    value operator()(const int_type&) {
        if (!data_.IsInt()) {
            throw std::invalid_argument(
              fmt::format("Expected int value, got {}", data_.GetType()));
        }
        return int_value{data_.GetInt()};
    }
    value operator()(const long_type&) {
        if (!data_.IsInt64()) {
            throw std::invalid_argument(
              fmt::format("Expected long value, got {}", data_.GetType()));
        }
        return long_value{data_.GetInt64()};
    }
    value operator()(const float_type&) {
        if (!data_.IsDouble()) {
            throw std::invalid_argument(
              fmt::format("Expected float value, got {}", data_.GetType()));
        }
        auto v = data_.GetDouble();
        if (
          v > std::numeric_limits<float>::max()
          || v < std::numeric_limits<float>::min()) {
            throw std::invalid_argument(
              fmt::format("Expected float value, got double: {}", v));
        }
        return float_value{static_cast<float>(data_.GetDouble())};
    }
    value operator()(const double_type&) {
        if (!data_.IsDouble()) {
            throw std::invalid_argument(
              fmt::format("Expected double value, got {}", data_.GetType()));
        }
        return double_value{data_.GetDouble()};
    }
    value operator()(const date_type&) {
        using namespace std::chrono_literals;
        auto str = parse_string_view(data_, "date_value");
        auto tp = sv_to_time_point("%F", str);
        return date_value{
          static_cast<int32_t>(tp.time_since_epoch() / std::chrono::days{1})};
    }
    value operator()(const time_type&) {
        using namespace std::chrono_literals;
        auto str = parse_string_view(data_, "time_value");
        std::vector<std::string_view> split = absl::StrSplit(str, '.');
        if (split.size() != 2) {
            throw std::invalid_argument(
              fmt::format(
                "Expected fractional part for time_value, got '{}'", str));
        }
        auto t_str = split[0];
        auto u_str = split[1];
        auto us_sub_sec = sv_to_us(u_str);
        auto since_midnight = sv_to_time_point("%T", t_str)
                              - sv_to_time_point("%T", "00:00:00");
        return time_value{(since_midnight + us_sub_sec) / 1us};
    }
    value operator()(const timestamp_type&) {
        using namespace std::chrono_literals;
        auto str = parse_string_view(data_, "timestamp_value");
        std::vector<std::string_view> split = absl::StrSplit(str, '.');
        if (split.size() != 2) {
            throw std::invalid_argument(
              fmt::format(
                "Expected fractional part for timestamp_value, got '{}'", str));
        }
        auto t_str = split[0];
        auto u_str = split[1];
        auto us_sub_sec = sv_to_us(u_str);
        auto since_epoch = sv_to_time_point("%FT%T", t_str).time_since_epoch();
        return timestamp_value{(since_epoch + us_sub_sec) / 1us};
    }
    value operator()(const timestamptz_type&) {
        using namespace std::chrono_literals;
        auto str = parse_string_view(data_, "timestamptz_value");
        std::vector<std::string_view> split = absl::StrSplit(str, '.');
        if (split.size() != 2) {
            throw std::invalid_argument(
              fmt::format(
                "Expected fractional part for timestamptz_value, got '{}'",
                str));
        }
        auto t_str = split[0];
        auto rest = split[1];
        split = absl::StrSplit(rest, '+');
        if (split.size() != 2) {
            throw std::invalid_argument(
              fmt::format(
                "Expected offset part for timestamptz_value, got '{}'", str));
        }
        auto u_str = split[0];
        auto us_sub_sec = sv_to_us(u_str);
        auto since_epoch = sv_to_time_point("%FT%T", t_str).time_since_epoch();
        return timestamptz_value{(since_epoch + us_sub_sec) / 1us};
    }
    value operator()(const string_type&) {
        auto str = parse_string_view(data_, "string_value");
        return string_value{iobuf::from(str)};
    }
    value operator()(const fixed_type& t) {
        auto str = parse_string_view(data_, "fixed_value");
        if (t.length * 2 != str.size()) {
            throw std::invalid_argument(
              fmt::format(
                "Expected {} hex digits for {}: got {}",
                t.length * 2,
                t,
                str.size()));
        }
        return fixed_value{hex_str_to_iobuf(str)};
    }
    value operator()(const uuid_type&) {
        auto str = parse_string_view(data_, "uuid_value");
        try {
            return uuid_value{uuid_t::from_string(str)};
        } catch (const boost::wrapexcept<std::runtime_error>& e) {
            throw std::invalid_argument(
              fmt::format("Failed to parse uuid: {}", e.what()));
        }
    }
    value operator()(const binary_type&) {
        auto str = parse_string_view(data_, "binary_value");
        return binary_value{hex_str_to_iobuf(str)};
    }
    value operator()(const decimal_type& t) {
        // TODO(oren): need to support negative scale? see datatypes.h
        auto str = parse_string_view(data_, "decimal_value");

        std::vector<std::string_view> split = absl::StrSplit(str, '.');
        if (split.size() > 2) {
            throw std::invalid_argument(
              fmt::format("Too many decimal points for {}: '{}'", t, str));
        }

        auto int_part = split[0];
        bool is_neg = !int_part.empty() && int_part.front() == '-';
        if (is_neg) {
            int_part = int_part.substr(1);
        }

        auto nz_it = std::ranges::find_if_not(
          int_part, [](char c) { return c == '0'; });
        int_part = std::string_view{nz_it, int_part.end()};

        auto frac_part = split.size() == 2 ? split[1] : std::string_view{};

        if (auto json_scale = frac_part.size(); json_scale > t.scale) {
            // TODO(oren): should we round up rather than truncate?
            frac_part = frac_part.substr(0, t.scale);
        }

        auto json_precision = int_part.size() + frac_part.size();

        if (json_precision > t.precision) {
            throw std::invalid_argument(
              fmt::format(
                "Expected at most {}-byte precision for {}, got {}",
                t.precision,
                t,
                json_precision));
        }

        absl::int128 integral{0};
        if (!int_part.empty() && !absl::SimpleAtoi(int_part, &integral)) {
            throw std::invalid_argument(
              fmt::format("Failed to parse int128 from '{}'", int_part));
        }

        absl::int128 frac{0};
        if (!frac_part.empty() && !absl::SimpleAtoi(frac_part, &frac)) {
            throw std::invalid_argument(
              fmt::format("Failed to parse int128 from '{}'", frac_part));
        }

        std::ranges::for_each(
          boost::irange(0u, t.scale),
          [&integral](const auto) { integral *= 10; });

        return decimal_value{(integral + frac) * (is_neg ? -1 : 1)};
    }

private:
    const json::Value& data_;
};

struct value_parsing_visitor {
    explicit value_parsing_visitor(const json::Value& d)
      : data_(d) {}

    value operator()(const primitive_type& t) {
        return std::visit(primitive_value_parsing_visitor{data_}, t);
    }
    value operator()(const list_type& t) {
        if (!data_.IsArray()) {
            throw std::invalid_argument("Expected a JSON array for list_value");
        }
        auto v = std::make_unique<list_value>();
        const auto& arr = data_.GetArray();
        std::ranges::transform(
          arr, std::back_inserter(v->elements), [&t](const auto& elt) {
              return value_from_json(
                elt, t.element_field->type, t.element_field->required);
          });
        return v;
    }
    value operator()(const struct_type& t) {
        if (!data_.IsObject()) {
            throw std::invalid_argument(
              "Expected a JSON object for struct_value");
        }
        auto obj = data_.GetObject();
        if (t.fields.size() != obj.MemberCount()) {
            throw std::invalid_argument(
              fmt::format(
                "Expected JSON object with {} members, got {}",
                t.fields.size(),
                obj.MemberCount()));
        }

        auto v = std::make_unique<struct_value>();
        v->fields.reserve(t.fields.size());
        std::ranges::transform(
          boost::irange(0ul, t.fields.size()),
          std::back_inserter(v->fields),
          [&t, &obj](const auto i) {
              const auto& t_field = t.fields[i];
              const auto& o_member = *(obj.MemberBegin() + i);
              if (t_field == nullptr) {
                  throw std::invalid_argument(
                    fmt::format("Null field in struct type {}", t));
              }
              return value_from_json(
                o_member.value, t_field->type, t_field->required);
          });

        return v;
    }
    value operator()(const map_type& t) {
        const auto& keys_arr = parse_required_array(data_, "keys");
        const auto& values_arr = parse_required_array(data_, "values");
        if (keys_arr.Size() != values_arr.Size()) {
            throw std::invalid_argument(
              fmt::format(
                "Expected complete key-value mapping, got {} keys and {} "
                "values",
                keys_arr.Size(),
                values_arr.Size()));
        }

        auto v = std::make_unique<map_value>();
        v->kvs.reserve(keys_arr.Size());
        std::ranges::transform(
          boost::irange(0u, keys_arr.Size()),
          std::back_inserter(v->kvs),
          [&t, &keys_arr, &values_arr](const auto i) -> kv_value {
              return {
                value_from_json(
                  keys_arr[i], t.key_field->type, field_required::yes)
                  .value(),
                value_from_json(
                  values_arr[i], t.value_field->type, t.value_field->required)};
          });

        return v;
    }

private:
    const json::Value& data_;
};

void value_to_json(
  iceberg::json_writer&,
  const iceberg::primitive_value&,
  const iceberg::primitive_type&);
void value_to_json(
  iceberg::json_writer&,
  const iceberg::struct_value&,
  const iceberg::struct_type&);
void value_to_json(
  iceberg::json_writer&, const iceberg::list_value&, const iceberg::list_type&);
void value_to_json(
  iceberg::json_writer&, const iceberg::map_value&, const iceberg::map_type&);

struct rjson_visitor {
    explicit rjson_visitor(iceberg::json_writer& w)
      : w(w) {}
    void
    operator()(const iceberg::boolean_value& v, const iceberg::boolean_type&) {
        w.Bool(v.val);
    }
    void operator()(const iceberg::int_value& v, const iceberg::int_type&) {
        w.Int(v.val);
    }
    void operator()(const iceberg::long_value& v, const iceberg::long_type&) {
        w.Int64(v.val);
    }
    void operator()(const iceberg::float_value& v, const iceberg::float_type&) {
        w.Double(v.val);
    }
    void
    operator()(const iceberg::double_value& v, const iceberg::double_type&) {
        w.Double(v.val);
    }

    void operator()(const iceberg::date_value& v, const iceberg::date_type&) {
        const std::chrono::system_clock::time_point tp{
          std::chrono::days(v.val)};
        const std::chrono::year_month_day ymd{
          std::chrono::floor<std::chrono::days>(tp)};
        w.String(fmt::to_string(ymd));
    }

    void operator()(const iceberg::time_value& v, const iceberg::time_type&) {
        using namespace std::chrono_literals;
        const std::chrono::microseconds us{v.val};
        const auto s = std::chrono::floor<std::chrono::seconds>(us);
        const auto rest = (us - s) / 1us;

        const std::chrono::system_clock::time_point tp{s};
        const auto tt = std::chrono::system_clock::to_time_t(tp);
        const auto tm = *std::gmtime(&tt);
        w.String(fmt::format("{}.{:06}", std::put_time(&tm, "%T"), rest));
    }

    void operator()(
      const iceberg::timestamp_value& v, const iceberg::timestamp_type&) {
        using namespace std::chrono_literals;
        const std::chrono::microseconds us{v.val};
        const auto s = std::chrono::floor<std::chrono::seconds>(us);
        const auto rest = (us - s) / 1us;

        const std::chrono::system_clock::time_point tp{s};
        const auto tt = std::chrono::system_clock::to_time_t(tp);
        const auto tm = *std::gmtime(&tt);
        w.String(fmt::format("{}.{:06}", std::put_time(&tm, "%FT%T"), rest));
    }
    void operator()(
      const iceberg::timestamptz_value& v, const iceberg::timestamptz_type&) {
        // Stores ISO - 8601 standard timestamp with microsecond precision;
        // must include a zone offset and it must be '+00:00'.
        // That is, timestamp with time zone is always stored as UTC.
        using namespace std::chrono_literals;
        const std::chrono::microseconds us{v.val};
        const auto s = std::chrono::floor<std::chrono::seconds>(us);
        const auto rest = (us - s) / 1us;

        const std::chrono::system_clock::time_point tp{s};
        const auto tt = std::chrono::system_clock::to_time_t(tp);
        const auto tm = *std::gmtime(&tt);
        w.String(
          fmt::format("{}.{:06}+00:00", std::put_time(&tm, "%FT%T"), rest));
    }

    void
    operator()(const iceberg::string_value& v, const iceberg::string_type&) {
        w.String(v.val);
    }
    void operator()(const iceberg::uuid_value& v, const iceberg::uuid_type&) {
        w.String(fmt::to_string(v.val));
    }
    void
    operator()(const iceberg::fixed_value& v, const iceberg::fixed_type& t) {
        if (auto sz = v.val.size_bytes(); sz > t.length) {
            throw std::invalid_argument(
              fmt::format(
                "Expected fixed_value of type {} but got {}B", t, sz));
        }
        w.String(to_hex(iobuf_to_bytes(v.val)));
    }
    void
    operator()(const iceberg::binary_value& v, const iceberg::binary_type&) {
        w.String(to_hex(iobuf_to_bytes(v.val)));
    }
    void operator()(
      const iceberg::decimal_value& v, const iceberg::decimal_type& t) {
        // TODO(oren): need negative scale? see datatypes.h
        auto [p, s] = t;
        // NOTE: Interestingly, max precision of 38 decimal digits is not enough
        // to support int128::max, but we can truncate if needed
        vassert(p <= 38 && s <= p, "Malformed decimal_type {}", t);

        absl::int128 val = v.val;
        bool is_neg = val < 0;
        if (is_neg) {
            val *= -1;
        }

        // left pad w/ zeros, taking into account sign of int representation.
        // this makes precision arithmetic a bit easier. we will drop these
        // later
        auto raw = fmt::format("{:0>{}}", val, 38);

        // left truncate based on decimal_type::precision
        auto truncated = raw | std::views::reverse | std::views::take(p)
                         | std::views::reverse;

        auto integral_part = truncated | std::views::take(p - s)
                             | std::views::drop_while(
                               [](const char c) { return c == '0'; });
        auto fractional_part = truncated | std::views::drop(p - s);
        w.String(
          fmt::format(
            "{}{}.{}",
            is_neg ? "-" : "",
            std::string_view{
              integral_part.empty() ? nullptr : &integral_part.front(),
              integral_part.size()},
            std::string_view{
              fractional_part.empty() ? nullptr : &fractional_part.front(),
              fractional_part.size()}));
    }

    void operator()(
      const iceberg::primitive_value& v, const iceberg::primitive_type& t) {
        value_to_json(w, v, t);
    }
    void
    operator()(const iceberg::struct_value& v, const iceberg::struct_type& t) {
        value_to_json(w, v, t);
    }
    void operator()(const iceberg::list_value& v, const iceberg::list_type& t) {
        value_to_json(w, v, t);
    }
    void operator()(const iceberg::map_value& v, const iceberg::map_type& t) {
        value_to_json(w, v, t);
    }

    void operator()(
      const std::unique_ptr<iceberg::struct_value>& pv,
      const iceberg::struct_type& t) {
        if (pv == nullptr) {
            w.Null();
        } else {
            value_to_json(w, *pv, t);
        }
    }

    void operator()(
      const std::unique_ptr<iceberg::list_value>& pv,
      const iceberg::list_type& t) {
        if (pv == nullptr) {
            w.Null();
        } else {
            value_to_json(w, *pv, t);
        }
    }

    void operator()(
      const std::unique_ptr<iceberg::map_value>& pv,
      const iceberg::map_type& t) {
        if (pv == nullptr) {
            w.Null();
        } else {
            value_to_json(w, *pv, t);
        }
    }

    template<typename V, typename T>
    void operator()(const V& v, const T& t) {
        throw std::invalid_argument(
          fmt::format(
            "JSON serde type mismatch for value {}, got type {}", v, t));
    }

private:
    iceberg::json_writer& w;
};

void value_to_json(
  iceberg::json_writer& w,
  const iceberg::primitive_value& v,
  const iceberg::primitive_type& t) {
    std::visit(rjson_visitor{w}, v, t);
}
void value_to_json(
  iceberg::json_writer& w,
  const iceberg::struct_value& s,
  const iceberg::struct_type& t) {
    if (s.fields.size() != t.fields.size()) {
        throw std::invalid_argument(
          fmt::format(
            "Wrong number of fields for struct_value {} of type {}", s, t));
    }
    w.StartObject();

    std::ranges::for_each(
      boost::irange(0ul, s.fields.size()), [&w, &s, &t](const auto i) {
          const auto& v_field = s.fields[i];
          const auto& t_field = t.fields[i];
          if (t_field == nullptr) {
              throw std::invalid_argument(
                fmt::format(
                  "Found null nested field in struct type for value {}", s));
          }
          w.Key(fmt::to_string(t_field->id));
          if (v_field.has_value()) {
              value_to_json(w, v_field.value(), t_field->type);
          } else if (!t_field->required) {
              w.Null();
          } else {
              throw std::invalid_argument(
                fmt::format(
                  "Found null value for required field {}", t_field->name));
          }
      });

    w.EndObject();
}
void value_to_json(
  iceberg::json_writer& w,
  const iceberg::list_value& l,
  const iceberg::list_type& t) {
    if (t.element_field == nullptr) {
        throw std::invalid_argument(fmt::format("Malformed list_type: {}", t));
    }
    w.StartArray();
    for (const auto& e : l.elements) {
        if (e.has_value()) {
            value_to_json(w, e.value(), t.element_field->type);
        } else if (!t.element_field->required) {
            w.Null();
        } else {
            throw std::invalid_argument(
              "Found null value for required list element");
        }
    }
    w.EndArray();
}
void value_to_json(
  iceberg::json_writer& w,
  const iceberg::map_value& m,
  const iceberg::map_type& t) {
    if (t.key_field == nullptr || t.value_field == nullptr) {
        throw std::invalid_argument(fmt::format("Malformed map_type: {}", t));
    }

    w.StartObject();
    w.Key("keys");
    w.StartArray();
    for (const auto& kv : m.kvs) {
        value_to_json(w, kv.key, t.key_field->type);
    }
    w.EndArray();

    w.Key("values");
    w.StartArray();
    for (const auto& kv : m.kvs) {
        if (kv.val.has_value()) {
            value_to_json(w, kv.val.value(), t.value_field->type);
        } else if (!t.value_field->required) {
            w.Null();
        } else {
            throw std::invalid_argument(
              fmt::format(
                "Found null value for required map value for key '{}'",
                kv.key));
        }
    }
    w.EndArray();

    w.EndObject();
}
} // namespace

namespace iceberg {

std::optional<value> value_from_json(
  const json::Value& data, const field_type& type, field_required required) {
    if (data.IsNull()) {
        if (required) {
            throw std::invalid_argument(
              fmt::format("Unexpected JSON null for required {}", type));
        }
        return std::nullopt;
    }
    return std::visit(value_parsing_visitor{data}, type);
}

void value_to_json(
  iceberg::json_writer& w,
  const iceberg::value& v,
  const iceberg::field_type& t) {
    std::visit(rjson_visitor{w}, v, t);
}

} // namespace iceberg
