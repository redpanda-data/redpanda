/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/type_traits.h"
#include "debug_bundle/error.h"
#include "debug_bundle/types.h"
#include "json/document.h"
#include "json/types.h"
#include "reflection/type_traits.h"
#include "security/types.h"
#include "strings/utf8.h"
#include "utils/functional.h"
#include "utils/uuid.h"

#include <exception>
#include <type_traits>

namespace debug_bundle {

inline std::string_view as_string_view(const json::Value& v) {
    vassert(v.IsString(), "You must check that v is a string;");
    return {v.GetString(), v.GetStringLength()};
}

template<typename T>
debug_bundle::result<T> from_json(
  const json::Document::ConstObject& p, std::string_view f, bool required);

template<typename T>
debug_bundle::result<T> from_json(const json::Value& v) {
    constexpr auto parse_error = [](std::string_view extra_msg) {
        return error_info{
          error_code::invalid_parameters,
          fmt::format("Failed to parse{}", extra_msg)};
    };

    if constexpr (reflection::is_std_optional<T>) {
        if (v.IsNull()) {
            return std::nullopt;
        }
        auto r = from_json<typename T::value_type>(v);
        if (r.has_value()) {
            return T{std::move(r).assume_value()};
        }
        return std::move(r).assume_error();
    } else if constexpr (reflection::is_rp_named_type<T>) {
        auto r = from_json<typename T::type>(v);
        if (r.has_value()) {
            return T{std::move(r).assume_value()};
        }
        return std::move(r).assume_error();
    } else if constexpr (std::is_same_v<T, int>) {
        if (v.IsInt()) {
            return v.GetInt();
        }
        return parse_error(": expected int");
    } else if constexpr (
      std::is_same_v<T, uint64_t> || std::is_same_v<T, unsigned long long>) {
        if (v.IsUint64()) {
            return v.GetUint64();
        }
        return parse_error(": expected uint64_t");
    } else if constexpr (
      std::is_same_v<T, int64_t> || std::is_same_v<T, signed long long>) {
        if (v.IsInt64()) {
            return v.GetInt64();
        }
        return parse_error(": expected int64_t");
    } else if constexpr (std::is_same_v<T, std::chrono::seconds>) {
        auto r = from_json<typename T::rep>(v);
        if (r.has_value()) {
            return T{v.GetInt64()};
        }
        return std::move(r).assume_error();
    } else if constexpr (std::is_same_v<T, ss::sstring>) {
        if (v.IsString()) {
            auto vv = as_string_view(v);
            try {
                validate_no_control(vv);
            } catch (const std::runtime_error& e) {
                return parse_error(
                  fmt::format(": invalid control character: {}", e.what()));
            }
            return T{vv};
        }
        return parse_error(": expected string");
    } else if constexpr (std::is_same_v<T, uuid_t>) {
        if (v.IsString()) {
            try {
                return uuid_t::from_string(as_string_view(v));
            } catch (const std::exception&) {
                return parse_error(": invalid uuid");
            }
        }
        return parse_error(": expected uuid");
    } else if constexpr (std::is_same_v<T, special_date>) {
        if (v.IsString()) {
            try {
                return boost::lexical_cast<special_date>(as_string_view(v));
            } catch (const std::exception&) {
                return parse_error(": invalid special_date");
            }
        }
        return parse_error(": expected special_date");
    } else if constexpr (std::is_same_v<T, clock::time_point>) {
        if (v.IsString()) {
            // TODO: Improve this with HowardHinnant/date or
            // std::chrono::zoned_time
            // TODO: Remove the intermediate string in c++26
            std::istringstream ss(std::string{as_string_view(v)});
            std::tm tmp{};
            ss >> std::get_time(&tmp, "%FT%T");
            if (ss.fail()) {
                return parse_error(": invalid ISO 8601 date");
            }
            // Forces `std::mktime` to "figure it out"
            tmp.tm_isdst = -1;
            std::time_t tt = std::mktime(&tmp);
            return clock::from_time_t(tt);
        }
        return parse_error(": expected ISO 8601 date");
    } else if constexpr (std::is_same_v<T, time_variant>) {
        if (auto r = from_json<special_date>(v); r.has_value()) {
            return T{std::move(r).assume_value()};
        } else if (auto res = from_json<clock::time_point>(v);
                   res.has_value()) {
            return T{std::move(res).assume_value()};
        }
        return parse_error(": expected ISO 8601 date or special_date");
    } else if constexpr (std::is_same_v<T, scram_creds>) {
        if (v.IsObject()) {
            auto o = v.GetObject();
            scram_creds sc;
            if (auto r = from_json<decltype(sc.username)>(o, "username", true);
                r.has_value()) {
                sc.username = std::move(r).assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(sc.password)>(o, "password", true);
                r.has_value()) {
                sc.password = std::move(r).assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(sc.mechanism)>(
                  o, "mechanism", true);
                r.has_value()) {
                sc.mechanism = std::move(r).assume_value();
            } else {
                return std::move(r).assume_error();
            }
            return std::move(sc);
        }
        return parse_error(": expected scram_creds");
    } else if constexpr (std::is_same_v<T, debug_bundle_authn_options>) {
        auto r = from_json<scram_creds>(v);
        if (r.has_value()) {
            return T{std::move(r).assume_value()};
        }
        return std::move(r).assume_error();
    } else if constexpr (std::is_same_v<T, partition_selection>) {
        if (v.IsString()) {
            auto r = partition_selection::from_string_view(as_string_view(v));
            if (r.has_value()) {
                return std::move(r).value();
            }
        }
        return parse_error(": expected partition_selection "
                           "'{namespace}/[topic]/[partitions...]'");
    } else if constexpr (detail::is_specialization_of_v<T, std::vector>) {
        if (v.IsArray()) {
            using V = T::value_type;
            auto arr = v.GetArray();
            std::vector<V> vec;
            vec.reserve(arr.Size());
            for (const auto& e : arr) {
                auto r = from_json<V>(e);
                if (r.has_value()) {
                    vec.push_back(std::move(r).assume_value());
                } else {
                    return std::move(r).assume_error();
                }
            }
            return std::move(vec);
        }
        return parse_error(": expected an array");
    } else if constexpr (detail::is_specialization_of_v<T, absl::btree_set>) {
        if (v.IsArray()) {
            using V = T::value_type;
            auto arr = v.GetArray();
            absl::btree_set<V> set;
            for (const auto& e : arr) {
                auto r = from_json<V>(e);
                if (r.has_value()) {
                    set.emplace(std::move(r).assume_value());
                } else {
                    return std::move(r).assume_error();
                }
            }
            return std::move(set);
        }
        return parse_error(": expected an array");
    } else if constexpr (std::is_same_v<T, bool>) {
        if (v.IsBool()) {
            return v.GetBool();
        }
        return parse_error(": expected bool");
    } else if constexpr (std::is_same_v<T, label_selection>) {
        if (v.IsObject()) {
            auto o = v.GetObject();
            label_selection ls;
            if (auto r = from_json<decltype(ls.key)>(o, "key", true);
                r.has_value()) {
                ls.key = std::move(r).assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(ls.value)>(o, "value", true);
                r.has_value()) {
                ls.value = std::move(r).assume_value();
            } else {
                return std::move(r).assume_error();
            }
            return std::move(ls);
        }
        return parse_error(": expected object");
    } else if constexpr (std::is_same_v<T, debug_bundle_parameters>) {
        debug_bundle_parameters params;
        if (v.IsObject()) {
            const auto& obj = v.GetObject();
            if (auto r = from_json<decltype(params.authn_options)>(
                  obj, "authentication", false);
                r.has_value()) {
                params.authn_options = std::move(r).assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r
                = from_json<decltype(params.controller_logs_size_limit_bytes)>(
                  obj, "controller_logs_size_limit_bytes", false);
                r.has_value()) {
                params.controller_logs_size_limit_bytes = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.cpu_profiler_wait_seconds)>(
                  obj, "cpu_profiler_wait_seconds", false);
                r.has_value()) {
                params.cpu_profiler_wait_seconds = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.logs_since)>(
                  obj, "logs_since", false);
                r.has_value()) {
                params.logs_since = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.logs_size_limit_bytes)>(
                  obj, "logs_size_limit_bytes", false);
                r.has_value()) {
                params.logs_size_limit_bytes = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.logs_until)>(
                  obj, "logs_until", false);
                r.has_value()) {
                params.logs_until = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.metrics_interval_seconds)>(
                  obj, "metrics_interval_seconds", false);
                r.has_value()) {
                params.metrics_interval_seconds = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.metrics_samples)>(
                  obj, "metrics_samples", false);
                r.has_value()) {
                params.metrics_samples = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.partition)>(
                  obj, "partition", false);
                r.has_value()) {
                params.partition = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.tls_enabled)>(
                  obj, "tls_enabled", false);
                r.has_value()) {
                params.tls_enabled = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.tls_insecure_skip_verify)>(
                  obj, "tls_insecure_skip_verify", false);
                r.has_value()) {
                params.tls_insecure_skip_verify = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.k8s_namespace)>(
                  obj, "namespace", false);
                r.has_value()) {
                params.k8s_namespace = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }
            if (auto r = from_json<decltype(params.label_selector)>(
                  obj, "label_selector", false);
                r.has_value()) {
                params.label_selector = r.assume_value();
            } else {
                return std::move(r).assume_error();
            }

            return std::move(params);
        }
        return parse_error(": expected debug_bundle_parameters");
    } else {
        static_assert(always_false_v<T>, "Not implemented");
    }
    return parse_error(": unsupported type");
}

template<typename T>
debug_bundle::result<T> from_json(
  const json::Document::ConstObject& p, std::string_view f, bool required) {
    auto it = p.FindMember(json::Document::StringRefType{
      f.data(), static_cast<json::SizeType>(f.length())});
    if (required && it == p.MemberEnd()) {
        return debug_bundle::error_info{
          debug_bundle::error_code::invalid_parameters,
          fmt::format("Failed to parse: field '{}' is required", f)};
    }
    if (it == p.MemberEnd()) {
        return outcome::success();
    }
    if (auto r = from_json<T>(it->value); r.has_error()) {
        return debug_bundle::error_info{
          r.assume_error().code(),
          fmt::format("{} for field '{}'", r.assume_error().message(), f)};
    } else {
        return r;
    }
}

} // namespace debug_bundle
