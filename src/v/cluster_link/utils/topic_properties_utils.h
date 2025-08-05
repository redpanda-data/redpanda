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

#pragma once

#include "kafka/server/handlers/configs/config_utils.h"

namespace cluster_link::utils {
template<typename T>
struct noop_validator {
    std::optional<ss::sstring> operator()(const ss::sstring&, const T&) {
        return std::nullopt;
    }
};
struct noop_bool_validator {
    std::optional<ss::sstring>
    operator()(::model::topic_namespace_view, const ss::sstring&, bool) {
        return std::nullopt;
    }
};
template<
  typename T,
  typename Validator = noop_validator<T>,
  typename ParseFunc = decltype(boost::lexical_cast<T, ss::sstring>)>
requires requires(
  const T& value,
  const ss::sstring& str,
  Validator validator,
  ParseFunc parse) {
    { parse(str) } -> std::convertible_to<T>;
    {
        validator(str, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
bool parse_and_set_optional(
  cluster::property_update<std::optional<T>>& property,
  const std::optional<ss::sstring>& value,
  const std::optional<T>& current_property,
  Validator validator = noop_validator<T>{},
  ParseFunc parse = boost::lexical_cast<T, ss::sstring>) {
    if (value) {
        try {
            auto v = parse(*value);
            auto v_error = validator(*value, v);
            if (v_error) {
                throw kafka::validation_error(*v_error);
            }
            if (current_property != v) {
                property.op = cluster::incremental_update_operation::set;
                property.value = std::move(v);
                return true;
            }
        } catch (const std::runtime_error&) {
            throw boost::bad_lexical_cast();
        }
    }
    return false;
}
template<typename T, typename Validator = noop_validator<tristate<T>>>
requires requires(
  const tristate<T>& value, const ss::sstring& str, Validator validator) {
    {
        validator(str, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
bool parse_and_set_tristate(
  cluster::property_update<tristate<T>>& property,
  const std::optional<ss::sstring>& value,
  const tristate<T>& current_property,
  Validator validator = noop_validator<tristate<T>>{}) {
    // set property value
    using config_t
      = std::conditional_t<std::is_floating_point_v<T>, T, int64_t>;
    auto parsed = boost::lexical_cast<config_t>(*value);

    auto v_error = validator(*value, property.value);
    if (v_error) {
        throw kafka::validation_error(*v_error);
    }

    if (current_property.is_disabled() && parsed <= 0) {
        return false;
    }
    if (
      current_property.has_optional_value()
      && current_property.value() == static_cast<T>(parsed)) {
        return false;
    }

    if (parsed <= 0) {
        property.value = tristate<T>{};
    } else {
        property.value = tristate<T>(std::make_optional<T>(parsed));
    }

    property.op = cluster::incremental_update_operation::set;
    return true;
}
template<typename Validator = noop_bool_validator>
requires requires(
  ::model::topic_namespace_view tn,
  const ss::sstring& str,
  Validator validator,
  bool val) {
    {
        validator(tn, str, val)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
bool parse_and_set_bool(
  ::model::topic_namespace_view tn,
  cluster::property_update<bool>& property,
  const std::optional<ss::sstring>& value,
  bool current_value,
  Validator validator = noop_bool_validator{}) {
    try {
        // Ignore case.
        auto str_value = std::move(*value);
        std::transform(
          str_value.begin(),
          str_value.end(),
          str_value.begin(),
          [](const auto& c) { return std::tolower(c); });

        bool v = string_switch<bool>(str_value)
                   .match("true", true)
                   .match("false", false);

        auto v_error = validator(tn, str_value, v);
        if (v_error) {
            throw kafka::validation_error{*v_error};
        }

        if (v == current_value) {
            return false;
        }

        property.value = v;
        property.op = cluster::incremental_update_operation::set;
        return true;
    } catch (const std::runtime_error&) {
        // Our callers expect this exception type on malformed values
        throw boost::bad_lexical_cast();
    }
    return false;
}
template<class Dur, class Validator = noop_validator<Dur>>
requires requires(
  const Dur& value, const ss::sstring& str, Validator validator) {
    { boost::lexical_cast<typename Dur::rep>(str) };
    {
        validator(str, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
bool parse_and_set_optional_duration(
  cluster::property_update<std::optional<Dur>>& property,
  const std::optional<ss::sstring>& value,
  const std::optional<Dur>& current_property,
  Validator validator = noop_validator<Dur>{},
  bool clamp_to_duration_max = false) {
    // set property value if preset, otherwise do nothing
    if (value) {
        try {
            auto parsed = boost::lexical_cast<typename Dur::rep>(*value);
            // Certain Kafka clients have LONG_MAX duration to represent
            // maximum duration but that overflows during serde serialization
            // to nanos. Clamping to max allowed duration gives the same
            // desired behavior of no timeout without having to fail the
            // request.
            constexpr auto max = std::chrono::duration_cast<Dur>(
              std::chrono::nanoseconds::max());
            auto v = clamp_to_duration_max ? Dur(std::min(parsed, max.count()))
                                           : Dur(parsed);
            auto v_error = validator(*value, v);
            if (v_error) {
                throw kafka::validation_error(*v_error);
            }
            if (current_property != v) {
                property.value = std::move(v);
                property.op = cluster::incremental_update_operation::set;
                return true;
            }
        } catch (const std::runtime_error&) {
            throw boost::bad_lexical_cast();
        }
    }
    return false;
}

template<
  typename T,
  typename Validator = kafka::noop_validator_with_tn<T>,
  typename ParseFunc = decltype(boost::lexical_cast<T, ss::sstring>)>
requires requires(
  ::model::topic_namespace_view tn,
  const T& value,
  const ss::sstring& str,
  Validator validator,
  ParseFunc parse) {
    { parse(str) } -> std::convertible_to<T>;
    {
        validator(tn, str, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
bool parse_and_set_property(
  ::model::topic_namespace_view tn,
  cluster::property_update<T>& property,
  const std::optional<ss::sstring>& value,
  const T& current_value,
  Validator validator = noop_validator<T>{},
  ParseFunc parse = boost::lexical_cast<T, ss::sstring>) {
    // set property value if preset, otherwise do nothing
    if (value) {
        try {
            auto v = parse(*value);
            auto v_error = validator(tn, *value, v);
            if (v_error) {
                throw kafka::validation_error(*v_error);
            }
            if (v != current_value) {
                property.op = cluster::incremental_update_operation::set;
                property.value = std::move(v);
                return true;
            }

        } catch (const std::runtime_error&) {
            throw boost::bad_lexical_cast();
        }
    }
    return false;
}

bool parse_and_set_optional_bool_alpha(
  cluster::property_update<std::optional<bool>>& property,
  const std::optional<ss::sstring>& value,
  const std::optional<bool>& current_value);

bool maybe_append_update(
  cluster::topic_properties_update& update,
  const ss::sstring& config_name,
  const ss::sstring& config_value,
  const cluster::topic_configuration& topic_config);
} // namespace cluster_link::utils
