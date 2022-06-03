/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/base_property.h"
#include "config/rjson_serialization.h"
#include "reflection/type_traits.h"
#include "utils/to_string.h"

#include <seastar/util/noncopyable_function.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <chrono>
#include <optional>

namespace config {

using namespace std::chrono_literals;

template<class T>
class property : public base_property {
public:
    using validator =
      typename ss::noncopyable_function<std::optional<ss::sstring>(const T&)>;

    property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta = {},
      T def = T{},
      property::validator validator = property::noop_validator)
      : base_property(conf, name, desc, meta)
      , _value(def)
      , _default(std::move(def))
      , _validator(std::move(validator)) {}

    property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      required req = {},
      T def = T{},
      property::validator validator = property::noop_validator)
      : base_property(conf, name, desc, {.required = req})
      , _value(def)
      , _default(std::move(def))
      , _validator(std::move(validator)) {}

    const T& value() { return _value; }

    const T& value() const { return _value; }

    const T& default_value() const { return _default; }

    std::string_view type_name() const override;

    std::optional<std::string_view> units_name() const override;

    bool is_nullable() const override;

    bool is_array() const override;

    bool is_overriden() const { return is_required() || _value != _default; }

    bool is_default() const override { return _value == _default; }

    const T& operator()() { return value(); }

    const T& operator()() const { return value(); }

    operator T() const { return value(); } // NOLINT

    void print(std::ostream& o) const override {
        o << name() << ":";

        if (is_secret() && !is_default()) {
            o << secret_placeholder;
        } else {
            o << _value;
        }
    }

    // serialize the value. the key is taken from the property name at the
    // serialization point in config_store::to_json to avoid users from being
    // forced to consume the property as a json object.
    void
    to_json(rapidjson::Writer<rapidjson::StringBuffer>& w, redact_secrets redact) const override {
        if (is_secret() && !is_default() && redact == redact_secrets::yes) {
            json::rjson_serialize(w, secret_placeholder);
        } else {
            json::rjson_serialize(w, _value);
        }
    }

    std::optional<validation_error> validate() const override {
        if (auto err = _validator(_value); err) {
            return std::make_optional<validation_error>(name().data(), *err);
        }
        return std::nullopt;
    }

    void set_value(std::any v) override {
        _value = std::any_cast<T>(std::move(v));
    }

    bool set_value(YAML::Node n) override {
        return update_value(std::move(n.as<T>()));
    }

    void reset() override { _value = default_value(); }

    property<T>& operator()(T v) {
        _value = std::move(v);
        return *this;
    }

    base_property& operator=(const base_property& pr) override {
        _value = dynamic_cast<const property<T>&>(pr)._value;
        return *this;
    }

protected:
    bool update_value(T&& new_value) {
        if (new_value != _value) {
            _value = std::move(new_value);
            return true;
        } else {
            return false;
        }
    }

    T _value;
    const T _default;

private:
    validator _validator;
    constexpr static auto noop_validator = [](const auto&) {
        return std::nullopt;
    };
};

namespace detail {

template<typename T>
concept has_type_name = requires(T x) {
    x.type_name();
};

template<typename T>
concept is_collection = requires(T x) {
    typename T::value_type;
    !std::is_same_v<typename T::value_type, char>;
    {x.size()};
};

template<typename T>
struct dependent_false : std::false_type {};

template<typename T>
consteval std::string_view property_type_name() {
    using type = std::decay_t<T>;
    if constexpr (std::is_same_v<type, ss::sstring>) {
        // String check must come before is_collection check
        return "string";
    } else if constexpr (std::is_same_v<type, bool>) {
        // boolean check must come before is_integral check
        return "boolean";
    } else if constexpr (reflection::is_std_optional_v<type>) {
        return property_type_name<typename type::value_type>();
    } else if constexpr (is_collection<type>) {
        return property_type_name<typename type::value_type>();
    } else if constexpr (has_type_name<type>) {
        return type::type_name();
    } else if constexpr (std::is_same_v<type, model::compression>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::timestamp_type>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::cleanup_policy_bitflags>) {
        return "string";
    } else if constexpr (std::
                           is_same_v<type, model::violation_recovery_policy>) {
        return "string";
    } else if constexpr (std::is_same_v<type, config::data_directory_path>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::node_id>) {
        return "integer";
    } else if constexpr (std::is_same_v<type, std::chrono::seconds>) {
        return "integer";
    } else if constexpr (std::is_same_v<type, std::chrono::milliseconds>) {
        return "integer";
    } else if constexpr (std::is_same_v<type, seed_server>) {
        return "seed_server";
    } else if constexpr (std::is_same_v<type, unresolved_address>) {
        return "unresolved_address";
    } else if constexpr (std::is_same_v<type, tls_config>) {
        return "tls_config";
    } else if constexpr (std::is_same_v<type, endpoint_tls_config>) {
        return "endpoint_tls_config";
    } else if constexpr (std::is_same_v<type, model::broker_endpoint>) {
        return "broker_endpoint";
    } else if constexpr (std::is_floating_point_v<type>) {
        return "number";
    } else if constexpr (std::is_integral_v<type>) {
        return "integer";
    } else {
        static_assert(dependent_false<T>::value, "Type name not defined");
    }
}

template<typename T>
consteval std::string_view property_units_name() {
    using type = std::decay_t<T>;
    if constexpr (std::is_same_v<type, std::chrono::milliseconds>) {
        return "ms";
    } else if constexpr (std::is_same_v<type, std::chrono::seconds>) {
        return "s";
    } else if constexpr (reflection::is_std_optional_v<type>) {
        return property_units_name<typename type::value_type>();
    } else {
        // This will be transformed to nullopt at runtime: returning
        // std::optional from this function triggered a clang crash.
        return "";
    }
}

} // namespace detail

template<typename T>
std::string_view property<T>::type_name() const {
    // Go via a non-member function so that specialized implementations
    // can use concepts without all the same concepts having to apply
    // to the type T in the class definition.
    return detail::property_type_name<T>();
}

template<typename T>
std::optional<std::string_view> property<T>::units_name() const {
    auto u = detail::property_units_name<T>();
    if (u == "") {
        return std::nullopt;
    } else {
        return u;
    }
}

template<typename T>
bool property<T>::is_nullable() const {
    if constexpr (reflection::is_std_optional_v<std::decay_t<T>>) {
        return true;
    } else {
        return false;
    }
}

template<typename T>
bool property<T>::is_array() const {
    if constexpr (detail::is_collection<std::decay_t<T>>) {
        return true;
    } else {
        return false;
    }
}

/*
 * Same as property<std::vector<T>> but will also decode a single T. This can be
 * useful for dealing with backwards compatibility or creating easier yaml
 * schemas that have simplified special cases.
 */
template<typename T>
class one_or_many_property : public property<std::vector<T>> {
public:
    using property<std::vector<T>>::property;

    bool set_value(YAML::Node n) override {
        std::vector<T> value;
        if (n.IsSequence()) {
            for (auto elem : n) {
                value.push_back(std::move(elem.as<T>()));
            }
        } else {
            value.push_back(std::move(n.as<T>()));
        }
        return property<std::vector<T>>::update_value(std::move(value));
    }
};

/**
 * A numeric property that is clamped to a range.
 */
template<typename T>
class clamped_property : public property<T> {
public:
    using property<T>::property;

    clamped_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta,
      T def = T{},
      std::optional<T> min = std::nullopt,
      std::optional<T> max = std::nullopt)
      : property<T>(conf, name, desc, meta, def)
      , _min(min)
      , _max(max) {}

    bool set_value(YAML::Node n) override {
        auto val = std::move(n.as<T>());

        if (val.has_value()) {
            if (_min.has_value()) {
                val = std::max(val, _min.value());
            }
            if (_max.has_value()) {
                val = std::min(val, _max.value());
            }
        }

        return property<T>::update_value(std::move(val));
    };

private:
    std::optional<T> _min;
    std::optional<T> _max;
};

/**
 * A deprecated property only exposes metadata and does not expose a usable
 * value.
 */
class deprecated_property : public property<ss::sstring> {
public:
    deprecated_property(config_store& conf, std::string_view name)
      : property(conf, name, "", {.visibility = visibility::deprecated}) {}

    void set_value(std::any) override { return; }
};

class retention_duration_property final
  : public property<std::optional<std::chrono::milliseconds>> {
public:
    using property::property;
    void set_value(std::any v) final {
        update_value(std::any_cast<std::chrono::milliseconds>(std::move(v)));
    }
    bool set_value(YAML::Node n) final {
        return update_value(n.as<std::chrono::milliseconds>());
    }

    void print(std::ostream& o) const final {
        vassert(!is_secret(), "{} must not be a secret", name());
        o << name() << ":" << _value.value_or(-1ms);
    }

    // serialize the value. the key is taken from the property name at the
    // serialization point in config_store::to_json to avoid users from being
    // forced to consume the property as a json object.
    void to_json(rapidjson::Writer<rapidjson::StringBuffer>& w, redact_secrets) const final {
        // TODO: there's nothing forcing the retention duration to be a
        // non-secret; if a secret retention duration is ever introduced,
        // redact it, but consider the implications on the JSON type.
        vassert(!is_secret(), "{} must not be a secret", name());
        json::rjson_serialize(w, _value.value_or(-1ms));
    }

private:
    bool update_value(std::chrono::milliseconds value) {
        if (value < 0ms) {
            return property::update_value(std::nullopt);
        } else {
            return property::update_value(value);
        }
    }
};

}; // namespace config
