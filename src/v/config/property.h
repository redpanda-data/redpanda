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
#include "utils/to_string.h"

#include <seastar/util/noncopyable_function.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace config {

template<class T>
class property : public base_property {
public:
    using validator =
      typename ss::noncopyable_function<std::optional<ss::sstring>(const T&)>;

    property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      required req = required::yes,
      T def = T{},
      property::validator validator = property::noop_validator)
      : base_property(conf, name, desc, req)
      , _value(def)
      , _default(std::move(def))
      , _validator(std::move(validator)) {}

    const T& value() { return _value; }

    const T& value() const { return _value; }

    const T& default_value() const { return _default; }

    bool is_overriden() const { return is_required() || _value != _default; }

    const T& operator()() { return value(); }

    const T& operator()() const { return value(); }

    operator T() const { return value(); } // NOLINT

    void print(std::ostream& o) const override { o << name() << ":" << _value; }

    // serialize the value. the key is taken from the property name at the
    // serialization point in config_store::to_json to avoid users from being
    // forced to consume the property as a json object.
    void to_json(rapidjson::Writer<rapidjson::StringBuffer>& w) const override {
        json::rjson_serialize(w, _value);
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

    void set_value(YAML::Node n) override { _value = std::move(n.as<T>()); }

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
    T _value;
    const T _default;

private:
    validator _validator;
    constexpr static auto noop_validator = [](const auto&) {
        return std::nullopt;
    };
};

/*
 * Same as property<std::vector<T>> but will also decode a single T. This can be
 * useful for dealing with backwards compatibility or creating easier yaml
 * schemas that have simplified special cases.
 */
template<typename T>
class one_or_many_property : public property<std::vector<T>> {
public:
    using property<std::vector<T>>::property;

    void set_value(YAML::Node n) override {
        std::vector<T> value;
        if (n.IsSequence()) {
            for (auto elem : n) {
                value.push_back(std::move(elem.as<T>()));
            }
        } else {
            value.push_back(std::move(n.as<T>()));
        }
        this->_value = std::move(value);
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
      required req = required::yes,
      T def = T{},
      std::optional<T> min = std::nullopt,
      std::optional<T> max = std::nullopt)
      : property<T>(conf, name, desc, req, def)
      , _min(min)
      , _max(max) {}

    void set_value(YAML::Node n) override {
        auto val = std::move(n.as<T>());

        if (val.has_value()) {
            if (_min.has_value()) {
                val = std::max(val, _min.value());
            }
            if (_max.has_value()) {
                val = std::min(val, _max.value());
            }
        }
        this->_value = std::move(val);
    };

private:
    std::optional<T> _min;
    std::optional<T> _max;
};

}; // namespace config
