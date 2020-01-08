#pragma once
#include "config/base_property.h"
#include "utils/to_string.h"

#include <seastar/util/noncopyable_function.hh>

namespace config {

template<class T>
class property : public base_property {
public:
    using validator =
      typename seastar::noncopyable_function<std::optional<sstring>(const T&)>;

    property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      required req = required::yes,
      T def = T{},
      property::validator validator = property::noop_validator)
      : base_property(conf, name, desc, req)
      , _value(std::move(def))
      , _validator(std::move(validator)) {}

    const T& value() { return _value; }

    const T& value() const { return _value; }

    const T& operator()() { return value(); }

    const T& operator()() const { return value(); }

    operator T() const { return value(); } // NOLINT

    void print(std::ostream& o) const override { o << name() << ":" << _value; }

    // serialize the value. the key is taken from the property name at the
    // serialization point in config_store::to_json to avoid users from being
    // forced to consume the property as a json object.
    void to_json(nlohmann::json& j) const override { j = _value; }

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

    property<T>& operator()(T v) {
        _value = std::move(v);
        return *this;
    }

    base_property& operator=(const base_property& pr) override {
        _value = dynamic_cast<const property<T>&>(pr)._value;
        return *this;
    }

private:
    T _value;
    validator _validator;
    constexpr static auto noop_validator = [](const auto&) {
        return std::nullopt;
    };
};
}; // namespace config
