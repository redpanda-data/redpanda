/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/oncore.h"
#include "config/base_property.h"
#include "config/rjson_serialization.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "reflection/type_traits.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/to_string.h"

#include <seastar/util/noncopyable_function.hh>

#include <boost/intrusive/list.hpp>

#include <chrono>
#include <optional>

namespace config {

using namespace std::chrono_literals;

template<class T>
class binding;

template<typename T>
class mock_property;

/**
 * An alternative default that only applies to clusters created before a
 * particular logical version.
 *
 * This enables changing defaults for new clusters without disrupting
 * existing clusters.
 *
 * **Be aware** that this only works for properties that are read _after_
 * the bootstrap phase of startup.
 */
template<class T>
class legacy_default {
public:
    legacy_default() = delete;

    legacy_default(T v, legacy_version ov)
      : value(std::move(v))
      , max_original_version(ov) {}

    T value;
    legacy_version max_original_version;
};

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
      property::validator validator = property::noop_validator,
      std::optional<legacy_default<T>> ld = std::nullopt)
      : base_property(conf, name, desc, meta)
      , _value(def)
      , _default(std::move(def))
      , _legacy_default(std::move(ld))
      , _validator(std::move(validator)) {}

    /**
     * Properties aren't moved in normal used on the per-shard
     * cluster configuration objects.  This method exists for
     * use in unit tests of things like kafka client that carry
     * around a config_store as a member.
     */
    property(property<T>&& rhs)
      : base_property(rhs)
      , _value(std::move(rhs._value))
      , _default(std::move(rhs._default))
      , _validator(std::move(rhs._validator)) {
        for (auto binding_ptr : _bindings) {
            binding_ptr._parent = this;
        }
    }

    ~property() {
        for (auto& binding : _bindings) {
            binding.detach();
        }
    }

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
    void to_json(json::Writer<json::StringBuffer>& w, redact_secrets redact)
      const override {
        if (is_secret() && !is_default() && redact == redact_secrets::yes) {
            json::rjson_serialize(w, secret_placeholder);
        } else {
            json::rjson_serialize(w, _value);
        }
    }

    void set_value(std::any v) override {
        update_value(std::any_cast<T>(std::move(v)));
    }

    bool set_value(YAML::Node n) override {
        return update_value(std::move(n.as<T>()));
    }

    std::optional<validation_error> validate(T const& v) const {
        if (auto err = _validator(v); err) {
            return std::make_optional<validation_error>(name().data(), *err);
        }
        return std::nullopt;
    }

    std::optional<validation_error> validate(YAML::Node n) const override {
        auto v = std::move(n.as<T>());
        return validate(v);
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

    /**
     * Returns a binding<T> object which offers live access to the
     * value of the property as well as the ability to watch for
     * changes to the property.
     */
    binding<T> bind() {
        assert_live_settable();
        return {*this};
    }

    std::optional<std::string_view> example() const override {
        if (_meta.example.has_value()) {
            return _meta.example;
        } else {
            if constexpr (std::is_same_v<T, bool>) {
                // Provide an example that is the opposite of the default
                // (i.e. an example of how to _change_ the setting)
                return _default ? "false" : "true";
            } else {
                return std::nullopt;
            }
        }
    }

    void notify_original_version(legacy_version ov) override {
        if (!_legacy_default.has_value()) {
            // Most properties have no legacy default, and ignore this.
            return;
        }

        if (ov < _legacy_default.value().max_original_version) {
            _default = _legacy_default.value().value;
            // In case someone already made a binding to us early in startup
            notify_watchers(_default);
        }
    }

    constexpr static auto noop_validator = [](const auto&) {
        return std::nullopt;
    };

protected:
    void notify_watchers(const T& new_value) {
        std::exception_ptr ex;
        for (auto& binding : _bindings) {
            try {
                binding.update(new_value);
            } catch (...) {
                // In case there are multiple bindings:
                // if one of them throws an exception from an on_change
                // callback, proceed to update all bindings' values before
                // re-raising the last exception we saw.  This avoids
                // a situation where bindings could disagree about
                // the property's value.
                ex = std::current_exception();
            }
        }

        if (ex) {
            rethrow_exception(ex);
        }
    }

    bool update_value(T&& new_value) {
        if (new_value != _value) {
            notify_watchers(new_value);
            _value = std::move(new_value);

            return true;
        } else {
            return false;
        }
    }

    T _value;
    T _default;

    // An alternative default that applies if the cluster's original logical
    // version is <= the defined version
    const std::optional<legacy_default<T>> _legacy_default;

private:
    validator _validator;

    friend class binding<T>;
    friend class mock_property<T>;
    intrusive_list<binding<T>, &binding<T>::_hook> _bindings;
};

/**
 * A property binding contains a copy of the property's value, which
 * will be updated in-place whenever the property is updated in the
 * cluster configuration.
 *
 * This is useful for classes that want a copy of a property without
 * having to write their own logic for subscribing to value changes.
 */
template<class T>
class binding {
private:
    T _value;
    property<T>* _parent{nullptr};

    std::optional<std::function<void()>> _on_change;

    void update(const T& v) {
        oncore_debug_verify(_verify_shard);
        auto changed = _value != v;
        _value = v;
        if (changed && _on_change.has_value()) {
            _on_change.value()();
        }
    }
    void detach() { _parent = nullptr; }

    expression_in_debug_mode(oncore _verify_shard);

protected:
    intrusive_list_hook _hook;

    /**
     * This constructor is only for tests: construct a binding
     * with an arbitrary fixed value, that is not connected to any underlying
     * property.
     */
    explicit binding(T&& value)
      : _value(std::move(value))
      , _parent(nullptr) {}

public:
    binding(property<T>& parent)
      : _value(parent())
      , _parent(&parent) {
        _parent->_bindings.push_back(*this);
    }

    binding(const binding<T>& rhs)
      : _value(rhs._value)
      , _parent(rhs._parent)
      , _on_change(rhs._on_change) {
        if (_parent) {
            // May not copy between shards, parent is
            // on the rhs instance's shard.
            oncore_debug_verify(rhs._verify_shard);

            // Both self and rhs now in property's binding list
            _parent->_bindings.push_back(*this);
        }
    }

    binding& operator=(const binding& rhs) {
        _value = rhs._value;
        _parent = rhs._parent;
        _on_change = rhs._on_change;
        _hook.unlink();
        if (_parent) {
            _parent->_bindings.push_back(*this);
        }
        return *this;
    }

    /**
     * This needs to be noexcept for objects with bindings to
     * be usable in seastar futures.  This is not strictly
     * noexcept, because in principle the parent binding list insert
     * could be an allocation, but this is only used in practice
     * in unit tests: in normal usage we do not move around bindings
     * after startup time.
     */
    binding(binding<T>&& rhs) noexcept {
        _value = std::move(rhs._value);
        _on_change = std::move(rhs._on_change);
        _parent = rhs._parent;

        if (_parent) {
            // May not move between shards, parent is
            // on the rhs instance's shard.
            oncore_debug_verify(rhs._verify_shard);
        }

        // Steal moved-from binding's place in the property's binding list
        _hook.swap_nodes(rhs._hook);
    }

    /**
     * Register a callback on changes to the property value.  Note that you
     * do not need to call this for the binding's value to remain up to date,
     * only if you need to do some extra action when it changes.
     *
     * Callbacks should endeavor not to throw, but if they do then
     * the configuration value will be marked 'invalid' in the node's
     * configuration status, but the new value will still be set.
     *
     * The callback is moved into this binding, and so will live as long as
     * the binding. This means that callers must ensure
     * that any objects referenced by the callback remain valid for as long as
     * this binding: the simplest way to  accomplish this is to make both
     * the callback and the binding attributes of the same object.
     */
    void watch(std::function<void()>&& f) {
        oncore_debug_verify(_verify_shard);
        _on_change = std::move(f);
    }

    const T& operator()() const {
        oncore_debug_verify(_verify_shard);
        return _value;
    }

    friend class property<T>;
    friend class mock_property<T>;
    template<typename U>
    friend inline binding<U> mock_binding(U&&);
};

/**
 * Test helper.  Construct a property binding with no underlying
 * property object, which just contains a static value which remains
 * unchanged through its lifetime.
 *
 * This exists to make it more obvious at the call site that we
 * are constructing something static, rather than just calling
 * a binding constructor directly.
 */
template<typename T>
inline binding<T> mock_binding(T&& value) {
    return binding<T>(std::forward<T>(value));
}

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
    {x.begin()};
    {x.end()};
};

template<typename T>
concept is_pair = requires(T x) {
    typename T::first_type;
    typename T::second_type;
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
    } else if constexpr (reflection::is_std_optional<type>) {
        return property_type_name<typename type::value_type>();
    } else if constexpr (is_collection<type>) {
        return property_type_name<typename type::value_type>();
    } else if constexpr (is_pair<type>) {
        return property_type_name<typename type::second_type>();
    } else if constexpr (has_type_name<type>) {
        return type::type_name();
    } else if constexpr (std::is_same_v<type, model::compression>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::timestamp_type>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::cleanup_policy_bitflags>) {
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
    } else if constexpr (std::is_same_v<type, net::unresolved_address>) {
        return "net::unresolved_address";
    } else if constexpr (std::is_same_v<type, tls_config>) {
        return "tls_config";
    } else if constexpr (std::is_same_v<type, endpoint_tls_config>) {
        return "endpoint_tls_config";
    } else if constexpr (std::is_same_v<type, model::broker_endpoint>) {
        return "broker_endpoint";
    } else if constexpr (std::is_same_v<type, model::rack_id>) {
        return "rack_id";
    } else if constexpr (std::is_same_v<
                           type,
                           model::partition_autobalancing_mode>) {
        return "partition_autobalancing_mode";
    } else if constexpr (std::is_floating_point_v<type>) {
        return "number";
    } else if constexpr (std::is_integral_v<type>) {
        return "integer";
    } else if constexpr (std::
                           is_same_v<type, model::cloud_credentials_source>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::cloud_storage_backend>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::leader_balancer_mode>) {
        return "string";
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
    } else if constexpr (reflection::is_std_optional<type>) {
        return property_units_name<typename type::value_type>();
    } else {
        // This will be transformed to nullopt at runtime: returning
        // std::optional from this function triggered a clang crash.
        return "";
    }
}

template<typename T>
consteval bool is_array() {
    if constexpr (
      std::is_same_v<T, ss::sstring> || std::is_same_v<T, std::string>) {
        // Special case for strings, which are collections but we do not
        // want to report them that way.
        return false;
    } else if constexpr (detail::is_collection<std::decay_t<T>>) {
        return true;
    } else if constexpr (reflection::is_std_optional<T>) {
        return is_array<typename T::value_type>();
    } else {
        return false;
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
    return reflection::is_std_optional<std::decay_t<T>>;
}

template<typename T>
bool property<T>::is_array() const {
    return detail::is_array<T>();
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
        auto value = decode_yaml(std::move(n));
        return property<std::vector<T>>::update_value(std::move(value));
    }

    std::optional<validation_error>
    validate([[maybe_unused]] YAML::Node n) const override {
        std::vector<T> value = decode_yaml(std::move(n));
        return property<std::vector<T>>::validate(value);
    }

private:
    /**
     * Given either a single value or a list of values, return
     * a list of decoded values.
     */
    std::vector<T> decode_yaml(const YAML::Node& n) const {
        std::vector<T> value;
        if (n.IsSequence()) {
            for (auto elem : n) {
                value.push_back(std::move(elem.as<T>()));
            }
        } else {
            value.push_back(std::move(n.as<T>()));
        }
        return value;
    }
};

/*
 * Same as property<std::unordered_map<T::key_type, T>> but will also decode a
 * single T. This can be useful for dealing with backwards compatibility or
 * creating easier yaml schemas that have simplified special cases.
 */
template<typename T>
class one_or_many_map_property
  : public property<std::unordered_map<typename T::key_type, T>> {
public:
    using property<std::unordered_map<typename T::key_type, T>>::property;

    bool set_value(YAML::Node n) override {
        auto value = decode_yaml(std::move(n));
        return property<std::unordered_map<typename T::key_type, T>>::
          update_value(std::move(value));
    }

    std::optional<validation_error> validate(YAML::Node n) const override {
        std::unordered_map<typename T::key_type, T> value = decode_yaml(
          std::move(n));
        return property<std::unordered_map<typename T::key_type, T>>::validate(
          value);
    }

private:
    /**
     * Given either a single value or a list of values, return
     * a hash_map of decoded values.
     **/
    std::unordered_map<typename T::key_type, T>
    decode_yaml(const YAML::Node& n) const {
        std::unordered_map<typename T::key_type, T> value;
        if (n.IsSequence()) {
            for (const auto& elem : n) {
                auto elem_val = elem.as<T>();
                value.emplace(elem_val.key(), std::move(elem_val));
            }
        } else {
            auto elem_val = n.as<T>();
            value.emplace(elem_val.key(), std::move(elem_val));
        }
        return value;
    }
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

template<typename T>
class enum_property : public property<T> {
public:
    enum_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta,
      T def,
      std::vector<T> values)
      : property<T>(
        conf,
        name,
        desc,
        meta,
        def,
        [this](T new_value) -> std::optional<ss::sstring> {
            auto found = std::find_if(
              _values.begin(), _values.end(), [&new_value](T const& v) {
                  return v == new_value;
              });
            if (found == _values.end()) {
                return help_text();
            } else {
                return std::nullopt;
            }
        })
      , _values(values) {}

    std::optional<validation_error>
    validate(YAML::Node n) const final override {
        try {
            auto v = n.as<T>();
            return property<T>::validate(v);
        } catch (...) {
            // Not convertible (e.g. if the underlying type is an enum class)
            // therefore assume it is out of bounds.
            return validation_error{property<T>::name().data(), help_text()};
        }
    }

    std::vector<ss::sstring> enum_values() const final override {
        std::vector<ss::sstring> r;
        for (const auto& v : _values) {
            r.push_back(ssx::sformat("{}", v));
        }

        return r;
    }

private:
    ss::sstring help_text() const {
        // String-ize the available values
        std::vector<std::string> str_values;
        str_values.reserve(_values.size());
        std::transform(
          _values.begin(),
          _values.end(),
          std::back_inserter(str_values),
          [](const T& v) { return fmt::format("{}", v); });

        return fmt::format(
          "Must be one of {}",
          fmt::join(str_values.begin(), str_values.end(), ","));
    }

    std::vector<T> _values;
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
    void
    to_json(json::Writer<json::StringBuffer>& w, redact_secrets) const final {
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
