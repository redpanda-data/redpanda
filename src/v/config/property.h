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
#include "base/oncore.h"
#include "base/type_traits.h"
#include "config/base_property.h"
#include "config/rjson_serialization.h"
#include "container/intrusive_list_helpers.h"
#include "features/enterprise_feature_messages.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "utils/to_string.h"

#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <chrono>
#include <exception>
#include <functional>
#include <optional>
#include <variant>
#include <vector>

namespace config {

using namespace std::chrono_literals;

template<class T>
class binding_base;

template<class T>
class binding;

template<typename U, typename T>
class conversion_binding;

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
    using value_type = T;
    using validator =
      typename ss::noncopyable_function<std::optional<ss::sstring>(const T&)>;

    property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta = {},
      value_type def = value_type{},
      property::validator validator = property::noop_validator,
      std::optional<legacy_default<value_type>> ld = std::nullopt)
      : base_property(conf, name, desc, meta)
      , _value(def)
      , _default(std::move(def))
      , _legacy_default(std::move(ld))
      , _validator(std::move(validator)) {}

    /**
     * Properties aren't moved in normal use on the per-shard
     * cluster configuration objects.  This method exists for
     * use in unit tests of things like kafka client that carry
     * around a config_store as a member.
     */
    property(property<value_type>&& rhs)
      : base_property(rhs)
      , _value(std::move(rhs._value))
      , _default(std::move(rhs._default))
      , _validator(std::move(rhs._validator))
      , _bindings(std::move(rhs._bindings)) {
        for (auto& binding : _bindings) {
            binding._parent = this;
        }
    }

    ~property() {
        for (auto& binding : _bindings) {
            binding.detach();
        }
    }

    const value_type& value() { return _value; }

    const value_type& value() const { return _value; }

    const value_type& default_value() const { return _default; }

    std::string_view type_name() const override;

    std::optional<std::string_view> units_name() const override;

    bool is_nullable() const override;

    bool is_array() const override;

    bool is_overriden() const { return is_required() || _value != _default; }

    bool is_default() const override { return _value == _default; }

    bool is_hidden() const override {
        return get_visibility() == visibility::deprecated;
    }

    const value_type& operator()() { return value(); }

    const value_type& operator()() const { return value(); }

    operator value_type() const { return value(); } // NOLINT

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
        update_value(std::any_cast<value_type>(std::move(v)));
    }

    template<typename U>
    requires std::constructible_from<value_type, U>
    void set_value(U&& v) {
        // needs to go through virtual inheritance chain, since this class is
        // not final
        set_value(std::make_any<value_type>(std::forward<U>(v)));
    }

    bool set_value(YAML::Node n) override {
        return update_value(std::move(n.as<T>()));
    }

    std::optional<validation_error> validate(const value_type& v) const {
        if (auto err = _validator(v); err) {
            return std::make_optional<validation_error>(name().data(), *err);
        }
        return std::nullopt;
    }

    std::optional<validation_error> validate(YAML::Node n) const override {
        auto v = std::move(n.as<value_type>());
        return validate(v);
    }

    std::optional<validation_error>
    check_restricted(YAML::Node) const override {
        // Config properties are unrestricted by default
        return std::nullopt;
    }

    bool check_restricted(const T& value) const {
        return do_check_restricted(value);
    }

    void reset() override {
        auto v = default_value();
        update_value(std::move(v));
    }

    base_property& operator=(const base_property& pr) override {
        auto v = dynamic_cast<const property<value_type>&>(pr)._value;
        update_value(std::move(v));
        return *this;
    }

    /**
     * Returns a binding<T> object which offers live access to the
     * value of the property as well as the ability to watch for
     * changes to the property.
     */
    binding<value_type> bind() {
        assert_live_settable();
        return {*this};
    }

    template<typename U>
    auto bind(std::function<U(const value_type&)> conv)
      -> conversion_binding<U, value_type> {
        assert_live_settable();
        return {*this, std::move(conv)};
    }

    std::optional<std::string_view> example() const override {
        if (_meta.example.has_value()) {
            return _meta.example;
        } else {
            if constexpr (std::is_same_v<value_type, bool>) {
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

        if (
          ov <= _legacy_default.value().max_original_version && is_default()) {
            _default = _legacy_default.value().value;
            _value = _default;
            // In case someone already made a binding to us early in startup
            notify_watchers(_default);
        }
    }

    constexpr static auto noop_validator = [](const auto&) {
        return std::nullopt;
    };

protected:
    void notify_watchers(const value_type& new_value) {
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
            std::rethrow_exception(ex);
        }
    }

    bool update_value(value_type&& new_value) {
        if (new_value != _value) {
            // Update the main value first, in case one of the binding updates
            // throws.
            _value = std::move(new_value);
            notify_watchers(_value);

            return true;
        } else {
            return false;
        }
    }

    value_type _value;
    value_type _default;

    // An alternative default that applies if the cluster's original logical
    // version is <= the defined version
    const std::optional<legacy_default<value_type>> _legacy_default;

private:
    virtual bool do_check_restricted(const T&) const {
        // Config properties are unrestricted by default
        return false;
    }

    validator _validator;

    friend class binding_base<value_type>;
    friend class mock_property<value_type>;
    intrusive_list<binding_base<value_type>, &binding_base<value_type>::_hook>
      _bindings;
};

template<class T>
class binding_base {
    property<T>* _parent{nullptr};
    intrusive_list_hook _hook;
    std::optional<std::function<void()>> _on_change;

protected:
    expression_in_debug_mode(oncore _verify_shard);

    /**
     * This constructor is only for tests: construct a binding
     * with an arbitrary fixed value, that is not connected to any underlying
     * property.
     */
    binding_base() = default;

    explicit binding_base(property<T>& parent)
      : _parent(&parent) {
        _parent->_bindings.push_back(*this);
    }

    binding_base(const binding_base& rhs)
      : _parent(rhs._parent)
      , _on_change(rhs._on_change) {
        if (_parent) {
            // May not copy between shards, parent is
            // on the rhs instance's shard.
            oncore_debug_verify(rhs._verify_shard);

            // Both self and rhs now in property's binding list
            _parent->_bindings.push_back(*this);
        }
    }

    binding_base& operator=(const binding_base& rhs) {
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
    binding_base(binding_base&& rhs) noexcept
      : _parent(rhs._parent)
      , _on_change(std::move(rhs._on_change)) {
        if (_parent) {
            // May not move between shards, parent is
            // on the rhs instance's shard.
            oncore_debug_verify(rhs._verify_shard);
        }

        // Steal moved-from binding's place in the property's binding list
        _hook.swap_nodes(rhs._hook);
    }

public:
    virtual ~binding_base() = default;
    binding_base& operator=(binding_base&&) = delete;

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
        oncore_debug_verify(binding_base<T>::_verify_shard);
        _on_change = std::move(f);
    }

    // private interface for the property<T> friend
private:
    friend class property<T>;
    void detach() { _parent = nullptr; }
    void update(const T& v) {
        oncore_debug_verify(_verify_shard);
        const bool changed = do_update(v);
        if (changed && _on_change.has_value()) {
            _on_change.value()();
        }
    }

    // override interface
protected:
    /// Apply the updated property value to the binding, return true if
    /// the value has changed and the base_binding should call the watcher sink
    virtual bool do_update(const T& v) = 0;
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
class binding : public binding_base<T> {
private:
    T _value;

    bool do_update(const T& v) override {
        auto changed = _value != v;
        _value = v;
        return changed;
    }

protected:
    /**
     * This constructor is only for tests: construct a binding
     * with an arbitrary fixed value, that is not connected to any underlying
     * property.
     */
    explicit binding(T&& value)
      : _value(std::move(value)) {}

public:
    binding(property<T>& parent)
      : binding_base<T>(parent)
      , _value(parent()) {}

    binding(const binding<T>& rhs)
      : binding_base<T>(rhs)
      , _value(rhs._value) {}

    binding& operator=(const binding& rhs) {
        binding_base<T>::operator=(rhs);
        _value = rhs._value;
        return *this;
    }

    binding(binding<T>&& rhs) noexcept
      // The base move constructor doesn't touch _value
      // so that's why it's safe to use `rhs` after.
      : binding_base<T>(std::move(rhs))
      // NOLINTNEXTLINE(*-use-after-move)
      , _value(std::move(rhs._value)) {}

    const T& operator()() const {
        oncore_debug_verify(binding_base<T>::_verify_shard);
        return _value;
    }

    friend class mock_property<T>;
    template<typename U>
    friend inline binding<U> mock_binding(U&&);
    template<typename U>
    friend inline binding<U> mock_binding(const U&);
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

template<typename T>
inline binding<T> mock_binding(const T& value) {
    return binding<T>(T(value));
}
/**
 * A conversion property binding contains the result of application of
 * a conversion function to property value. The result is update in-place
 * whenever the property is updated in the cluster configuration.
 *
 * This is useful when the actually useful value of the configuration property
 * needs to be different from its representation in the cluster config.
 *
 * /param U The type of converted value, accessible from the binding
 * /param T The type of the parent property
 */
template<typename U, typename T>
class conversion_binding : public binding_base<T> {
public:
    using conversion_func = std::function<U(const T&)>;

private:
    U _value;
    conversion_func _convert;

    bool do_update(const T& v) override {
        U converted = _convert(v);
        const bool changed = _value != converted;
        _value = std::move(converted);
        return changed;
    }

protected:
    /**
     * This constructor is only for tests: construct a binding
     * with an arbitrary fixed value, that is not connected to any underlying
     * property.
     */
    explicit conversion_binding(U&& value)
      : _value(std::move(value)) {}

public:
    conversion_binding(property<T>& parent, conversion_func convert)
      : binding_base<T>(parent)
      , _value(convert(parent()))
      , _convert(std::move(convert)) {}

    conversion_binding(const conversion_binding<U, T>& rhs)
      : binding_base<T>(rhs)
      , _value(rhs._value)
      , _convert(rhs._convert) {}

    conversion_binding& operator=(const conversion_binding& rhs) {
        binding_base<T>::operator=(rhs);
        _value = rhs._value;
        _convert = rhs._convert;
        return *this;
    }

    conversion_binding(conversion_binding&& rhs) noexcept
      // The base move constructor doesn't touch _value or _convert
      // so that's why it's safe to use `rhs` after.
      : binding_base<T>(std::move(rhs))
      // NOLINTNEXTLINE(*-use-after-move)
      , _value(std::move(rhs._value))
      // NOLINTNEXTLINE(*-use-after-move)
      , _convert(std::move(rhs._convert)) {}

    conversion_binding& operator=(conversion_binding&&) = delete;

    const U& operator()() const {
        oncore_debug_verify(binding_base<T>::_verify_shard);
        return _value;
    }
};

namespace detail {

template<typename T>
concept has_type_name = requires(T x) { x.type_name(); };

template<typename T>
concept is_collection = requires(T x) {
    typename T::value_type;
    !std::is_same_v<typename T::value_type, char>;
    { x.size() };
    { x.begin() };
    { x.end() };
};

template<typename T>
concept is_pair = requires(T x) {
    typename T::first_type;
    typename T::second_type;
};

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
    } else if constexpr (std::is_same_v<type, s3_url_style>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::cloud_storage_backend>) {
        return "string";
    } else if constexpr (std::is_same_v<type, std::filesystem::path>) {
        return "string";
    } else if constexpr (std::is_same_v<
                           type,
                           model::cloud_storage_chunk_eviction_strategy>) {
        return "string";
    } else if constexpr (std::is_same_v<
                           type,
                           pandaproxy::schema_registry::
                             schema_id_validation_mode>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::fetch_read_strategy>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::write_caching_mode>) {
        return "string";
    } else if constexpr (std::
                           is_same_v<type, model::recovery_validation_mode>) {
        return "recovery_validation_mode";
    } else if constexpr (std::is_same_v<type, config::fips_mode_flag>) {
        return "string";
    } else if constexpr (std::is_same_v<type, config::tls_version>) {
        return "string";
    } else if constexpr (std::is_same_v<type, model::node_uuid>) {
        return "string";
    } else if constexpr (std::is_same_v<type, config::node_id_override>) {
        return "node_id_override";
    } else if constexpr (std::is_same_v<type, config::leaders_preference>) {
        return "leaders_preference";
    } else if constexpr (std::is_same_v<type, config::datalake_catalog_type>) {
        return "string";
    } else {
        static_assert(
          base::unsupported_type<T>::value, "Type name not defined");
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
        auto value = decode_yaml(n);
        return property<std::vector<T>>::update_value(std::move(value));
    }

    std::optional<validation_error>
    validate([[maybe_unused]] YAML::Node n) const override {
        std::vector<T> value = decode_yaml(n);
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
        auto value = decode_yaml(n);
        return property<std::unordered_map<typename T::key_type, T>>::
          update_value(std::move(value));
    }

    std::optional<validation_error> validate(YAML::Node n) const override {
        std::unordered_map<typename T::key_type, T> value = decode_yaml(n);
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
              auto found = std::ranges::find_if(
                _values, [&new_value](const T& v) { return v == new_value; });
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
    using property::set_value;

    void set_value(std::any v) final {
        update_value(
          std::any_cast<std::optional<std::chrono::milliseconds>>(std::move(v))
            .value_or(-1ms));
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

template<typename T>
class hidden_when_default_property : public property<T> {
public:
    hidden_when_default_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta,
      T def,
      property<T>::validator validator = property<T>::noop_validator)
      : property<T>(conf, name, desc, meta, def, std::move(validator)) {}

    bool is_hidden() const override {
        return this->value() == this->default_value();
    }
};
namespace detail {

template<typename P>
concept Property = requires() {
    std::derived_from<P, base_property>;
    typename P::value_type;
};

template<typename T>
concept Array = detail::is_array<T>() && !reflection::is_std_optional<T>;

} // namespace detail

/**
 * config::enterprise is a wrapper template logically comprising
 *   - a config::property<T>
 *   - a predicate check_restricted(T)
 *
 * The intended usage of this class is to attach metadata to a
 * config property, allowing clients of the config subsystem to
 * determine whether some config setting should be restricted to
 * enterprise licensed clusters.
 *
 * Note that config::enterprise does not do any _enforcement_ of
 * that restriction - it only provides the interface for clients
 * to determine whether some value satisfies the predicate.
 * Checking the license and/or applying any corrective action is
 * left entirely to the API consumer.
 *
 */
template<detail::Property P>
class enterprise : public P {
    template<typename T, typename = void>
    struct value_type {
        using type = T;
    };
    template<detail::Array T>
    struct value_type<T, std::void_t<typename T::value_type>> {
        using type = T::value_type;
    };
    template<reflection::is_std_optional T>
    struct value_type<T, std::void_t<typename T::value_type>> {
        using type = T::value_type;
    };

    // The value type of the wrapped property. Could be a container, or
    // an optional, primitive type, aggregate, etc.
    using T = typename P::value_type;

    /**
     * val_t is the unwrapped value type of the property
     *   - If T is an array, val_t is the element type
     *   - If T is an optional, val_t is the value type
     *   - otherwise val_t is T
     *
     * This allows us to express restrictions on array-properties by describing
     * which potential values for _elements_ of the array are 'restricted'
     * without enumerating all possible values for the array itself.
     *
     * e.g. given an array A<T>, where T={w, x, y, z}, we can say that T::x is
     * 'restricted' such that
     *   - A = [x]  is restricted
     *   - A = [z, y, x] is restricted
     *   - A = [z, x, y] is restricted
     *   - A = [y, z] is NOT restricted
     *
     */
    using val_t = typename value_type<T>::type;
    using val_container_t = std::vector<val_t>;
    using restrict_check_t = std::function<bool(const val_t&)>;
    using restrict_variant_t
      = std::variant<val_t, val_container_t, restrict_check_t>;

public:
    /**
     * Construct an enterprise property.
     *
     * Largely a pass-through to the wrapped property's constructor, with the
     * addition of a variant parameter describing restricted values, which may
     * take the type
     *   - val_t: that value is restricted
     *   - vector<val_t>: each element is restricted
     *   - std::function<bool(val_t)>: predicate for whether some val_t is
     *     restricted
     *
     * NOTE: Value restrictions must never conflict with a config's default
     * value. This condition is enforced by an assertion in the constructor.
     *
     */
    template<typename... Args>
    enterprise(
      config_store& conf, restrict_variant_t restricted, Args&&... args)
      : P(conf, std::forward<Args>(args)...)
      , _restriction(std::move(restricted)) {
        assert_no_default_conflict();
    }

    // Needed because the following override shadows the rest of the overloads
    using P::check_restricted;

    /**
     * Decodes the given YAML node into the underlying property's value_type and
     * checks whether that value should be restricted to enterprise clusters
     * based on the unwrapped value restrictions described at construction.
     */
    std::optional<validation_error> check_restricted(YAML::Node n) const final {
        auto v = std::move(n.as<T>());
        if (do_check_restricted(v)) {
            return std::make_optional<validation_error>(
              P::name().data(),
              features::enterprise_error_message::cluster_property(
                P::name().data(), v));
        }
        return std::nullopt;
    }

private:
    bool do_check_restricted(const T& setting) const final {
        // depending on how the restriction was defined, construct an applicable
        // check function for bare instances of the underlying value type
        auto restriction_check = [this](const val_t& v) -> bool {
            return ss::visit(
              _restriction,
              [&v](const val_t& val) { return v == val; },
              [&v](const val_container_t& vals) {
                  return std::ranges::find(vals, v) != std::ranges::end(vals);
              },
              [&v](const restrict_check_t& check) { return check(v); });
        };

        if constexpr (detail::Array<T>) {
            return std::ranges::any_of(setting, restriction_check);
        }

        if constexpr (reflection::is_std_optional<T>) {
            return setting.has_value() && restriction_check(setting.value());
        }

        if constexpr (std::is_same_v<T, val_t>) {
            return restriction_check(setting);
        }
    }

    void assert_no_default_conflict() const {
        vassert(
          !do_check_restricted(this->default_value()),
          "Enterprise properties must not restrict the default value of the "
          "underlying property!");
    }

    restrict_variant_t _restriction;
};

}; // namespace config
