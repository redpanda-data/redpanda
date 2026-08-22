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
#include "base/format_to.h"
#include "base/seastarx.h"
#include "config/validation_error.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "utils/named_type.h"

#include <seastar/util/bool_class.hh>

#include <yaml-cpp/yaml.h>

#include <any>
#include <iosfwd>
#include <string>

namespace config {

// String to use when logging the value of a secret property
static constexpr std::string_view secret_placeholder = "[secret]";

class config_store;
using required = ss::bool_class<struct required_tag>;
using needs_restart = ss::bool_class<struct needs_restart_tag>;
using is_secret = ss::bool_class<struct is_secret_tag>;
using gets_restored = ss::bool_class<struct gets_restored_tag>;

// Whether this property's value may be read before the owning config_store has
// been marked ready by the startup process. See base_property::assert_readable
// and config_store::is_ready for details.
using usable_before_ready = ss::bool_class<struct usable_before_ready_tag>;

// Whether to redact secrets. If true, `secret_placeholder` should be used
// instead of the config value.
using redact_secrets = ss::bool_class<struct redact_secrets_tag>;

enum class visibility {
    // Tunables can be set by the user, but they control implementation
    // details like (e.g. buffer sizes, queue lengths)
    tunable,
    // User properties are normal, end-user visible settings that control
    // functional redpanda behaviours (e.g. enable a feature)
    user,
    // Deprecated properties are kept around to avoid complaining
    // about invalid config after upgrades, but they do nothing and
    // should never be presented to the user for editing.
    deprecated,
};

// Whether to force an even or an odd value for a given property.
enum class odd_even_constraint {
    even,
    odd,
};

// Whether to use the pending (pre-restart) value instead of the active value
// when serializing properties.
using use_pending = ss::bool_class<struct use_pending_tag>;

// This is equivalent to cluster::cluster_version, but defined here to
// avoid a dependency between config/ and cluster/
using legacy_version = named_type<int64_t, struct legacy_version_tag>;

std::string_view to_string_view(visibility v);
fmt::iterator format_to(visibility v, fmt::iterator);

/**
 * Abstract interface for all configuration properties.
 *
 * Properties that have needs_restart::yes maintain two value slots: an active
 * value used by the running system, and an optional pending value representing
 * a user-submitted change that takes effect after restart. The configured_value
 * (exposed via to_json with use_pending::yes) returns the pending value when
 * present, otherwise the active value.
 *
 * Lifecycle of a needs_restart property:
 *
 *                         set_value(V1)
 *                    (live-settable props only)
 *   +----------+    ───────────────────────────>    +----------+
 *   |  active  |                                    |  active  |
 *   |   = V0   |                                    |   = V1   |
 *   | pending  |                                    | pending  |
 *   |   = {}   |                                    |   = {}   |
 *   +----------+                                    +----------+
 *        │
 *        │ set_pending_value(V1)
 *        v
 *   +----------+    promote_pending()               +----------+
 *   |  active  |    ─────────────────────────>      |  active  |
 *   |   = V0   |       (on restart)                 |   = V1   |
 *   | pending  |                                    | pending  |
 *   |   = V1   |                                    |   = {}   |
 *   +----------+                                    +----------+
 *        │
 *        │ set_pending_value_to_default()
 *        v
 *   +---------------+
 *   |  active       |
 *   |   = V0        |
 *   | pending       |
 *   |   = default() |
 *   +---------------+
 *
 * Query methods in each state (when pending = V1, active = V0):
 *
 *   value()            -> V0   (always the active runtime value)
 *   configured_value() -> V1   (pending if present, else active)
 *   has_pending()      -> true
 *   is_default()       -> V0 == default
 *   is_default_pending() -> V1 == default
 */
class base_property {
public:
    struct metadata {
        std::string_view name;
        std::string_view desc;

        required required{required::no};
        needs_restart needs_restart{needs_restart::yes};
        std::optional<ss::sstring> example{std::nullopt};
        visibility visibility{visibility::user};
        is_secret secret{is_secret::no};

        // Whether or not this property should be restored following cluster
        // restore events.
        //
        // This is particularly important to allow the restored cluster to
        // define its own set of configs required for e.g., interacting with
        // cloud storage, or creating hardware-specific definitions like cache
        // sizes.
        gets_restored gets_restored{gets_restored::yes};

        // Aliases are used exclusively for input: all output (e.g. listing
        // configuration) uses the primary name of the property.
        std::vector<std::string_view> aliases;

        // Whether this property may have its active value read before the
        // owning config_store has been marked ready. Defaults to no: reading a
        // property's value before the cluster configuration has established a
        // consistent view with the rest of the cluster can potentially lead to
        // bugs, since the value may be stale. Properties consumed during the
        // bootstrap process (before that view exists) must opt in by setting
        // this to yes, having audited that using a potentially stale value
        // during bootstrap has no lasting impact on the behavior of the system.
        usable_before_ready usable_before_ready{usable_before_ready::no};
    };

    base_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      metadata meta);

    std::string_view name() const { return _meta->name; }
    std::string_view desc() const { return _meta->desc; }

    const required is_required() const { return _meta->required; }
    bool needs_restart() const { return bool(_meta->needs_restart); }
    visibility get_visibility() const { return _meta->visibility; }
    bool is_secret() const { return bool(_meta->secret); }
    const std::vector<std::string_view>& aliases() const {
        return _meta->aliases;
    }
    bool gets_restored() const { return bool(_meta->gets_restored); }

    bool usable_before_ready() const {
        return bool(_meta->usable_before_ready);
    }

    /// Serialize the property value to JSON. A full configuration
    /// serialization is performed in config_store::to_json where the JSON
    /// object key is taken from the property name.
    ///
    /// When \p pending is use_pending::yes (the default), properties that
    /// have a pending value awaiting restart will serialize that pending
    /// value. When use_pending::no, only the active runtime value is used.
    virtual void to_json(
      json::Writer<json::StringBuffer>& w,
      redact_secrets redact,
      use_pending pending = use_pending::yes) const = 0;

    virtual fmt::iterator format_to(fmt::iterator it) const = 0;

    /// Set the active value from a YAML node. For needs_restart properties,
    /// this immediately changes the runtime value; prefer set_pending_value
    /// when the caller intends the change to take effect after restart.
    /// Returns true if the value changed.
    virtual bool set_value(YAML::Node) = 0;

    /// Set the active value from a type-erased std::any.
    virtual void set_value(std::any) = 0;

    /// Reset the active value to its default.
    virtual void reset() = 0;

    /// Stage a pending value from a YAML node. The pending value is not
    /// visible to property::value() until promote_pending() is called
    /// (typically on restart). Returns true if, after this call, a pending
    /// value is staged that differs from the active value (i.e. if a restart is
    /// required).
    virtual bool set_pending_value(YAML::Node) = 0;

    /// Stage a pending value from a type-erased std::any.
    virtual void set_pending_value(std::any) = 0;

    /// Set the pending value to the property's default. Unlike reset()
    /// (which immediately changes the active value), this stages the default
    /// as pending so it takes effect after restart.
    virtual void set_pending_value_to_default() = 0;

    /// Returns true if a pending value has been staged that differs from
    /// the active value.
    virtual bool has_pending() const = 0;

    /// Promote the pending value to become the active value. After this
    /// call, has_pending() returns false and value() reflects the
    /// previously-pending value.
    virtual void promote_pending() = 0;

    /// Returns true if the active runtime value equals the default.
    virtual bool is_default() const = 0;

    /// Returns true if the configured value (pending if present, otherwise
    /// active) equals the default. Use this when filtering with
    /// use_pending::yes to correctly exclude properties whose pending
    /// value is the default.
    virtual bool is_default_pending() const = 0;

    virtual bool is_set() const = 0;
    virtual bool is_hidden() const = 0;

    /**
     * Helper for logging string-ized values of a property, e.g.
     * while processing an API request or loading from file, before
     * the property itself is initialized.
     *
     * Use this to ensure that any logged values are properly
     * redacted if secret.
     */
    template<typename U>
    std::string_view format_raw(const U& in) {
        if (is_secret() && !in.empty()) {
            return secret_placeholder;
        } else {
            return in;
        }
    }

    virtual std::string_view type_name() const = 0;
    virtual std::optional<std::string_view> units_name() const = 0;
    virtual bool is_nullable() const = 0;
    virtual bool is_array() const = 0;

    /// Serialize this property's *default* value (not its current value) to
    /// JSON, the same way to_json() serializes the current value. Used by
    /// util::generate_json_schema (redpanda/admin/cluster_config_schema_util.cc)
    /// to describe a property's default without a live instance.
    virtual void to_json_default(
      json::Writer<json::StringBuffer>& w, redact_secrets /*redact*/) const {
        w.Null();
    }

    /// Lower/upper bound as a formatted string, for properties with no
    /// runtime-queryable bound (i.e. anything not wrapping bounded_property).
    /// Overridden by bounded_property<T>.
    virtual std::optional<ss::sstring> minimum_as_string() const {
        return std::nullopt;
    }
    virtual std::optional<ss::sstring> maximum_as_string() const {
        return std::nullopt;
    }

    /// True for a property wrapped in config::enterprise<>. Overridden there.
    virtual bool is_enterprise() const { return false; }

    /// The community/sanctioned value used when there is no valid license.
    /// Only meaningful when is_enterprise() is true. Overridden by
    /// config::enterprise<P>.
    virtual void to_json_enterprise_sanctioned(
      json::Writer<json::StringBuffer>& w) const {
        w.Null();
    }

    /// The restricted value(s) that require a license -- a single value or
    /// an array, matching whichever shape config::enterprise<P> was
    /// constructed with. Null when enterprise_restriction_is_dynamic() is
    /// true (the restriction is a predicate function, not a static value).
    virtual void to_json_enterprise_restricted(
      json::Writer<json::StringBuffer>& w) const {
        w.Null();
    }

    /// True when this property's enterprise restriction is a predicate
    /// function rather than a static value or list of values -- i.e.
    /// to_json_enterprise_restricted() cannot describe it and will write
    /// null even though is_enterprise() is true.
    virtual bool enterprise_restriction_is_dynamic() const { return false; }
    /**
     * Example of correct syntax for this property. In most cases, this value
     * should be accepted by the config api (JSON API/YAML parser) as a valid
     * value for this property.
     */
    virtual std::optional<std::string_view> example() const = 0;
    virtual std::vector<ss::sstring> enum_values() const { return {}; };

    /**
     * Validation of a proposed new value before it has been assigned
     * to this property.
     */
    virtual std::optional<validation_error> validate(YAML::Node) const = 0;

    /**
     * Check whether a proposed new value is restricted before it has been
     * assigned. Rejection logic should be accounted for at the call site.
     */
    virtual std::optional<validation_error>
      check_restricted(YAML::Node) const = 0;

    /// Copy the configured value (pending if present, otherwise active)
    /// from another property of the same type into this property's active
    /// value. This is used when creating temporary config copies for
    /// validation, ensuring the copy reflects the user's configured intent
    /// rather than just the current runtime state.
    ///
    /// NB: This sets the active value directly (triggering watchers and
    /// clearing any pending state on the target). Only safe on temporary
    /// config objects — do not use on the live shard_local_cfg().
    virtual base_property& operator=(const base_property&) = 0;
    virtual ~base_property() noexcept = default;

    /**
     * Notify the property of the cluster's original logical version, in case
     * it has alternative defaults for old clusters.
     */
    virtual void notify_original_version(legacy_version) = 0;

    /// Assert (debug only) that this property's active value may be read right
    /// now. Note: bindings may still be *constructed* before the store is
    /// ready; only reading their value is gated.
    void debug_assert_readable() const;

protected:
    const metadata* _meta;
    config_store* _conf{nullptr};
    void assert_live_settable() const;
};
}; // namespace config
