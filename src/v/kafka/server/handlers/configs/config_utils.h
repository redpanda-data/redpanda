
/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/node_hash_set.h"
#include "base/outcome.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "datalake/partition_spec_parser.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fwd.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "security/acl.h"
#include "serde/rw/chrono.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <algorithm>
#include <optional>
#include <string_view>
#include <type_traits>

using namespace std::chrono_literals;

namespace kafka {

template<typename T, typename R>
T assemble_alter_config_response(std::vector<chunked_vector<R>> responses) {
    T response;
    for (auto& v : responses) {
        std::move(
          v.begin(), v.end(), std::back_inserter(response.data.responses));
    }

    return response;
}
template<typename T, typename R>
T make_error_alter_config_resource_response(
  const R& resource, error_code err, std::optional<ss::sstring> msg = {}) {
    return T{
      .error_code = err,
      .error_message = std::move(msg),
      .resource_type = resource.resource_type,
      .resource_name = resource.resource_name};
}

template<typename T, typename R>
ss::future<chunked_vector<R>> unsupported_broker_configuration(
  chunked_vector<T> resources, const std::string_view msg) {
    chunked_vector<R> responses;
    responses.reserve(resources.size());
    std::transform(
      resources.begin(),
      resources.end(),
      std::back_inserter(responses),
      [msg](T& resource) {
          return make_error_alter_config_resource_response<R>(
            resource, error_code::invalid_config, ss::sstring(msg));
      });

    return ss::make_ready_future<chunked_vector<R>>(std::move(responses));
}

class validation_error final : public std::exception {
public:
    explicit validation_error(ss::sstring what)
      : _what(std::move(what)) {}

    const char* what() const noexcept final { return _what.c_str(); }

private:
    ss::sstring _what;
};

template<typename T>
struct noop_validator {
    std::optional<ss::sstring> operator()(const ss::sstring&, const T&) {
        return std::nullopt;
    }
};

struct noop_bool_validator {
    std::optional<ss::sstring>
    operator()(model::topic_namespace_view, const ss::sstring&, bool) {
        return std::nullopt;
    }
};

template<typename T>
struct noop_validator_with_tn {
    std::optional<ss::sstring>
    operator()(model::topic_namespace_view, const ss::sstring&, const T&) {
        return std::nullopt;
    }
};

struct segment_size_validator {
    std::optional<ss::sstring>
    operator()(const ss::sstring&, const size_t& value) {
        // use reasonable defaults even if they are not set in configuration
        size_t min
          = config::shard_local_cfg().log_segment_size_min.value().value_or(1);
        size_t max
          = config::shard_local_cfg().log_segment_size_max.value().value_or(
            std::numeric_limits<size_t>::max());

        if (value < min || value > max) {
            return fmt::format(
              "segment size {} is outside of allowed range [{}, {}]",
              value,
              min,
              max);
        }
        return std::nullopt;
    }
};

struct replication_factor_must_be_positive {
    std::optional<ss::sstring> operator()(
      model::topic_namespace_view,
      const ss::sstring& raw,
      const cluster::replication_factor&) {
        static_assert(sizeof(int) > sizeof(cluster::replication_factor::type));
        auto value = boost::lexical_cast<int>(raw);
        if (value <= 0) {
            return fmt::format("replication factor {} must be positive", value);
        }
        return std::nullopt;
    }
};

struct replication_factor_must_be_odd {
    std::optional<ss::sstring> operator()(
      model::topic_namespace_view,
      const ss::sstring&,
      const cluster::replication_factor& value) {
        if (value % 2 == 0) {
            return fmt::format("replication factor {} must be odd", value);
        }
        return std::nullopt;
    }
};

struct replication_factor_must_be_greater_or_equal_to_minimum {
    std::optional<ss::sstring> operator()(
      model::topic_namespace_view tns,
      const ss::sstring&,
      const cluster::replication_factor& value) {
        if (model::is_user_topic(tns)) {
            const auto min_val
              = config::shard_local_cfg().minimum_topic_replication.value();
            if (value() < min_val) {
                return fmt::format(
                  "replication factor ({}) must be greater or equal to "
                  "specified minimum value ({})",
                  value,
                  min_val);
            }
        }

        return std::nullopt;
    }
};

struct write_caching_config_validator {
    std::optional<ss::sstring> operator()(
      const ss::sstring&,
      const std::optional<model::write_caching_mode>& maybe_value) {
        if (!maybe_value) {
            return std::nullopt;
        }
        auto value = maybe_value.value();
        if (value == model::write_caching_mode::disabled) {
            return fmt::format(
              "Invalid value {} for {}, accepted values: [{}, {}]",
              value,
              topic_property_write_caching,
              model::write_caching_mode::default_true,
              model::write_caching_mode::default_false);
        }
        auto cluster_default
          = config::shard_local_cfg().write_caching_default();
        if (cluster_default == model::write_caching_mode::disabled) {
            return fmt::format("write caching disabled at cluster level");
        }
        return std::nullopt;
    }
};

struct duration_validator {
    std::string_view name;
    std::chrono::milliseconds min = std::chrono::milliseconds{0};
    std::chrono::milliseconds max = serde::max_serializable_ms;

    std::optional<ss::sstring> operator()(
      const ss::sstring&,
      const std::optional<std::chrono::milliseconds>& maybe_value) {
        if (!maybe_value.has_value()) {
            return std::nullopt;
        }
        auto value = maybe_value.value();
        if (value < min || value > max) {
            return fmt::format(
              "{} value invalid, expected to be in range [{}, {}]",
              name,
              min,
              max);
        }
        return std::nullopt;
    }
};

const auto flush_ms_validator = duration_validator{
  .name = "flush.ms", .min = 1ms};
const auto iceberg_target_lag_ms_validator = duration_validator{
  .name = "target.lag.ms", .min = 10s};
const auto min_compaction_lag_ms_validator = duration_validator{
  .name = "min.compaction.lag.ms"};
const auto max_compaction_lag_ms_validator = duration_validator{
  .name = "max.compaction.lag.ms", .min = 1ms};
const auto message_timestamp_before_max_ms_validator = duration_validator{
  .name = "message.timestamp.before.max.ms", .min = 0ms};
const auto message_timestamp_after_max_ms_validator = duration_validator{
  .name = "message.timestamp.after.max.ms", .min = 0ms};

struct flush_bytes_validator {
    std::optional<ss::sstring>
    operator()(const ss::sstring&, const std::optional<size_t>& maybe_value) {
        if (!maybe_value.has_value()) {
            return std::nullopt;
        }
        auto value = maybe_value.value();
        if (value <= 0) {
            return fmt::format("config value too low, expected to be > 0");
        }
        return std::nullopt;
    }
};

struct iceberg_config_validator {
    std::optional<ss::sstring> operator()(
      model::topic_namespace_view tns,
      const ss::sstring&,
      const model::iceberg_mode& value) {
        if (!model::is_user_topic(tns)) {
            return fmt::format(
              "Iceberg configuration cannot be altered on non user topics");
        }
        if (
          !config::shard_local_cfg().iceberg_enabled()
          && value != model::iceberg_mode::disabled) {
            return fmt::format(
              "Iceberg disabled in the cluster configuration, enable it by "
              "setting: {}",
              config::shard_local_cfg().iceberg_enabled.name());
        }
        return std::nullopt;
    }
};

struct delete_retention_ms_validator {
    std::optional<ss::sstring> operator()(
      const ss::sstring&,
      const tristate<std::chrono::milliseconds>& maybe_value) {
        if (maybe_value.has_optional_value()) {
            const auto& value = maybe_value.value();
            if (value < 1ms || value > serde::max_serializable_ms) {
                return fmt::format(
                  "delete.retention.ms value invalid, expected to be in range "
                  "[1, {}]",
                  serde::max_serializable_ms);
            }
        }
        return std::nullopt;
    }
};

struct iceberg_partition_spec_validator {
    std::optional<ss::sstring>
    operator()(const ss::sstring& /*raw*/, const ss::sstring& value) {
        auto parsed = datalake::parse_partition_spec(value);
        if (parsed.has_error()) {
            return fmt::format(
              "couldn't parse iceberg partition spec `{}': {}",
              value,
              parsed.error());
        }
        return std::nullopt;
    }
};

struct min_cleanable_dirty_ratio_validator {
    std::optional<ss::sstring>
    operator()(const ss::sstring&, const tristate<double>& value) {
        double min = 0.0;
        double max = 1.0;
        if (value.has_optional_value()) {
            if (value.value() < min || value.value() > max) {
                return fmt::format(
                  "min.cleanable.dirty.ratio {} is outside of allowed range "
                  "[{}, {}]",
                  value.value(),
                  min,
                  max);
            }
        }
        return std::nullopt;
    }
};

struct batch_max_bytes_limits_validator {
    std::optional<ss::sstring>
    operator()(const ss::sstring&, const std::optional<uint32_t>& value) {
        if (!value) {
            return {};
        }

        const auto v = value.value();
        const uint32_t upper_limit
          = config::shard_local_cfg()
              .kafka_max_message_size_upper_limit_bytes()
              .value_or(std::numeric_limits<int32_t>::max());
        if (v == 0 || v > upper_limit) {
            return fmt::format(
              "max.message.bytes {} is outside of the allowed range [1, {}]",
              v,
              upper_limit);
        }

        return {};
    }
};

// Check if a storage mode transition is permitted.
// Returns true if the transition is allowed, false otherwise.
//
// Permitted transitions:
//   local -> tiered: Permitted
//   tiered -> local: Permitted (with caution)
//   unset -> local: Permitted (with caution)
//   unset -> tiered: Permitted
//   cloud -> tiered_cloud: Permitted
//   tiered_cloud -> cloud: Permitted
// Not permitted:
//   local -> unset: Not permitted
//   local -> cloud: Not permitted
//   tiered -> unset: Not permitted
//   tiered -> cloud: Not permitted
//   cloud -> local: Not permitted
//   cloud -> tiered: Not permitted
//   unset <-> cloud: Not permitted (cloud requires explicit choice)
//   local -> tiered_cloud: Not permitted
//   tiered -> tiered_cloud: Not permitted
//   tiered_cloud -> local: Not permitted
//   tiered_cloud -> tiered: Not permitted
//   unset <-> tiered_cloud: Not permitted
inline bool is_storage_mode_transition_permitted(
  model::redpanda_storage_mode from, model::redpanda_storage_mode to) {
    using sm = model::redpanda_storage_mode;

    // No-op transitions are fine.
    if (from == to) {
        return true;
    }

    // Permitted transitions:
    //   local -> tiered: Permitted
    //   tiered -> local: Permitted (with caution)
    //   unset -> local: Permitted (with caution)
    //   unset -> tiered: Permitted
    if (from == sm::local && to == sm::tiered) {
        return true;
    }
    if (from == sm::tiered && to == sm::local) {
        return true;
    }
    if (from == sm::unset && to == sm::local) {
        return true;
    }
    if (from == sm::unset && to == sm::tiered) {
        return true;
    }

    // cloud <-> tiered_cloud: Permitted
    if (from == sm::cloud && to == sm::tiered_cloud) {
        return true;
    }
    if (from == sm::tiered_cloud && to == sm::cloud) {
        return true;
    }

    // All other transitions are not permitted
    return false;
}

/// Validator for redpanda.storage.mode property.
/// Validates that the transition from current storage mode to the new value is
/// permitted.
struct storage_mode_validator {
    std::optional<model::redpanda_storage_mode> current_mode;

    std::optional<ss::sstring>
    operator()(const ss::sstring&, const model::redpanda_storage_mode& value) {
        // If we don't have a current mode (new topic), allow any value
        if (!current_mode) {
            return std::nullopt;
        }

        if (!is_storage_mode_transition_permitted(*current_mode, value)) {
            return fmt::format(
              "Cannot alter redpanda.storage.mode from {} to {} - this "
              "transition is not permitted",
              *current_mode,
              value);
        }
        return std::nullopt;
    }
};

template<typename T, typename... ValidatorTypes>
requires requires(
  model::topic_namespace_view tns,
  const ss::sstring& s,
  const T& value,
  ValidatorTypes... validators) {
    {
        std::get<0>(std::tuple{validators...})(tns, s, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
    (std::is_same_v<
       decltype(std::get<0>(std::tuple{validators...})(tns, s, value)),
       decltype(validators)>
     && ...);
}
struct config_validator_list {
    std::optional<ss::sstring> operator()(
      model::topic_namespace_view tns, const ss::sstring& raw, const T& value) {
        std::optional<ss::sstring> result;
        ((result = ValidatorTypes{}(tns, raw, value)) || ...);
        return result;
    }
};

using replication_factor_validator = config_validator_list<
  cluster::replication_factor,
  replication_factor_must_be_positive,
  replication_factor_must_be_odd,
  replication_factor_must_be_greater_or_equal_to_minimum>;

template<
  typename T,
  typename Validator = noop_validator_with_tn<T>,
  typename ParseFunc = decltype(boost::lexical_cast<T, ss::sstring>)>
requires requires(
  model::topic_namespace_view tn,
  const T& value,
  const ss::sstring& str,
  Validator validator,
  ParseFunc parse) {
    { parse(str) } -> std::convertible_to<T>;
    {
        validator(tn, str, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
void parse_and_set_property(
  model::topic_namespace_view tn,
  cluster::property_update<T>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  Validator validator = noop_validator<T>{},
  ParseFunc parse = boost::lexical_cast<T, ss::sstring>) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value if preset, otherwise do nothing
    if (op == config_resource_operation::set && value) {
        property.op = cluster::incremental_update_operation::set;
        try {
            auto v = parse(*value);
            auto v_error = validator(tn, *value, v);
            if (v_error) {
                throw validation_error(*v_error);
            }
            property.value = std::move(v);
        } catch (const std::runtime_error&) {
            throw boost::bad_lexical_cast();
        }
        return;
    }
}

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
void parse_and_set_optional(
  cluster::property_update<std::optional<T>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  Validator validator = noop_validator<T>{},
  ParseFunc parse = boost::lexical_cast<T, ss::sstring>) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value if preset, otherwise do nothing
    if (op == config_resource_operation::set && value) {
        property.op = cluster::incremental_update_operation::set;
        try {
            auto v = parse(*value);
            auto v_error = validator(*value, v);
            if (v_error) {
                throw validation_error(*v_error);
            }
            property.value = std::move(v);
        } catch (const std::runtime_error&) {
            throw boost::bad_lexical_cast();
        }
        return;
    }
}

template<class Dur, class Validator = noop_validator<Dur>>
requires requires(
  const Dur& value, const ss::sstring& str, Validator validator) {
    { boost::lexical_cast<typename Dur::rep>(str) };
    {
        validator(str, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
inline void parse_and_set_optional_duration(
  cluster::property_update<std::optional<Dur>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  Validator validator = noop_validator<Dur>{},
  bool clamp_to_duration_max = false) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value if preset, otherwise do nothing
    if (op == config_resource_operation::set && value) {
        property.op = cluster::incremental_update_operation::set;
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
                throw validation_error(*v_error);
            }
            property.value = std::move(v);
        } catch (const std::runtime_error&) {
            throw boost::bad_lexical_cast();
        }
        return;
    }
}

inline void parse_and_set_optional_bool_alpha(
  cluster::property_update<std::optional<bool>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value if preset, otherwise do nothing
    if (op == config_resource_operation::set && value) {
        try {
            property.value = string_switch<bool>(*value)
                               .match("true", true)
                               .match("false", false);
        } catch (const std::runtime_error&) {
            // Our callers expect this exception type on malformed values
            throw boost::bad_lexical_cast();
        }
        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

template<typename Validator = noop_bool_validator>
requires requires(
  model::topic_namespace_view tn,
  const ss::sstring& str,
  Validator validator,
  bool val) {
    {
        validator(tn, str, val)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
inline void parse_and_set_bool(
  model::topic_namespace_view tn,
  cluster::property_update<bool>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  bool default_value,
  Validator validator = noop_bool_validator{}) {
    // A remove on a concrete (non-nullable) property is a reset to default,
    // as is an assignment to nullopt.
    if (
      op == config_resource_operation::remove
      || (op == config_resource_operation::set && !value)) {
        property.op = cluster::incremental_update_operation::set;
        property.value = default_value;
        return;
    }

    if (op == config_resource_operation::set && value) {
        try {
            // Ignore case.
            auto str_value = *value;
            std::transform(
              str_value.begin(),
              str_value.end(),
              str_value.begin(),
              [](const auto& c) { return std::tolower(c); });

            property.value = string_switch<bool>(str_value)
                               .match("true", true)
                               .match("false", false);
            auto v_error = validator(tn, str_value, property.value);
            if (v_error) {
                throw validation_error{*v_error};
            }
        } catch (const std::runtime_error&) {
            // Our callers expect this exception type on malformed values
            throw boost::bad_lexical_cast();
        }
        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

template<typename T, typename Validator = noop_validator<tristate<T>>>
requires requires(
  const tristate<T>& value, const ss::sstring& str, Validator validator) {
    {
        validator(str, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
void parse_and_set_tristate(
  cluster::property_update<tristate<T>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  Validator validator = noop_validator<tristate<T>>{}) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value
    if (op == config_resource_operation::set) {
        using config_t
          = std::conditional_t<std::is_floating_point_v<T>, T, int64_t>;
        auto parsed = boost::lexical_cast<config_t>(*value);
        if (parsed < 0) {
            property.value = tristate<T>{disable_tristate};
        } else {
            property.value = tristate<T>(std::make_optional<T>(parsed));
        }

        auto v_error = validator(*value, property.value);
        if (v_error) {
            throw validation_error(*v_error);
        }

        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

template<typename Validator = noop_validator<cluster::replication_factor>>
requires requires(
  model::topic_namespace_view tns,
  const ss::sstring& raw,
  const cluster::replication_factor& value,
  Validator validator) {
    {
        validator(tns, raw, value)
    } -> std::convertible_to<std::optional<ss::sstring>>;
}
inline void parse_and_set_topic_replication_factor(
  model::topic_namespace_view tns,
  cluster::property_update<std::optional<cluster::replication_factor>>&
    property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  Validator validator = noop_validator<cluster::replication_factor>{}) {
    // set property value
    if (op == config_resource_operation::set) {
        property.value = std::nullopt;
        if (value) {
            auto v = cluster::parsing_replication_factor(*value);
            auto v_error = validator(tns, *value, v);
            if (v_error) {
                throw validation_error(*v_error);
            }
            property.value = v;
        }
        property.op = cluster::incremental_update_operation::set;
    }
    return;
}

///\brief Topic property parsing for schema id validation.
///
/// Handles parsing properties for create, alter and incremental_alter.
template<typename Props>
class schema_id_validation_config_parser {
public:
    explicit schema_id_validation_config_parser(Props& props)
      : props(props) {}

    ///\brief Parse a topic property from the supplied name and value
    template<typename T, typename S>
    bool operator()(
      const T& name, const S& value, kafka::config_resource_operation op) {
        using property_t = std::variant<
          decltype(&props.record_key_schema_id_validation),
          decltype(&props.record_key_subject_name_strategy)>;

        auto matcher = string_switch<std::optional<property_t>>(name);
        switch (config::shard_local_cfg().enable_schema_id_validation()) {
        case pandaproxy::schema_registry::schema_id_validation_mode::compat:
            matcher
              .match(
                topic_property_record_key_schema_id_validation_compat,
                &props.record_key_schema_id_validation_compat)
              .match(
                topic_property_record_key_subject_name_strategy_compat,
                &props.record_key_subject_name_strategy_compat)
              .match(
                topic_property_record_value_schema_id_validation_compat,
                &props.record_value_schema_id_validation_compat)
              .match(
                topic_property_record_value_subject_name_strategy_compat,
                &props.record_value_subject_name_strategy_compat);
            [[fallthrough]];
        case pandaproxy::schema_registry::schema_id_validation_mode::redpanda:
            matcher
              .match(
                topic_property_record_key_schema_id_validation,
                &props.record_key_schema_id_validation)
              .match(
                topic_property_record_key_subject_name_strategy,
                &props.record_key_subject_name_strategy)
              .match(
                topic_property_record_value_schema_id_validation,
                &props.record_value_schema_id_validation)
              .match(
                topic_property_record_value_subject_name_strategy,
                &props.record_value_subject_name_strategy);
            [[fallthrough]];
        case pandaproxy::schema_registry::schema_id_validation_mode::none:
            break;
        }
        auto prop = matcher.default_match(std::nullopt);
        if (prop.has_value()) {
            ss::visit(
              prop.value(), [&value, op](auto& p) { apply(*p, value, op); });
        }
        return prop.has_value();
    }

    ///\brief Parse a topic property from the supplied cfg.
    template<typename C>
    bool operator()(const C& cfg, kafka::config_resource_operation op) {
        return (*this)(cfg.name, cfg.value, op);
    }

private:
    ///\brief Parse and set a boolean from 'true' or 'false'.
    static void apply(
      cluster::property_update<std::optional<bool>>& prop,
      const std::optional<ss::sstring>& value,
      kafka::config_resource_operation op) {
        kafka::parse_and_set_optional_bool_alpha(prop, value, op);
    }
    ///\brief Parse and set the Subject Name Strategy
    static void apply(
      cluster::property_update<std::optional<
        pandaproxy::schema_registry::subject_name_strategy>>& prop,
      const std::optional<ss::sstring>& value,
      kafka::config_resource_operation op) {
        kafka::parse_and_set_optional(prop, value, op);
    }
    ///\brief Parse and set properties by wrapping them a property_update.
    template<typename T>
    static void apply(
      std::optional<T>& prop,
      std::optional<ss::sstring> value,
      kafka::config_resource_operation op) {
        cluster::property_update<std::optional<T>> up;
        apply(up, value, op);
        prop = up.value;
    }

    Props& props;
};

} // namespace kafka
