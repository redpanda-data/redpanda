
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

#include "base/outcome.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fwd.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "security/acl.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/node_hash_set.h>

#include <algorithm>
#include <optional>

namespace kafka {
template<typename T>
struct groupped_resources {
    chunked_vector<T> topic_changes;
    chunked_vector<T> broker_changes;
};

template<typename T>
groupped_resources<T> group_alter_config_resources(chunked_vector<T> req) {
    groupped_resources<T> ret;
    for (auto& res : req) {
        switch (config_resource_type(res.resource_type)) {
        case config_resource_type::topic:
            ret.topic_changes.push_back(std::move(res));
            break;
        default:
            ret.broker_changes.push_back(std::move(res));
        };
    }
    return ret;
}

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

template<typename R, typename T>
std::vector<chunked_vector<R>> make_audit_failure_response(
  groupped_resources<T>&& resources, chunked_vector<R> unauthorized_responses) {
    chunked_vector<R> responses;

    auto gen_resp = [](const T& res) {
        return make_error_alter_config_resource_response<R>(
          res,
          error_code::broker_not_available,
          "Broker not available - audit system failure");
    };

    responses.reserve(
      resources.broker_changes.size() + resources.topic_changes.size()
      + unauthorized_responses.size());

    std::transform(
      resources.broker_changes.begin(),
      resources.broker_changes.end(),
      std::back_inserter(responses),
      gen_resp);

    std::transform(
      resources.topic_changes.begin(),
      resources.topic_changes.end(),
      std::back_inserter(responses),
      gen_resp);

    std::for_each(
      unauthorized_responses.begin(), unauthorized_responses.end(), [](R& r) {
          r.error_code = error_code::broker_not_available;
          r.error_message = "Broker not available - audit system failure";
      });

    std::move(
      unauthorized_responses.begin(),
      unauthorized_responses.end(),
      std::back_inserter(responses));

    std::vector<chunked_vector<R>> res;
    res.push_back(std::move(responses));
    return res;
}

/**
 * Authorizes groupped alter configuration resources, it returns not authorized
 * responsens and modifies passed in group_resources<T>
 */
template<typename T, typename R>
chunked_vector<R> authorize_alter_config_resources(
  request_context& ctx, groupped_resources<T>& to_authorize) {
    chunked_vector<R> not_authorized;
    /**
     * Check broker configuration authorization
     */
    if (
      !to_authorize.broker_changes.empty()
      && !ctx.authorized(
        security::acl_operation::alter_configs,
        security::default_cluster_name)) {
        // not allowed
        std::transform(
          to_authorize.broker_changes.begin(),
          to_authorize.broker_changes.end(),
          std::back_inserter(not_authorized),
          [](T& res) {
              return make_error_alter_config_resource_response<R>(
                res, error_code::cluster_authorization_failed);
          });
        // all broker changes have to be dropped
        to_authorize.broker_changes.clear();
    }

    const auto& kafka_nodelete_topics
      = config::shard_local_cfg().kafka_nodelete_topics();
    const auto& kafka_noproduce_topics
      = config::shard_local_cfg().kafka_noproduce_topics();

    /**
     * Check topic configuration authorization
     */
    auto unauthorized_it = std::partition(
      to_authorize.topic_changes.begin(),
      to_authorize.topic_changes.end(),
      [&ctx, &kafka_nodelete_topics, &kafka_noproduce_topics](const T& res) {
          auto topic = model::topic(res.resource_name);

          auto is_nodelete_topic = std::find(
                                     kafka_nodelete_topics.cbegin(),
                                     kafka_nodelete_topics.cend(),
                                     topic)
                                   != kafka_nodelete_topics.cend();
          if (is_nodelete_topic) {
              return false;
          }

          auto is_noproduce_topic = std::find(
                                      kafka_noproduce_topics.cbegin(),
                                      kafka_noproduce_topics.cend(),
                                      topic)
                                    != kafka_noproduce_topics.cend();
          if (is_noproduce_topic) {
              return false;
          }

          return ctx.authorized(security::acl_operation::alter_configs, topic);
      });

    std::transform(
      unauthorized_it,
      to_authorize.topic_changes.end(),
      std::back_inserter(not_authorized),
      [](T& res) {
          return make_error_alter_config_resource_response<R>(
            res, error_code::topic_authorization_failed);
      });

    to_authorize.topic_changes.erase_to_end(unauthorized_it);

    return not_authorized;
}

template<typename T, typename R, typename Func>
ss::future<chunked_vector<R>> do_alter_topics_configuration(
  request_context& ctx,
  chunked_vector<T> resources,
  bool validate_only,
  Func f) {
    chunked_vector<R> responses;
    responses.reserve(resources.size());

    absl::node_hash_set<ss::sstring> topic_names;
    auto valid_end = std::stable_partition(
      resources.begin(), resources.end(), [&topic_names](T& r) {
          return !topic_names.contains(r.resource_name);
      });

    for (auto& r : boost::make_iterator_range(valid_end, resources.end())) {
        responses.push_back(make_error_alter_config_resource_response<R>(
          r,
          error_code::invalid_config,
          "duplicated topic {} alter config request"));
    }
    cluster::topic_properties_update_vector updates;
    for (auto& r : boost::make_iterator_range(resources.begin(), valid_end)) {
        auto res = f(r);
        if (res.has_error()) {
            responses.push_back(std::move(res.error()));
        } else {
            updates.push_back(std::move(res.value()));
        }
    }

    if (validate_only) {
        // all pending updates are valid, just generate responses
        for (auto& u : updates) {
            responses.push_back(R{
              .error_code = error_code::none,
              .resource_type = static_cast<int8_t>(config_resource_type::topic),
              .resource_name = u.tp_ns.tp,
            });
        }

        co_return responses;
    }

    auto update_results
      = co_await ctx.topics_frontend().update_topic_properties(
        std::move(updates),
        model::timeout_clock::now()
          + config::shard_local_cfg().alter_topic_cfg_timeout_ms());
    for (auto& res : update_results) {
        responses.push_back(R{
          .error_code = map_topic_error_code(res.ec),
          .resource_type = static_cast<int8_t>(config_resource_type::topic),
          .resource_name = res.tp_ns.tp(),
        });
    }

    // Group-by error code, value is list of topic names as strings
    absl::flat_hash_map<cluster::errc, std::vector<model::topic_namespace_view>>
      err_map;
    for (const auto& result : update_results) {
        auto [itr, _] = err_map.try_emplace(
          result.ec, std::vector<model::topic_namespace_view>());
        itr->second.emplace_back(result.tp_ns);
    }

    // Log success case
    auto found = err_map.find(cluster::errc::success);
    if (found != err_map.end()) {
        vlog(
          klog.info,
          "Altered topic properties of topic(s) {{{}}} successfully",
          fmt::join(found->second, ", "));
        err_map.erase(found);
    }

    // Log topics that had not successfully been created at warn level
    for (const auto& err : err_map) {
        vlog(
          klog.warn,
          "Failed to alter topic properties of topic(s) {{{}}} error_code "
          "observed: {}",
          fmt::join(err.second, ", "),
          err.first);
    }

    co_return responses;
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

class validation_error final : std::exception {
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

struct flush_ms_validator {
    std::optional<ss::sstring> operator()(
      const ss::sstring&,
      const std::optional<std::chrono::milliseconds>& maybe_value) {
        if (!maybe_value) {
            return std::nullopt;
        }
        auto value = maybe_value.value();
        if (value < 1ms || value > serde::max_serializable_ms) {
            return fmt::format(
              "flush.ms value invalid, expected to be in range [1, {}]",
              serde::max_serializable_ms);
        }
        return std::nullopt;
    }
};

struct flush_bytes_validator {
    std::optional<ss::sstring>
    operator()(const ss::sstring&, const std::optional<size_t>& maybe_value) {
        if (!maybe_value) {
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
      model::topic_namespace_view tns, const ss::sstring&, bool value) {
        if (!model::is_user_topic(tns)) {
            return fmt::format(
              "Iceberg configuration cannot be altered on non user topics");
        }
        if (!config::shard_local_cfg().iceberg_enabled() && value) {
            return fmt::format(
              "Iceberg disabled in the cluster configuration, enable it by "
              "setting: {}",
              config::shard_local_cfg().iceberg_enabled.name());
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
            auto str_value = std::move(*value);
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

template<typename T>
void parse_and_set_tristate(
  cluster::property_update<tristate<T>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value
    if (op == config_resource_operation::set) {
        auto parsed = boost::lexical_cast<int64_t>(*value);
        if (parsed <= 0) {
            property.value = tristate<T>{};
        } else {
            property.value = tristate<T>(std::make_optional<T>(parsed));
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

    ///\brief Parse a topic property from the supplied cfg.
    template<typename C>
    bool operator()(const C& cfg, kafka::config_resource_operation op) {
        using property_t = std::variant<
          decltype(&props.record_key_schema_id_validation),
          decltype(&props.record_key_subject_name_strategy)>;

        auto matcher = string_switch<std::optional<property_t>>(cfg.name);
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
              prop.value(), [&cfg, op](auto& p) { apply(*p, cfg.value, op); });
        }
        return prop.has_value();
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
