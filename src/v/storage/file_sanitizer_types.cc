/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage/file_sanitizer_types.h"

#include "json/document.h"
#include "json/schema.h"
#include "json/stringbuffer.h"
#include "json/validator.h"
#include "json/writer.h"
#include "model/fundamental.h"
#include "storage/logger.h"
#include "strings/string_switch.h"
#include "utils/file_io.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>

#include <functional>

namespace {
std::optional<model::record_batch_type>
batch_type_from_string_view(std::string_view sv) {
    using bt = model::record_batch_type;
    return string_switch<std::optional<bt>>(sv)
      .match("batch_type::raft_data", bt::raft_data)
      .match("batch_type::raft_configuration", bt::raft_configuration)
      .match("batch_type::controller", bt::controller)
      .match("batch_type::kvstore", bt::kvstore)
      .match("batch_type::checkpoint", bt::checkpoint)
      .match("batch_type::topic_management_cmd", bt::topic_management_cmd)
      .match("batch_type::ghost_batch", bt::ghost_batch)
      .match("batch_type::id_allocator", bt::id_allocator)
      .match("batch_type::tx_prepare", bt::tx_prepare)
      .match("batch_type::tx_fence", bt::tx_fence)
      .match("batch_type::tm_update", bt::tm_update)
      .match("batch_type::user_management_cmd", bt::user_management_cmd)
      .match("batch_type::acl_management_cmd", bt::acl_management_cmd)
      .match("batch_type::group_prepare_tx", bt::group_prepare_tx)
      .match("batch_type::group_commit_tx", bt::group_commit_tx)
      .match("batch_type::group_abort_tx", bt::group_abort_tx)
      .match("batch_type::node_management_cmd", bt::node_management_cmd)
      .match(
        "batch_type::data_policy_management_cmd",
        bt::data_policy_management_cmd)
      .match("batch_type::archival_metadata", bt::archival_metadata)
      .match("batch_type::cluster_config_cmd", bt::cluster_config_cmd)
      .match("batch_type::feature_update", bt::feature_update)
      .match("batch_type::cluster_bootstrap_cmd", bt::cluster_bootstrap_cmd)
      .match("batch_type::version_fence", bt::version_fence)
      .default_match(std::nullopt);
}

storage::failable_op_type op_type_from_string_view(const std::string_view& s) {
    using namespace storage;

    return string_switch<failable_op_type>(s)
      .match("write", failable_op_type::write)
      .match("falloc", failable_op_type::falloc)
      .match("flush", failable_op_type::flush)
      .match("truncate", failable_op_type::truncate)
      .match("close", failable_op_type::close);
}

storage::failable_op_config
from_json(const json::Value& key, const json::Value& value) {
    storage::failable_op_config op_config;
    op_config.op_type = op_type_from_string_view(
      std::string_view{key.GetString(), key.GetStringLength()});

    if (auto it = value.FindMember("batch_type"); it != value.MemberEnd()) {
        auto batch_type = batch_type_from_string_view(
          std::string_view{it->value.GetString(), it->value.GetStringLength()});

        if (!batch_type.has_value()) {
            throw std::runtime_error(fmt::format(
              "JSON failure injection config specifies invalid batch type: {}",
              key.GetString()));
        }

        op_config.batch_type = batch_type;
    }

    if (auto it = value.FindMember("failure_probability");
        it != value.MemberEnd()) {
        op_config.failure_probability = it->value.GetDouble();
    }

    if (auto it = value.FindMember("delay_probability");
        it != value.MemberEnd()) {
        op_config.delay_probability = it->value.GetDouble();

        if (auto it = value.FindMember("min_delay_ms");
            it != value.MemberEnd()) {
            op_config.min_delay_ms = it->value.GetInt();
        } else {
            throw std::runtime_error(
              "JSON failure injection config specifies "
              "'delay_probability', but no 'min_delay_ms'");
        }

        if (auto it = value.FindMember("max_delay_ms");
            it != value.MemberEnd()) {
            op_config.max_delay_ms = it->value.GetInt();
        } else {
            throw std::runtime_error(
              "JSON failure injection config specifies "
              "'delay_probability', but no 'max_delay_ms'");
        }
    }

    return op_config;
}

storage::ntp_sanitizer_config
from_json(const json::Value& ntp_cfg, size_t seed) {
    auto ns = ntp_cfg["namespace"].GetString();
    auto topic = ntp_cfg["topic"].GetString();
    auto partition = ntp_cfg["partition"].GetInt();

    model::ntp ntp{ns, topic, partition};
    const auto& failure_configs = ntp_cfg["failure_configs"].GetObject();

    std::vector<storage::failable_op_config> op_configs;
    for (auto it = failure_configs.MemberBegin();
         it != failure_configs.MemberEnd();
         ++it) {
        op_configs.push_back(from_json(it->name, it->value));
    }

    storage::ntp_failure_injection_config finject_cfg{
      .ntp = std::move(ntp), .seed = seed, .op_configs = std::move(op_configs)};

    return {.sanitize_only = false, .finjection_cfg = std::move(finject_cfg)};
}

} // namespace

namespace storage {
file_sanitize_config::file_sanitize_config(bool sanitize_only)
  : _sanitize_only(sanitize_only) {}

file_sanitize_config::file_sanitize_config(json::Document json) {
    auto validator = json::validator(failure_injector_schema);
    json::validate(validator, json);

    auto global_seed = json["seed"].GetInt();
    const auto& ntp_failure_configs = json["ntps"];
    for (const auto& ntp_cfg : ntp_failure_configs.GetArray()) {
        auto ns = ntp_cfg["namespace"].GetString();
        auto topic = ntp_cfg["topic"].GetString();
        auto partition = ntp_cfg["partition"].GetInt();

        model::ntp ntp{ns, topic, partition};
        auto mapped_ntp_cfg = from_json(ntp_cfg, global_seed);
        _ntp_failure_configs[ntp] = std::move(mapped_ntp_cfg);
    }
}

std::optional<ntp_sanitizer_config>
file_sanitize_config::get_config_for_ntp(const model::ntp& ntp) const {
    auto iter = _ntp_failure_configs.find(ntp);
    if (iter != _ntp_failure_configs.end()) {
        return iter->second;
    }

    return std::nullopt;
}

std::ostream& operator<<(std::ostream& o, const ntp_sanitizer_config& cfg) {
    o << "{sanitize_only=" << cfg.sanitize_only
      << ", failure_injection=" << static_cast<bool>(cfg.finjection_cfg) << "}";

    return o;
}

std::ostream& operator<<(std::ostream& o, const file_sanitize_config& cfg) {
    o << "{sanitize_only=" << cfg._sanitize_only
      << ", ntps_with_failure_injection_count: "
      << cfg._ntp_failure_configs.size() << "}";

    return o;
}

std::ostream& operator<<(std::ostream& o, failable_op_type op) {
    switch (op) {
    case failable_op_type::write:
        return o << "write";
    case failable_op_type::falloc:
        return o << "falloc";
    case failable_op_type::flush:
        return o << "flush";
    case failable_op_type::truncate:
        return o << "truncate";
    case failable_op_type::close:
        return o << "close";
    }
}

std::istream& operator>>(std::istream& i, failable_op_type& op) {
    ss::sstring s;
    i >> s;

    op = op_type_from_string_view(s);

    return i;
}

ss::future<std::optional<file_sanitize_config>>
make_finjector_file_config(std::filesystem::path config_path) {
    if (!co_await ss::file_exists(config_path.native())) {
        vlog(
          storage::stlog.warn,
          "Failure injection config not present at {}",
          config_path);

        co_return std::nullopt;
    }

    auto json_config = co_await read_fully_to_string(config_path);

    json::Document doc;
    if (doc.Parse(json_config.data()).HasParseError()) {
        vlog(
          storage::stlog.warn,
          "Failed to read failure injection config file at {}: {}",
          config_path,
          doc.GetParseError());
    }

    try {
        co_return file_sanitize_config{std::move(doc)};
    } catch (const std::exception& e) {
        vlog(
          storage::stlog.warn,
          "Failed to parse failure injection config file at {}: {}",
          config_path,
          e.what());
    }

    co_return std::nullopt;
}

file_sanitize_config make_sanitized_file_config() {
    return file_sanitize_config{true};
}
} // namespace storage
