/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/recovery_utils.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "storage/ntp_config.h"

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace {
// The top level path for all recovery state files on the cloud storage bucket
constexpr std::string_view recovery_result_prefix{"recovery_state"};

// The format for recovery state files, containing the NTP, a random UUID and
// the boolean status
// {recovery_result_prefix}/{ns}/{topic}/{partition}_{uuid}/{success:true|false}
constexpr std::string_view recovery_result_format{"{}/{}/{}/{}_{}.{}"};

// Matches the recovery_result_format. Example:
// "recovery_state/test_ns/test_topic/0_<UUID>.true" OR
// "recovery_state/test_ns/test_topic/0_<UUID>.false"
const std::regex result_expr{fmt::format(
  "{}/(.*?)/(.*?)/(\\d+)_(.*?).(true|false)", recovery_result_prefix)};
} // namespace

namespace cloud_storage {

recovery_result::recovery_result(
  model::topic_namespace topic_namespace,
  model::partition_id partition_id,
  ss::sstring uuid,
  bool result)
  : tp_ns(std::move(topic_namespace))
  , partition(partition_id)
  , uuid(std::move(uuid))
  , result(result) {}

ss::sstring
generate_result_path(const storage::ntp_config& ntp_cfg, bool result) {
    auto uuid = boost::uuids::random_generator()();
    return fmt::format(
      recovery_result_format,
      recovery_result_prefix,
      ntp_cfg.ntp().ns(),
      ntp_cfg.ntp().tp.topic(),
      ntp_cfg.ntp().tp.partition,
      boost::uuids::to_string(uuid),
      result);
}

ss::future<> place_download_result(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  const storage::ntp_config& ntp_cfg,
  bool result_completed,
  retry_chain_node& parent) {
    retry_chain_node fib{&parent};
    auto result_path = generate_result_path(ntp_cfg, result_completed);
    auto result = co_await remote.upload_object({
      .transfer_details
      = {.bucket = bucket, .key = cloud_storage_clients::object_key{result_path}, .parent_rtc = fib},
      .type = upload_type::download_result_file,
      .payload = iobuf{},
    });
    if (result != upload_result::success) {
        vlog(
          cst_log.error,
          "failed to upload result to {}, error: {}",
          result_path,
          result);
    }
    co_return;
}

ss::future<std::vector<recovery_result>> gather_recovery_results(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  retry_chain_node& parent) {
    retry_chain_node fib{&parent};
    std::vector<recovery_result> results{};
    auto result = co_await remote.list_objects(
      bucket, fib, cloud_storage_clients::object_key{recovery_result_prefix});
    if (result.has_error()) {
        vlog(
          cst_log.error, "failed to list recovery results: {}", result.error());
        co_return results;
    }

    results.reserve(result.value().contents.size());
    for (const auto& item : result.value().contents) {
        std::cmatch matches;
        if (std::regex_match(
              item.key.begin(), item.key.end(), matches, result_expr)) {
            results.emplace_back(
              model::topic_namespace{
                model::ns{matches[1].str()}, model::topic{matches[2].str()}},
              model::partition_id{std::stoi(matches[3].str())},
              matches[4].str(),
              matches[5].str() == "true");
        }
    }
    co_return results;
}

ss::future<> clear_recovery_results(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  retry_chain_node& parent,
  std::optional<std::vector<recovery_result>> items_to_delete) {
    retry_chain_node fib{&parent};
    if (!items_to_delete.has_value()) {
        auto r = co_await gather_recovery_results(remote, bucket, fib);
        items_to_delete.emplace(std::move(r));
    }

    if (items_to_delete->empty()) {
        vlog(cst_log.info, "skipping clear recovery results, nothing to clear");
        co_return;
    }

    std::vector<cloud_storage_clients::object_key> keys;
    keys.reserve(items_to_delete->size());
    std::transform(
      std::make_move_iterator(items_to_delete->begin()),
      std::make_move_iterator(items_to_delete->end()),
      std::back_inserter(keys),
      [](auto&& item) { return make_result_path(item); });

    vlog(cst_log.trace, "deleting {} result files", keys.size());
    auto result = co_await remote.delete_objects(bucket, keys, fib);
    if (result != upload_result::success) {
        vlog(cst_log.error, "failed to delete recovery results: {}", result);
    } else {
        vlog(cst_log.debug, "delete recovery results: {}", result);
    }

    co_return;
}

cloud_storage_clients::object_key make_result_path(const recovery_result& r) {
    return cloud_storage_clients::object_key{fmt::format(
      recovery_result_format,
      recovery_result_prefix,
      r.tp_ns.ns(),
      r.tp_ns.tp(),
      r.partition,
      r.uuid,
      r.result)};
}

} // namespace cloud_storage
