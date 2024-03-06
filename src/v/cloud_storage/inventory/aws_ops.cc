/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/inventory/aws_ops.h"

#include "cloud_storage/remote.h"

#include <boost/algorithm/string/replace.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <re2/re2.h>

using boost::property_tree::ptree;

namespace {

struct aws_report_configuration {
    static constexpr auto xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    static constexpr auto destination_path = "Destination.S3BucketDestination";
    static constexpr auto frequency_path = "Schedule.Frequency";

    cloud_storage_clients::bucket_name bucket;
    cloud_storage::inventory::inventory_config_id inventory_id;

    cloud_storage::inventory::report_format format;
    ss::sstring prefix;

    cloud_storage::inventory::report_generation_frequency frequency;
};

ptree destination_node(const aws_report_configuration& cfg) {
    ptree destination;
    destination.add("Format", cfg.format);
    destination.add("Prefix", cfg.prefix);
    destination.add("Bucket", fmt::format("arn::aws::s3:::{}", cfg.bucket()));
    return destination;
}

iobuf to_xml(const aws_report_configuration& cfg) {
    ptree inv_cfg;
    inv_cfg.put("<xmlattr>.xmlns", cfg.xmlns);
    inv_cfg.add_child(cfg.destination_path, destination_node(cfg));

    inv_cfg.add("IsEnabled", "true");
    inv_cfg.add("Id", cfg.inventory_id());

    inv_cfg.add(cfg.frequency_path, cfg.frequency);

    ptree root;
    root.put_child("InventoryConfiguration", inv_cfg);

    std::stringstream sstr;
    boost::property_tree::xml_parser::write_xml(sstr, root);

    iobuf b;
    b.append(sstr.str().data(), sstr.str().size());
    return b;
}

// YYYY-MM-DDTHH-MMZ/
const re2::RE2 trailing_date_expression{R"(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z/)"};

bool is_datetime(std::string_view path) {
    return RE2::FullMatch(path, trailing_date_expression);
}

ss::sstring checksum_path(std::string_view path) {
    return fmt::format("{}manifest.checksum", path);
}

ss::sstring manifest_path(std::string path) {
    boost::replace_last(path, "checksum", "json");
    return path;
}

} // namespace

namespace cloud_storage::inventory {

aws_ops::aws_ops(
  cloud_storage_clients::bucket_name bucket,
  inventory_config_id inventory_config_id,
  ss::sstring inventory_prefix)
  : _bucket(std::move(bucket))
  , _inventory_config_id(std::move(inventory_config_id))
  , _inventory_key(cloud_storage_clients::object_key{
      fmt::format("?inventory&id={}", _inventory_config_id())})
  , _prefix(std::move(inventory_prefix)) {}

ss::future<cloud_storage::upload_result>
aws_ops::create_inventory_configuration(
  cloud_storage::cloud_storage_api& remote,
  retry_chain_node& parent_rtc,
  report_generation_frequency frequency,
  report_format format) {
    const aws_report_configuration cfg{
      .bucket = _bucket,
      .inventory_id = _inventory_config_id,
      .format = format,
      .prefix = _prefix,
      .frequency = frequency};

    co_return co_await remote.upload_object(
      {.transfer_details
       = {.bucket = _bucket, .key = _inventory_key, .parent_rtc = parent_rtc},
       .type = upload_type::inventory_configuration,
       .payload = to_xml(cfg)});
}

ss::future<bool> aws_ops::inventory_configuration_exists(
  cloud_storage::cloud_storage_api& remote, retry_chain_node& parent_rtc) {
    // This buffer is thrown away after the GET call returns, we might introduce
    // some sort of struct (or reuse the aws_report_configuration used for
    // serialization), but for AWS we generally do not need the actual report
    // config, the HTTP status for the GET call is sufficient to tell us if the
    // config exists.
    iobuf buf;
    auto dl_res = co_await remote.download_object(
      {.transfer_details
       = {.bucket = _bucket, .key = _inventory_key, .parent_rtc = parent_rtc},
       .payload = buf});
    co_return dl_res == download_result::success;
}

ss::future<result<report_metadata_path, cloud_storage_clients::error_outcome>>
aws_ops::latest_report_path(
  cloud_storage::cloud_storage_api& remote, retry_chain_node& parent_rtc) {
    // The prefix is generated according to
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html
    const auto prefix = cloud_storage_clients::object_key{
      fmt::format("{}/{}/{}/", _prefix, _bucket, _inventory_config_id)};
    const auto list_result = co_await remote.list_objects(
      _bucket, parent_rtc, prefix, '/');

    if (list_result.has_error()) {
        co_return list_result.error();
    }

    // The prefix may contain non-datetime folders such as hive data, which we
    // ignore. The datetime folders are normalized with the prefix, and then the
    // checksum file path is appended to them. The final result is a series of
    // paths to checksums which we need to probe.
    auto filtered = list_result.value().common_prefixes
                    | std::views::filter(is_datetime)
                    | std::views::transform([&prefix](const auto& date_string) {
                          return fmt::format(
                            "{}{}", prefix().native(), date_string);
                      })
                    | std::views::transform(checksum_path);

    std::vector<std::string> cksum_paths{filtered.begin(), filtered.end()};
    // The datetime strings (YYYY-MM-DDTHH-MMZ) can be sorted by lexical
    // comparison
    std::ranges::sort(cksum_paths, std::ranges::greater());

    // A checksum appears in the bucket at the designated location only once the
    // inventory is ready, so we probe the checksum files from latest to
    // earliest. We stop at the first checksum we find.
    for (auto& checksum_path : cksum_paths) {
        if (const auto result = co_await remote.object_exists(
              _bucket,
              cloud_storage_clients::object_key{checksum_path},
              parent_rtc,
              existence_check_type::object);
            result == download_result::success) {
            // Since the actual report paths will be embedded within the
            // manifest, and the manifest and the report is guaranteed to exist
            // once the checksum is found, we return the path to manifest.
            co_return report_metadata_path{
              manifest_path(std::move(checksum_path))};
        }
    }

    co_return cloud_storage_clients::error_outcome::key_not_found;
}

} // namespace cloud_storage::inventory
