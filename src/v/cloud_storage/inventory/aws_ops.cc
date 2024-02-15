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

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

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

} // namespace

namespace cloud_storage::inventory {

aws_ops::aws_ops(
  cloud_storage_clients::bucket_name bucket,
  inventory_config_id inventory_config_id,
  ss::sstring inventory_prefix)
  : _bucket(std::move(bucket))
  , _inventory_config_id(std::move(inventory_config_id))
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

    const auto key = fmt::format("?inventory&id={}", _inventory_config_id());
    co_return co_await remote.upload_object(
      {.bucket_name = _bucket,
       .key = cloud_storage_clients::object_key{key},
       .payload = to_xml(cfg),
       .parent_rtc = parent_rtc});
}

} // namespace cloud_storage::inventory
