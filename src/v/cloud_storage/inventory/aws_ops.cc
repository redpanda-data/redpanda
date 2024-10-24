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

#include "bytes/streambuf.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "json/istreamwrapper.h"
#include "ssx/future-util.h"

#include <seastar/util/log.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <rapidjson/error/en.h>
#include <re2/re2.h>

#include <exception>
#include <ranges>

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
    destination.add("Bucket", fmt::format("arn:aws:s3:::{}", cfg.bucket()));
    return destination;
}

iobuf to_xml(const aws_report_configuration& cfg) {
    ptree inv_cfg;
    inv_cfg.put("<xmlattr>.xmlns", cfg.xmlns);
    inv_cfg.add_child(cfg.destination_path, destination_node(cfg));

    inv_cfg.add("IsEnabled", "true");
    inv_cfg.add("Id", cfg.inventory_id());
    inv_cfg.add("IncludedObjectVersions", "Current");

    inv_cfg.add(cfg.frequency_path, cfg.frequency);

    ptree root;
    root.put_child("InventoryConfiguration", inv_cfg);

    std::stringstream sstr;
    boost::property_tree::xml_parser::write_xml(sstr, root);

    iobuf b;
    b.append(sstr.str().data(), sstr.str().size());
    return b;
}

// Ends with YYYY-MM-DDTHH-MMZ/
const re2::RE2 trailing_date_expression{
  R"(.*/\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z/$)"};

struct path_with_datetime {
    cloud_storage::inventory::report_datetime datetime;
    cloud_storage_clients::object_key path;
};

bool ends_with_datetime(std::string_view path) {
    return RE2::FullMatch(path, trailing_date_expression);
}

path_with_datetime checksum_path(std::string_view datetime_path) {
    // We should have previously checked with a regex that the path ends with
    // a /date/ pattern. Throw an error here if that invariant is broken.
    if (!ends_with_datetime(datetime_path)) {
        throw std::invalid_argument{fmt::format(
          "unexpected path ending with datetime: {}", datetime_path)};
    }

    std::vector<std::string> parts;
    // prefix/bucket/inv-id/datetime
    parts.reserve(4);
    boost::split(parts, datetime_path, boost::is_any_of("/"));

    const auto& date_string = *(parts.end() - 2);
    return {
      .datetime = cloud_storage::inventory::
        report_datetime{date_string.data(), date_string.size()},
      .path = cloud_storage_clients::object_key{
        fmt::format("{}manifest.checksum", datetime_path)}};
}

constexpr auto max_logged_json_size = 128;

json::Document parse_json_response(iobuf resp) {
    iobuf_istreambuf ibuf{resp};
    std::istream stream{&ibuf};
    json::Document doc;
    json::IStreamWrapper wrapper(stream);
    doc.ParseStream(wrapper);
    return doc;
}

ss::sstring maybe_truncate_json(iobuf_parser p) {
    if (p.bytes_left() <= max_logged_json_size) {
        return p.read_string(p.bytes_left());
    }

    constexpr auto fragment_sz = max_logged_json_size / 4;
    const auto head = p.read_string(fragment_sz);
    p.skip(p.bytes_left() - fragment_sz);
    const auto tail = p.read_string(p.bytes_left());
    return head + "..." + tail;
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

ss::future<op_result<void>> aws_ops::create_inventory_configuration(
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

    const auto create_res = co_await remote.upload_object(
      {.transfer_details
       = {.bucket = _bucket, .key = _inventory_key, .parent_rtc = parent_rtc},
       .type = upload_type::inventory_configuration,
       .payload = to_xml(cfg)});

    if (create_res != upload_result::success) {
        vlog(
          cst_log.error,
          "Failed to create inventory {}: {}",
          _inventory_config_id(),
          create_res);
        co_return error_outcome::create_inv_cfg_failed;
    }

    co_return outcome::success();
}

ss::future<op_result<bool>> aws_ops::inventory_configuration_exists(
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
       .payload = buf,
       .expect_missing = true});

    if (dl_res == download_result::success) {
        co_return true;
    }

    if (dl_res == download_result::notfound) {
        co_return false;
    }

    vlog(
      cst_log.error,
      "Failed to check if inventory {} exists: {}",
      _inventory_config_id(),
      dl_res);
    co_return error_outcome::failed_to_check_inv_status;
}

ss::future<op_result<report_metadata>> aws_ops::fetch_latest_report_metadata(
  cloud_storage::cloud_storage_api& remote,
  retry_chain_node& parent_rtc) const noexcept {
    try {
        co_return co_await do_fetch_latest_report_metadata(remote, parent_rtc);
    } catch (...) {
        // We are logging at error level here because we are not expecting
        // exceptions to be thrown from this method. Errors are returned as
        // results instead. If we get an exception, then likely it is a bug.
        // Shutdown exceptions are ... an exception to this rule, and are logged
        // at info level.
        auto ex = std::current_exception();
        auto ex_log_level = ss::log_level::error;
        if (ssx::is_shutdown_exception(ex)) {
            ex_log_level = ss::log_level::info;
        }
        vlogl(
          cst_log,
          ex_log_level,
          "Failed to fetch latest report metadata: {}",
          std::current_exception());
        co_return error_outcome::failed;
    }
}

ss::future<op_result<report_metadata>> aws_ops::do_fetch_latest_report_metadata(
  cloud_storage::cloud_storage_api& remote,
  retry_chain_node& parent_rtc) const {
    // The prefix is generated according to
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html
    const auto full_prefix = cloud_storage_clients::object_key{
      fmt::format("{}/{}/{}/", _prefix, _bucket, _inventory_config_id)};

    vlog(cst_log.trace, "Listing inventory report prefix {}", full_prefix());
    const auto list_result = co_await remote.list_objects(
      _bucket, parent_rtc, full_prefix, '/');

    if (list_result.has_error()) {
        vlog(
          cst_log.error,
          "Failed to list report prefix: {}",
          list_result.error());
        co_return error_outcome::failed;
    }

    vlog(
      cst_log.trace,
      "Found common prefixes: {}",
      list_result.value().common_prefixes);

    // The prefix may contain non-datetime folders such as hive data, which we
    // ignore. The datetime folders are normalized with the prefix, and then the
    // checksum file path is appended to them. The final result is a series of
    // paths to checksums which we need to probe.
    auto filtered = list_result.value().common_prefixes
                    | std::views::filter(ends_with_datetime)
                    | std::views::transform(checksum_path);

    std::vector<path_with_datetime> checksum_paths{
      filtered.begin(), filtered.end()};

    // The datetime strings (YYYY-MM-DDTHH-MMZ) can be sorted by lexical
    // comparison
    std::ranges::sort(
      checksum_paths, std::ranges::greater{}, &path_with_datetime::datetime);

    auto projected = checksum_paths
                     | std::views::transform(&path_with_datetime::path);
    std::vector<cloud_storage_clients::object_key> paths{
      projected.begin(), projected.end()};
    vlog(cst_log.trace, "Possible checksum paths: {}", paths);

    // A checksum appears in the bucket at the designated location only once the
    // inventory is ready, so we probe the checksum files from latest to
    // earliest. We stop at the first checksum we find.
    for (const auto& path_with_datetime : checksum_paths) {
        const auto result = co_await remote.object_exists(
          _bucket,
          path_with_datetime.path,
          parent_rtc,
          existence_check_type::object);

        // The latest checksum file does not exist. The report is not ready yet.
        if (result == download_result::notfound) {
            // Try the next latest report.
            continue;
        }

        // If we failed to check for the checksum existence, instead of probing
        // older reports we bail out and try again later.
        if (result != download_result::success) {
            vlog(
              cst_log.error,
              "Failed to probe checksum object at {}: {}",
              path_with_datetime.path().native(),
              result);
            co_return error_outcome::failed;
        }

        auto path = path_with_datetime.path();

        // Once a checksum is found, we download the corresponding
        // manifest.json file
        path.replace_extension("json");

        const auto manifest_path = cloud_storage_clients::object_key{path};

        // The manifest is downloaded, parsed and report paths are extracted
        // from it.
        auto report_paths = co_await fetch_and_parse_metadata(
          remote, parent_rtc, manifest_path);

        if (report_paths.has_error()) {
            vlog(
              cst_log.error,
              "Failed to fetch metadata: {}",
              report_paths.error());
            co_return report_paths.error();
        }

        co_return report_metadata{
          .metadata_path = manifest_path,
          .report_paths = report_paths.value(),
          .datetime = path_with_datetime.datetime};
    }

    co_return error_outcome::no_reports_found;
}

ss::future<op_result<report_paths>> aws_ops::fetch_and_parse_metadata(
  cloud_storage::cloud_storage_api& remote,
  retry_chain_node& parent_rtc,
  cloud_storage_clients::object_key metadata_path) const noexcept {
    try {
        co_return co_await do_fetch_and_parse_metadata(
          remote, parent_rtc, metadata_path);
    } catch (...) {
        vlog(
          cst_log.error,
          "Failed to fetch and parse metadata: {}",
          std::current_exception());
        co_return error_outcome::failed;
    }
}

ss::future<op_result<report_paths>> aws_ops::do_fetch_and_parse_metadata(
  cloud_storage::cloud_storage_api& remote,
  retry_chain_node& parent_rtc,
  cloud_storage_clients::object_key metadata_path) const {
    iobuf json_recv;
    const auto dl_res = co_await remote.download_object({
      .transfer_details
      = {.bucket = _bucket, .key = metadata_path, .parent_rtc = parent_rtc},
      .type = download_type::inventory_report_manifest,
      .payload = json_recv,
    });

    if (dl_res != download_result::success) {
        vlog(cst_log.error, "Failed to download manifest JSON: {}", dl_res);
        co_return error_outcome::manifest_download_failed;
    }

    co_return parse_report_paths(std::move(json_recv));
}

op_result<report_paths> aws_ops::parse_report_paths(iobuf json_response) const {
    auto err_parser = iobuf_parser{json_response.copy()};
    const auto doc = parse_json_response(std::move(json_response));
    if (doc.HasParseError()) {
        vlog(
          cst_log.error,
          "JSON parse error at offset {}: {}, JSON document: {}",
          doc.GetErrorOffset(),
          rapidjson::GetParseError_En(doc.GetParseError()),
          maybe_truncate_json(std::move(err_parser)));
        return error_outcome::manifest_deserialization_failed;
    }

    if (!doc.IsObject() || !doc.HasMember("files") || !doc["files"].IsArray()) {
        vlog(
          cst_log.error,
          "Document does not have a files array: {}",
          maybe_truncate_json(std::move(err_parser)));
        return error_outcome::manifest_files_parse_failed;
    }

    auto has_embedded_key_path = [](const auto& f) {
        return f.IsObject() && f.HasMember("key") && f["key"].IsString();
    };

    auto paths = doc["files"].GetArray()
                 | std::views::filter(has_embedded_key_path)
                 | std::views::transform(
                   [](const auto& f) { return f["key"].GetString(); });

    return {paths.begin(), paths.end()};
}

} // namespace cloud_storage::inventory
