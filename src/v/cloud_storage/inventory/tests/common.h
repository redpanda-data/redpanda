/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/inventory/inv_consumer.h"
#include "cloud_storage/inventory/report_parser.h"
#include "cloud_storage/remote.h"

#include <gmock/gmock.h>

namespace cloud_storage::inventory {

class MockRemote : public cloud_storage::cloud_storage_api {
public:
    MOCK_METHOD(
      ss::future<cloud_storage::upload_result>,
      upload_object,
      (cloud_storage::upload_request),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::download_result>,
      download_object,
      (cloud_storage::download_request),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::cloud_storage_api::list_result>,
      list_objects,
      (const cloud_storage_clients::bucket_name&,
       retry_chain_node&,
       std::optional<cloud_storage_clients::object_key>,
       std::optional<char>,
       std::optional<cloud_storage_clients::client::item_filter>,
       std::optional<size_t>,
       std::optional<ss::sstring>),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::download_result>,
      object_exists,
      (const cloud_storage_clients::bucket_name&,
       const cloud_storage_clients::object_key&,
       retry_chain_node&,
       existence_check_type),
      (override));
    MOCK_METHOD(
      ss::future<download_result>,
      download_stream,
      (const cloud_storage_clients::bucket_name& bucket,
       const remote_segment_path& path,
       const try_consume_stream& cons_str,
       retry_chain_node& parent,
       const std::string_view stream_label,
       const download_metrics& metrics,
       std::optional<cloud_storage_clients::http_byte_range> byte_range),
      (override));
};

ss::input_stream<char> make_report_stream(
  ss::sstring s, is_gzip_compressed compress = is_gzip_compressed::no);

ss::input_stream<char> make_report_stream(
  std::vector<ss::sstring> rows,
  is_gzip_compressed compress = is_gzip_compressed::no);

class inventory_consumer_accessor {
public:
    explicit inventory_consumer_accessor(inventory_consumer& c);

    size_t num_flushes() const;

private:
    inventory_consumer& _c;
};

// some paths from the following compressed file:
// 03be712b/kafka/topic-A/110_24/554-559-1573504-1-v1.log.2
// 15d8084c/kafka/topic-A/110_24/680-685-1573504-1-v1.log.2
// 2762eee5/kafka/toro/110_24/661-666-1573504-1-v1.log.2
// 28e3e323/kafka/toro/110_24/275-280-1573504-1-v1.log.1
// ea2201bd/kafka/partagas/2_24/142-147-1573504-1-v1.log.1
// eba06988/kafka/partagas/0_24/195-200-1573504-1-v1.log.1
// cac7164a/kafka/topic-B/2_24/305-310-1573504-1-v1.log.1
// cb18cb4d/kafka/topic-B/2_24/440-445-1573504-1-v1.log.2
inline constexpr auto compressed
  = "H4sICLOMcmYAA3Rlc3QAjZjJclw3DEX3+QytQ4sEMZBL50dcHG2XnJJLlvL9QavbivwMhm+"
    "nzTvCcHEB9l1x9aU9jOe7P++873OCD/"
    "cPZT6U++fH71+b+3gfgv8EeI8RHcbsAkkkjy64f8KHb4+"
    "fP8DdH3fvKbEOCVBtChE6ohMUGWm0gjYFYnAQ+"
    "XdKOFAyTvFhkRFoLGDFcqAE6sknbDaFk3ecaJtRYJ6Q4oISwd1qKyGvIxHqjI1XjOjiayQp+"
    "BDXlNZiqwkWfRbvUE7kMwAl83/5PD2+Ndl7R/4Mokih1A0E++T4ksMGAZBbbq0YCARwCLJtL/"
    "BsjN1MBMERGohjFMIwxiArEQ6O2ZDqEZFGHNpBAwHaDVCVbRNpvYTYxEpEkqO0L2eMcXafZCGw"
    "fFHpvqIx0oBRF8Mrwcl+"
    "dGNuLKWkxejqkADgnlKBJbaVASS1kXQilllmg7GoCrBTO9ox0Heqfi4YIi7BCcYUJlqMPwfUkd"
    "lbKzJHQVpZaxIH+UQsjQGjX5gIg5oi7B0AR66CfUGBqLFcxw+D/"
    "r2KhcLFRmShFdLpIzE6dIiFYsk0uSwoSRdX2leXhErnNG1KiDrG0ajLMSNltA7L6rJjS3PHWGq"
    "IWd1+YfSUHPLeE6h5EYp9kZF+uF+"
    "h1KKvjEflXiurNSGrJsc4ZpVE4zjLV2cK4iLsVUtz4GwpWgyVrKXYA4FjmnP+"
    "pvtbFEGtYO8nXKoHrCYDcnbR752N25RJcuzKbW2p29O+s5JzoNiOir9tT1VHPMWYGVq2GMlZG/"
    "yQiHSOJQCZBc0ahD/BGMVHElNejGqLuB9cmVF0YI6De7N4XTeyb4pM9Y/ejyZ/Lah+h34/"
    "KqnWlFDMODArI+9zSXP4WfjXpvx1E0fWcmyjyL5H8dMkRF2Z6it7RvShNigWQ58ZDvx+"
    "VDIW9rWAxQiXs8oy9iODS8RB3WLoTZTSniDaEZ2IG+Hpsb78eH689USPGUx7M866/"
    "SuNbDHC5S2D+0yKvmVy7dNisAfHfn+rFl94jGrmEvRMDNd6/O/"
    "CLRk61BhMxiWGsLfiUpp6T7TrwbomeW/"
    "GpQHo7q8WAzQOOBFH9VWg5GYy1EjBMtIjQ4sVejQVpgtDB25vpDVU1P8lptLx0pMTcUDXbBjNu"
    "Q+6FOBEHOSh6cPHzEU9kGXvP5U66NhOMxed/"
    "HBi8itnyn2QyRDtq+"
    "wfIjW1ktIwc0E9uDHt56WWWQNVs6ZMeuayv87LZX5XjNpEd5zphIjqH7T3jzpj61yTyVBHR973"
    "pYUyA/"
    "KxHvCqdT1+"
    "IO5fmS330gmHxSB9khGciKM0CYzHerwyoic9ok7EUUNqFY8zB9eaeoe4Pyn75acSKcctd81F+"
    "0qWBx0ZVFoa4diXaxx6DCLsfb2z3rZ++"
    "hujji9fH8YVkTUCv69Gz75BHdUgMIsO7W3w01qkvQyqkYeBUIlaCj3GUGMHevtd6T0gXN4IV2H"
    "g67JbIXpjPRvYSoOcnKiD5jD57Xp6DyA1DbJM41iHWUdisupAuk/0sblFDH3PxPimiV/"
    "SiLqm4wlEFg69W5IIl2do3t9wo4CeWfXnhHwvT8/lc/"
    "lxg6BaqPXT1hFSi+ec0hFy9WFVJ5xQ56hj5DTZhJC/qGtvgKPrSk9oQ3TdOODrJSc6/"
    "ctIumDNbz8ItS8vT+3L12/f7kXkU7gHfbBA2B+E0/"
    "chBXGBQdLdQvsmT6BeCOcCEy7PpzPRxIGthL7AaDCUfh//IwT14MeQFxDvrunI5aW+QqSRe24/"
    "DeDv0l+eHm/fq2ZPSHa2Hn3D/CvhqjW6fL5/P80xO7e31rxHUGY9kW2l/QtIMRWsCRkAAA==";

bool is_hash_file(const std::filesystem::directory_entry& de);

std::vector<std::filesystem::path>
collect_hash_files(const std::filesystem::path& p);

} // namespace cloud_storage::inventory
