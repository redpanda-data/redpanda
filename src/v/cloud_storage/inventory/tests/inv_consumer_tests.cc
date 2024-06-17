/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_storage/inventory/inv_consumer.h"
#include "cloud_storage/inventory/ntp_hashes.h"
#include "cloud_storage/inventory/tests/common.h"
#include "test_utils/tmp_dir.h"
#include "utils/base64.h"

#include <absl/container/flat_hash_set.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <charconv>

using namespace cloud_storage::inventory;

namespace {

model::ntp make_ntp(std::string_view ns, std::string_view tp, int pid) {
    return model::ntp{
      model::ns{ns}, model::topic{tp}, model::partition_id{pid}};
}

// Wraps a string into a CSV row expected to be in the CSV report:
// "bucket","path"
ss::sstring r(std::string_view s) {
    return fmt::format(R"("bucket", "{}")", s);
}

bool is_hash_file(const std::filesystem::directory_entry& de) {
    const auto& p_str = de.path().filename().string();
    long v{};
    return de.is_regular_file()
           && std::from_chars(p_str.data(), p_str.data() + p_str.size(), v).ec
                != std::errc::invalid_argument;
}

std::vector<std::filesystem::path>
collect_hash_files(const std::filesystem::path& p) {
    std::vector<std::filesystem::path> hash_files;
    auto hash_files_view
      = std::filesystem::directory_iterator{p}
        | std::views::filter(is_hash_file)
        | std::views::transform(&std::filesystem::directory_entry::path)
        | std::views::transform(&std::filesystem::path::filename);
    std::ranges::copy(hash_files_view, std::back_inserter(hash_files));
    return hash_files;
}

// some paths from the following compressed file:
// 03be712b/kafka/topic-A/110_24/554-559-1573504-1-v1.log.2
// 15d8084c/kafka/topic-A/110_24/680-685-1573504-1-v1.log.2
// 2762eee5/kafka/toro/110_24/661-666-1573504-1-v1.log.2
// 28e3e323/kafka/toro/110_24/275-280-1573504-1-v1.log.1
// ea2201bd/kafka/partagas/2_24/142-147-1573504-1-v1.log.1
// eba06988/kafka/partagas/0_24/195-200-1573504-1-v1.log.1
// cac7164a/kafka/topic-B/2_24/305-310-1573504-1-v1.log.1
// cb18cb4d/kafka/topic-B/2_24/440-445-1573504-1-v1.log.2
constexpr auto compressed
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

} // namespace

TEST(ParseNTPFromPath, Consumer) {
    using p = std::pair<std::string_view, std::optional<model::ntp>>;
    std::vector<p> test_data{
      {"a0a6eeb8/kafka/topic-x/999_24/178-188-1574137-1-v1.log.1",
       std::make_optional(make_ntp("kafka", "topic-x", 999))},
      {"a/k/t/1_24/---", std::make_optional(make_ntp("k", "t", 1))},
      // Bad hex m
      {"m/k/t/1_24/---", std::nullopt},
      // Non-int pid
      {"a/k/t/x_24/---", std::nullopt},
      // Non-int rev-id
      {"a/k/t/1_x/---", std::nullopt},
      // Path ends without segment
      {"a/k/t/1_24", std::nullopt},
      // Malformed paths
      {"a/k/t/1_/---", std::nullopt},
      {"a/k/t/_0/---", std::nullopt},
      {"", std::nullopt},
      {"1_1", std::nullopt},
      {"////", std::nullopt},
    };

    for (const auto& [input, expected] : test_data) {
        EXPECT_EQ(inventory_consumer::ntp_from_path(input), expected);
    }
}

TEST(ParseCSV, Consumer) {
    inventory_consumer c{"", {}, 0};
    using p = std::pair<const char*, std::vector<ss::sstring>>;
    std::vector<p> test_data{
      {"x", {"x"}},
      {"x,y", {"x", "y"}},
      {"x,", {"x", ""}},
      {"", {}},
      {R"("bucket", "entry1")", {"bucket", "entry1"}},
      {R"("", "entry1")", {"", "entry1"}},
      {R"(, "entry1")", {"", "entry1"}},
      {R"("bucket", "entry1")"
       "\r\n",
       {"bucket", "entry1"}},
      {"\"bucket\", \"entry1\"\r", {"bucket", "entry1"}},
      {"\r\rbucket\n\n,\r\nentry1\r\r\n\n\r\r", {"bucket", "entry1"}},
    };
    for (const auto& [in, out] : test_data) {
        EXPECT_EQ(inventory_consumer::parse_row(in), out);
    }
}

TEST(LargeNumberOfPaths, Consumer) {
    temporary_dir t{"test_inv_consumer"};
    const auto p0 = make_ntp("kafka", "topic-A", 110);

    std::vector<ss::sstring> rows{
      100000, r("03be712b/kafka/topic-A/110_24/554-559-1573504-1-v1.log.2")};

    absl::node_hash_set<model::ntp> ntps{p0};
    inventory_consumer c{t.get_path(), ntps, 4096};

    c.consume(make_report_stream(rows), is_gzip_compressed::no).get();
    c.stop().get();

    auto hash_files = collect_hash_files(t.get_path() / std::string{p0.path()});
    ASSERT_GT(hash_files.size(), 1);

    // Make sure that all the sequence ids are seen as hash files in the data
    // path. The hash loader will pick up all files in the path, so the files
    // should not be in sequence, but this check makes sure that the inventory
    // consumer correctly increments the file sequence when writing, so we don't
    // end up overwriting any data.
    absl::flat_hash_set<ss::sstring> hash_file_names{
      hash_files.begin(), hash_files.end()};

    size_t min_expected = 0;
    size_t max_expected = hash_file_names.size() - 1;
    for (auto i = min_expected; i <= max_expected; ++i) {
        ASSERT_THAT(hash_file_names, testing::Contains(fmt::format("{}", i)));
    }
}

TEST(WriteThenReadHashes, Consumer) {
    temporary_dir t{"test_inv_consumer"};

    const auto p0 = make_ntp("kafka", "partagas", 0);
    const auto p2 = make_ntp("kafka", "partagas", 2);
    absl::node_hash_set<model::ntp> ntps{
      make_ntp("kafka", "topic-A", 110),
      make_ntp("kafka", "toro", 110),
      p0,
      p2,
      make_ntp("kafka", "topic-B", 2)};
    inventory_consumer c{t.get_path(), ntps, 20};
    auto decoded = base64_to_string(compressed);
    auto stream = make_report_stream(decoded);
    c.consume(std::move(stream), is_gzip_compressed::yes).get();
    c.stop().get();

    absl::node_hash_map<model::ntp, ss::lw_shared_ptr<ntp_path_hashes>> hashes;
    for (const auto& ntp : ntps) {
        auto h = ss::make_lw_shared<ntp_path_hashes>(ntp, t.get_path());
        hashes.insert({ntp, h});
        ASSERT_TRUE(hashes.at(ntp)->load_hashes().get());
    }

    EXPECT_EQ(
      hashes.at(p0)->exists(cloud_storage::remote_segment_path{
        "eba06988/kafka/partagas/0_24/195-200-1573504-1-v1.log.1"}),
      lookup_result::exists);
    EXPECT_EQ(
      hashes.at(p0)->exists(cloud_storage::remote_segment_path{"missing"}),
      lookup_result::missing);
    EXPECT_EQ(
      hashes.at(p2)->exists(cloud_storage::remote_segment_path{
        "ea2201bd/kafka/partagas/2_24/142-147-1573504-1-v1.log.1"}),
      lookup_result::exists);

    for (auto h : std::views::values(hashes)) {
        h->stop().get();
    }
}

TEST(CollisionDetection, Consumer) {
    temporary_dir t{"test_inv_consumer"};
    const auto p0 = make_ntp("kafka", "topic-A", 110);

    std::vector<ss::sstring> duplicate_rows{
      r("03be712b/kafka/topic-A/110_24/554-559-1573504-1-v1.log.2"),
      r("03be712b/kafka/topic-A/110_24/554-559-1573504-1-v1.log.2")};

    absl::node_hash_set<model::ntp> ntps{p0};
    inventory_consumer c{t.get_path(), ntps, 20};
    c.consume(make_report_stream(duplicate_rows), is_gzip_compressed::no).get();
    c.stop().get();

    ntp_path_hashes h{p0, t.get_path()};
    ASSERT_TRUE(h.load_hashes().get());

    EXPECT_EQ(
      h.exists(cloud_storage::remote_segment_path{
        "03be712b/kafka/topic-A/110_24/554-559-1573504-1-v1.log.2"}),
      lookup_result::possible_collision);
    h.stop().get();
}

TEST(ConsumeMultipleStreams, Consumer) {
    temporary_dir t{"test_inv_consumer"};
    const auto p0 = make_ntp("kafka", "partagas", 0);

    absl::node_hash_set<model::ntp> ntps{p0};
    inventory_consumer c{t.get_path(), ntps, 20};
    auto decoded = base64_to_string(compressed);
    auto fn = [&c](ss::input_stream<char> stream) {
        return c.consume(std::move(stream), is_gzip_compressed::yes);
    };

    // Simulates downloading multiple files and then consuming them. Since the
    // consumer will be used with remote, which provides the downloaded file as
    // a stream to the callback, this code simulates running the consumer in
    // sequence on multiple downloads.
    fn(make_report_stream(decoded)).get();
    fn(make_report_stream(decoded)).get();
    c.stop().get();

    ntp_path_hashes h{p0, t.get_path()};
    ASSERT_TRUE(h.load_hashes().get());

    EXPECT_EQ(
      h.exists(cloud_storage::remote_segment_path{
        "eba06988/kafka/partagas/0_24/195-200-1573504-1-v1.log.1"}),
      lookup_result::possible_collision);
    h.stop().get();
}
