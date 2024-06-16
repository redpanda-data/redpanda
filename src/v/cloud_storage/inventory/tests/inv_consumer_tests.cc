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
#include "cloud_storage/inventory/tests/common.h"
#include "test_utils/tmp_dir.h"

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
