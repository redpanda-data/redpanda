/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf_parser.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "iceberg/json_writer.h"
#include "iceberg/rest_catalog.h"
#include "iceberg/rest_client/catalog_client.h"
#include "iceberg/table_metadata_json.h"
#include "json/chunked_buffer.h"

#include <seastar/core/sleep.hh>

#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;
using namespace std::chrono_literals;
namespace {
class client_mock : public http::abstract_client {
public:
    MOCK_METHOD(
      ss::future<http::downloaded_response>,
      request_and_collect_response,
      (boost::beast::http::request_header<>&&,
       std::optional<iobuf>,
       ss::lowres_clock::duration),
      (override));

    MOCK_METHOD(ss::future<>, shutdown_and_stop, (), (override));
};

static constexpr auto endpoint = "http://localhost:8181";
iceberg::rest_client::credentials get_credentials() {
    return {
      .client_id = "redpanda",
      .client_secret = "being_cute",
    };
}
struct RestCatalogTest
  : Test
  , s3_imposter_fixture {
    RestCatalogTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , io(remote(), bucket_name) {
        set_expectations_and_listen({});
    }

    cloud_io::remote& remote() { return sr->remote.local(); }

    std::unique_ptr<iceberg::rest_client::catalog_client>
    make_catalog_client(const std::function<void(client_mock&)>& expectations) {
        auto http_client = std::make_unique<client_mock>();
        expectations(*http_client);
        return std::make_unique<iceberg::rest_client::catalog_client>(
          std::move(http_client),
          endpoint,
          get_credentials(),
          iceberg::rest_client::base_path{"/catalog"},
          std::nullopt,
          iceberg::rest_client::api_version("v1"));
    }
    std::unique_ptr<cloud_io::scoped_remote> sr;
    iceberg::manifest_io io;
};
AssertionResult query_params_equal(
  absl::flat_hash_map<ss::sstring, ss::sstring> expected,
  std::string_view received) {
    absl::flat_hash_map<ss::sstring, ss::sstring> received_params;
    std::vector<std::string> params = absl::StrSplit(received, "&");

    for (auto& p : params) {
        std::vector<std::string> kv = absl::StrSplit(p, "=");
        received_params[kv[0]] = kv[1];
    }

    if (expected.size() != received_params.size()) {
        return AssertionFailure() << fmt::format(
                 "Received parameters size is different than expected. "
                 "Received: {}, expected: {}",
                 fmt::join(received_params | std::views::keys, ", "),
                 fmt::join(expected | std::views::keys, ", "));
    }

    for (auto [k, v] : expected) {
        auto it = received_params.find(k);
        if (it == received_params.end()) {
            return AssertionFailure()
                   << fmt::format("missing {} in received query parameters", k);
        }

        if (it->second != v) {
            return AssertionFailure() << fmt::format(
                     "received query parameter mismatch {} != {}",
                     v,
                     it->second);
        }
    }
    return AssertionSuccess();
}
ss::future<http::downloaded_response> handle_token_request(
  boost::beast::http::request_header<>&& r,
  std::optional<iobuf> payload,
  [[maybe_unused]] ss::lowres_clock::duration timeout) {
    EXPECT_EQ(r.at(boost::beast::http::field::host), "localhost:8181");
    EXPECT_EQ(
      r.at(boost::beast::http::field::content_type),
      "application/x-www-form-urlencoded");

    EXPECT_TRUE(payload.has_value());
    iobuf_parser parser{std::move(payload.value())};
    auto received = parser.read_string(parser.bytes_left());

    EXPECT_TRUE(query_params_equal(
      absl::flat_hash_map<ss::sstring, ss::sstring>{
        {"grant_type", "client_credentials"},
        {"scope", "PRINCIPAL_ROLE%3aALL"},
        {"client_secret", get_credentials().client_secret},
        {"client_id", get_credentials().client_id}},
      received));

    co_return http::downloaded_response{
      .status = boost::beast::http::status::ok,
      .body = iobuf::from(
        R"J({"access_token": "token","token_type": "bearer", "expires_in": 1})J")};
}

static constexpr std::string_view table_metadata = R"J(
{
"metadata":{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/foo/bar/baz",
    "last-sequence-number": 123,
    "last-updated-ms": 11231231,
    "last-column-id": 2,
    "current-schema-id": 1,
    "schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"name","required":true,"type":"string"},{"id":2,"name":"age","required":true,"type":"int"}]}],
    "default-spec-id": 1,
    "partition-specs": [{"spec-id":1,"fields":[{"source-id":2,"field-id":1000,"name":"partition_by_age","transform":"identity"}]}],
    "last-partition-id": 1,
    "default-sort-order-id": 3,
    "sort-orders":[{"order-id":3,"fields":[{"transform":"identity","source-ids":[2],"direction":"asc","null-order":"nulls-first"}]}],
    "properties":{"read.split.target.size":"134217728"},
    "current-snapshot-id": 200,
    "snapshots": [{"snapshot-id": 200, "sequence-number": 0, "timestamp-ms": 0, "manifest-list": "s3://bucket/foo/bar/baz/ml.avro", "schema-id": 1, "summary": {"operation": "append"}}]
  }
}
)J";

chunked_vector<iceberg::schema> create_test_schemas() {
    iceberg::struct_type schema_struct;

    schema_struct.fields.push_back(iceberg::nested_field::create(
      iceberg::nested_field::id_t(1),
      "name",
      iceberg::field_required::yes,
      iceberg::string_type{}));
    schema_struct.fields.push_back(iceberg::nested_field::create(
      iceberg::nested_field::id_t(2),
      "age",
      iceberg::field_required::yes,
      iceberg::int_type{}));
    chunked_vector<iceberg::schema> ret;
    ret.push_back(iceberg::schema{
      .schema_struct = std::move(schema_struct),
      .schema_id = iceberg::schema::id_t{1},
    });
    return ret;
}

chunked_vector<iceberg::partition_spec> create_test_partition_spec() {
    chunked_vector<iceberg::partition_spec> ret;
    ret.push_back(iceberg::partition_spec{
      .spec_id = iceberg::partition_spec::id_t{1},
      .fields = {iceberg::partition_field{
        .source_id = iceberg::nested_field::id_t{2},
        .field_id = iceberg::partition_field::id_t{1000},
        .name = "partition_by_age",
        .transform = iceberg::identity_transform{},
      }}});
    return ret;
}

chunked_vector<iceberg::sort_order> create_sort_orders() {
    chunked_vector<iceberg::sort_order> ret;
    iceberg::sort_order so{
      .order_id = iceberg::sort_order::id_t{3},
      .fields = {},
    };
    so.fields.push_back(iceberg::sort_field{
      .transform = iceberg::identity_transform{},
      .source_ids = {iceberg::nested_field::id_t{2}},
      .direction = iceberg::sort_direction::asc,
      .null_order = iceberg::null_order::nulls_first,
    });
    ret.push_back(std::move(so));

    return ret;
}

iceberg::table_metadata create_table_metadata() {
    return {
      .format_version = iceberg::format_version::v2,
      .table_uuid = uuid_t::from_string("9c12d441-03fe-4693-9a96-a0705ddf69c1"),
      .location = iceberg::uri("s3://bucket/foo/bar/baz"),
      .last_sequence_number = iceberg::sequence_number(123),
      .last_updated_ms = model::timestamp(11231231),
      .last_column_id = iceberg::nested_field::id_t(2),
      .schemas = create_test_schemas(),
      .current_schema_id = iceberg::schema::id_t{1},
      .partition_specs = create_test_partition_spec(),
      .default_spec_id = iceberg::partition_spec::id_t{1},
      .last_partition_id = iceberg::partition_field::id_t{1},
      .properties = chunked_hash_map<
        ss::sstring,
        ss::sstring>{{"read.split.target.size", "134217728"}},
      .current_snapshot_id = iceberg::snapshot_id(200),
      .snapshots = chunked_vector<iceberg::snapshot>{iceberg::snapshot{
        .id = iceberg::snapshot_id{200},
        .sequence_number = iceberg::sequence_number(0),
        .timestamp_ms = model::timestamp(0),
        .summary = iceberg::
          snapshot_summary{.operation = iceberg::snapshot_operation::append},
        .manifest_list_path = iceberg::uri("s3://bucket/foo/bar/baz/ml.avro"),
        .schema_id = iceberg::schema::id_t{1},
      }},
      .sort_orders = create_sort_orders(),
      .default_sort_order_id = iceberg::sort_order::id_t{3}};
}

template<typename T>
iobuf as_json(const T& payload) {
    json::chunked_buffer buf;
    iceberg::json_writer writer(buf);
    rjson_serialize(writer, payload);

    return std::move(buf).as_iobuf();
}

ss::future<http::downloaded_response> handle_load_table(
  boost::beast::http::request_header<>&& r,
  std::optional<iobuf>,
  [[maybe_unused]] ss::lowres_clock::duration timeout) {
    EXPECT_EQ(r.at(boost::beast::http::field::host), "localhost:8181");

    co_return http::downloaded_response{
      .status = boost::beast::http::status::ok,
      .body = iobuf::from(table_metadata)};
}

void setup_token_request_expectations(client_mock& mock) {
    EXPECT_CALL(
      mock,
      request_and_collect_response(
        AllOf(
          Property(
            &boost::beast::http::request_header<>::target, EndsWith("/tokens")),
          Property(
            &boost::beast::http::request_header<>::method,
            Eq(boost::beast::http::verb::post))),
        _,
        _))
      .WillOnce(handle_token_request);
}
} // namespace

TEST_F(RestCatalogTest, CheckLoadTableHappyPath) {
    auto client = make_catalog_client({[](client_mock& m) {
        setup_token_request_expectations(m);

        EXPECT_CALL(
          m,
          request_and_collect_response(
            AllOf(
              Property(
                &boost::beast::http::request_header<>::target,
                EndsWith("/foo%1Fbar%1Fbaz/tables/panda_table")),
              Property(
                &boost::beast::http::request_header<>::method,
                Eq(boost::beast::http::verb::get))),
            _,
            _))
          .WillOnce(handle_load_table);
    }});

    iceberg::rest_catalog catalog(
      std::move(client), config::mock_binding<std::chrono::milliseconds>(10s));

    auto metadata = catalog
                      .load_table(iceberg::table_identifier{
                        .ns = {"foo", "bar", "baz"}, .table = "panda_table"})
                      .get();

    ASSERT_TRUE(metadata.has_value());
    ASSERT_EQ(metadata.value(), create_table_metadata());
}

ss::future<http::downloaded_response> handle_create_table(
  boost::beast::http::request_header<>&&,
  std::optional<iobuf>,
  [[maybe_unused]] ss::lowres_clock::duration timeout) {
    co_return http::downloaded_response{
      .status = boost::beast::http::status::ok,
      .body = iobuf::from(table_metadata)};
}

TEST_F(RestCatalogTest, CheckCreateTableHappyPath) {
    auto client = make_catalog_client({[](client_mock& m) {
        setup_token_request_expectations(m);

        EXPECT_CALL(
          m,
          request_and_collect_response(
            AllOf(
              Property(
                &boost::beast::http::request_header<>::target,
                EndsWith("/foo%1Fbar%1Fbaz/tables")),
              Property(
                &boost::beast::http::request_header<>::method,
                Eq(boost::beast::http::verb::post))),
            _,
            _))
          .WillOnce(handle_create_table);
    }});

    iceberg::rest_catalog catalog(
      std::move(client), config::mock_binding<std::chrono::milliseconds>(10s));

    auto metadata = catalog
                      .create_table(
                        iceberg::table_identifier{
                          .ns = {"foo", "bar", "baz"}, .table = "panda_table"},
                        create_test_schemas()[0],
                        create_test_partition_spec()[0])
                      .get();

    ASSERT_TRUE(metadata.has_value());
    ASSERT_EQ(
      iceberg::to_json_str(metadata.value()),
      iceberg::to_json_str(create_table_metadata()));
}

static constexpr auto commit_table_resp = R"J(
{
  "metadata-location": "s3://test/location/foo/bar/baz",
  "metadata": {
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "some-location",
    "last-sequence-number": 123,
    "last-updated-ms": 11231231,
    "last-column-id": 2,
    "current-schema-id": 1,
    "schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"name","required":true,"type":"string"},{"id":2,"name":"age","required":true,"type":"int"}]}],
    "default-spec-id": 1,
    "partition-specs": [{"spec-id":1,"fields":[{"source-id":2,"field-id":1000,"name":"partition_by_age","transform":"identity"}]}],
    "last-partition-id": 1,
    "default-sort-order-id": 3,
    "sort-orders":[{"order-id":3,"fields":[{"transform":"identity","source-ids":[2],"direction":"asc","null-order":"nulls-first"}]}],
    "properties":{"read.split.target.size":"134217728"},
    "current-snapshot-id": 200,
    "snapshots": []
  }
} 
)J";

ss::future<http::downloaded_response> handle_commit_table_txn(
  boost::beast::http::request_header<>&&,
  std::optional<iobuf> payload,
  [[maybe_unused]] ss::lowres_clock::duration timeout) {
    EXPECT_TRUE(payload.has_value());
    iobuf_parser parser(std::move(*payload));
    auto json_str = parser.read_string(parser.bytes_left());
    json::Document doc;
    doc.Parse(json_str);
    // validate that the request is a valid json
    EXPECT_FALSE(doc.HasParseError());

    co_return http::downloaded_response{
      .status = boost::beast::http::status::ok,
      .body = iobuf::from(commit_table_resp),
    };
}

iceberg::table_metadata create_empty_table_metadata(const ss::sstring& bucket) {
    return {
      .format_version = iceberg::format_version::v2,
      .table_uuid = uuid_t::from_string("9c12d441-03fe-4693-9a96-a0705ddf69c1"),
      .location = iceberg::uri(fmt::format("s3://{}/foo_table", bucket)),
      .last_sequence_number = iceberg::sequence_number(0),
      .last_updated_ms = model::timestamp(11231231),
      .last_column_id = iceberg::nested_field::id_t(2),
      .schemas = create_test_schemas(),
      .current_schema_id = iceberg::schema::id_t{1},
      .partition_specs = create_test_partition_spec(),
      .default_spec_id = iceberg::partition_spec::id_t{1},
      .last_partition_id = iceberg::partition_field::id_t{1},
      .properties = chunked_hash_map<
        ss::sstring,
        ss::sstring>{{"read.split.target.size", "134217728"}},
      .current_snapshot_id = std::nullopt,
      .snapshots = chunked_vector<iceberg::snapshot>{},
      .sort_orders = create_sort_orders(),
      .default_sort_order_id = iceberg::sort_order::id_t{3}};
}

TEST_F(RestCatalogTest, CommitTxnHappyPath) {
    auto client = make_catalog_client({[](client_mock& m) {
        setup_token_request_expectations(m);

        EXPECT_CALL(
          m,
          request_and_collect_response(
            AllOf(
              Property(
                &boost::beast::http::request_header<>::target,
                EndsWith("/foo%1Fbar%1Fbaz/tables/panda_table")),
              Property(
                &boost::beast::http::request_header<>::method,
                Eq(boost::beast::http::verb::post))),
            _,
            _))
          .WillOnce(handle_commit_table_txn);
    }});

    iceberg::rest_catalog catalog(
      std::move(client), config::mock_binding<std::chrono::milliseconds>(10s));
    auto table_md = create_empty_table_metadata(bucket_name);
    chunked_vector<iceberg::data_file> files;
    std::unique_ptr<iceberg::struct_value> partition_key_val
      = std::make_unique<iceberg::struct_value>();
    partition_key_val->fields.push_back(iceberg::int_value{0});

    files.push_back(iceberg::data_file{
      .content_type = iceberg::data_file_content_type::data,
      .file_format = iceberg::data_file_format::parquet,
      .partition = iceberg::partition_key{.val = std::move(partition_key_val)},
    });

    iceberg::transaction txn(std::move(table_md));

    auto outcome = txn.merge_append(io, std::move(files)).get();
    ASSERT_FALSE(outcome.has_error());
    auto result = catalog
                    .commit_txn(
                      iceberg::table_identifier{
                        .ns = {"foo", "bar", "baz"}, .table = "panda_table"},
                      std::move(txn))
                    .get();
    ASSERT_FALSE(result.has_error());
}

ss::future<http::downloaded_response> handle_load_table_check_concurrency(
  boost::beast::http::request_header<>&& r,
  std::optional<iobuf>,
  [[maybe_unused]] ss::lowres_clock::duration timeout) {
    static thread_local mutex m("test/rest_catalog");

    // the mutex must always be ready as there is currently only one inflight
    // request
    EXPECT_TRUE(m.ready());
    auto u = co_await m.get_units();
    EXPECT_EQ(r.at(boost::beast::http::field::host), "localhost:8181");
    // sleep to simulate long running request
    co_await ss::sleep(10ms);

    co_return http::downloaded_response{
      .status = boost::beast::http::status::ok,
      .body = iobuf::from(table_metadata)};
}

TEST_F(RestCatalogTest, TestConcurrentAccesses) {
    auto client = make_catalog_client({[](client_mock& m) {
        setup_token_request_expectations(m);
        // setup mock to always reply in a
        EXPECT_CALL(
          m,
          request_and_collect_response(
            AllOf(
              Property(
                &boost::beast::http::request_header<>::target,
                EndsWith("/foo%1Fbar%1Fbaz/tables/panda_table")),
              Property(
                &boost::beast::http::request_header<>::method,
                Eq(boost::beast::http::verb::get))),
            _,
            _))
          .WillRepeatedly(handle_load_table_check_concurrency);
    }});

    iceberg::rest_catalog catalog(
      std::move(client), config::mock_binding<std::chrono::milliseconds>(10s));
    auto t_id = iceberg::table_identifier{
      .ns = {"foo", "bar", "baz"}, .table = "panda_table"};
    ss::parallel_for_each(boost::irange(20), [&](int) {
        return catalog.load_table(t_id).discard_result();
    }).get();
}
