/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/recovery_request.h"

#include <seastar/http/request.hh>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(recovery_request_invalid_json) {
    ss::http::request r;
    r.content = "-";
    r.content_length = 1;
    r._headers = {{"Content-Type", "application/json"}};
    BOOST_REQUIRE_EXCEPTION(
      cloud_storage::recovery_request{r},
      cloud_storage::bad_request,
      [](const auto& ex) {
          return std::string_view{ex.what()} == "Invalid value.";
      });
}

BOOST_AUTO_TEST_CASE(recovery_request_missing_fields) {
    ss::http::request r;
    r.content = "{}";
    r.content_length = 1;
    r._headers = {{"Content-Type", "application/json"}};
    BOOST_REQUIRE_EXCEPTION(
      cloud_storage::recovery_request{r},
      cloud_storage::bad_request,
      [](const auto& ex) {
          const std::string_view s{ex.what()};
          return s.find("missing") != std::string_view::npos
                 && s.find("topic_names_pattern") != std::string_view::npos;
      });
}

BOOST_AUTO_TEST_CASE(recovery_request_missing_content_type) {
    ss::http::request r;
    r.content = "{}";
    r.content_length = 1;
    BOOST_REQUIRE_EXCEPTION(
      cloud_storage::recovery_request{r},
      cloud_storage::bad_request,
      [](const auto& ex) {
          return std::string_view{ex.what()} == "missing content type";
      });
}

BOOST_AUTO_TEST_CASE(recovery_request_pattern) {
    ss::http::request r;
    r.content = R"JSON({"topic_names_pattern": "asdf"})JSON";
    r.content_length = 1;
    r._headers = {{"Content-Type", "application/json"}};
    cloud_storage::recovery_request rec_req{r};
    BOOST_REQUIRE_EQUAL(rec_req.topic_names_pattern().value(), "asdf");
}

BOOST_AUTO_TEST_CASE(recovery_request_pattern_and_bytes) {
    ss::http::request r;
    r.content
      = R"JSON({"topic_names_pattern": "asdf", "retention_bytes": 1})JSON";
    r.content_length = 1;
    r._headers = {{"Content-Type", "application/json"}};
    cloud_storage::recovery_request rec_req{r};
    BOOST_REQUIRE_EQUAL(rec_req.topic_names_pattern().value(), "asdf");
    BOOST_REQUIRE_EQUAL(rec_req.retention_bytes().value(), 1);
}

BOOST_AUTO_TEST_CASE(recovery_request_pattern_and_ms) {
    ss::http::request r;
    r.content = R"JSON({"topic_names_pattern": "asdf", "retention_ms": 1})JSON";
    r.content_length = 1;
    r._headers = {{"Content-Type", "application/json"}};
    cloud_storage::recovery_request rec_req{r};
    BOOST_REQUIRE_EQUAL(rec_req.topic_names_pattern().value(), "asdf");
    BOOST_REQUIRE_EQUAL(rec_req.retention_ms().value().count(), 1);
}

BOOST_AUTO_TEST_CASE(recovery_request_invalid_combination) {
    ss::http::request r;
    r.content
      = R"JSON({"topic_names_pattern": "asdf", "retention_ms": 1, "retention_bytes": 1})JSON";
    r.content_length = 1;
    r._headers = {{"Content-Type", "application/json"}};
    BOOST_REQUIRE_EXCEPTION(
      cloud_storage::recovery_request rec_req{r},
      cloud_storage::bad_request,
      [](const auto& ex) {
          const std::string_view s{ex.what()};
          return s.find("not") != std::string_view::npos;
      });
}
