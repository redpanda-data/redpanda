#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage_clients/util.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_all_paths_to_file) {
    using namespace cloud_storage_clients;

    auto result1 = util::all_paths_to_file(object_key{"a/b/c/log.txt"});
    auto expected1 = std::vector<object_key>{
      object_key{"a"},
      object_key{"a/b"},
      object_key{"a/b/c"},
      object_key{"a/b/c/log.txt"}};
    BOOST_REQUIRE_EQUAL(result1, expected1);

    auto result2 = util::all_paths_to_file(object_key{"a/b/c/"});
    BOOST_REQUIRE_EQUAL(result2, std::vector<object_key>{});

    auto result3 = util::all_paths_to_file(object_key{""});
    BOOST_REQUIRE_EQUAL(result3, std::vector<object_key>{});

    auto result4 = util::all_paths_to_file(object_key{"foo"});
    BOOST_REQUIRE_EQUAL(result4, std::vector<object_key>{object_key{"foo"}});
}

// ============================================================================
// mime_header tests
// ============================================================================

namespace {
constexpr auto convert_cid = [](std::string_view raw) {
    return std::make_optional<int>(std::stoi(std::string{raw}));
};
} // namespace

BOOST_AUTO_TEST_CASE(test_mime_header_parse_basic) {
    using namespace cloud_storage_clients;

    const char* mime_data = "Content-Type: application/http\r\n"
                            "Content-ID: 42\r\n"
                            "\r\n";

    iobuf buf;
    buf.append(mime_data, strlen(mime_data));
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    // Check Content-Type
    auto content_type = header.get(boost::beast::http::field::content_type);
    BOOST_REQUIRE(content_type.has_value());
    BOOST_REQUIRE_EQUAL(content_type.value(), "application/http");

    // Check Content-ID
    auto content_id = header.content_id<int>(convert_cid);
    BOOST_REQUIRE(content_id.has_value());
    BOOST_REQUIRE_EQUAL(content_id.value(), 42);
}

BOOST_AUTO_TEST_CASE(test_mime_header_parse_only_content_type) {
    using namespace cloud_storage_clients;

    const char* mime_data = "Content-Type: text/plain\r\n"
                            "\r\n";

    iobuf buf;
    buf.append(mime_data, strlen(mime_data));
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    auto content_type = header.get(boost::beast::http::field::content_type);
    BOOST_REQUIRE(content_type.has_value());
    BOOST_REQUIRE_EQUAL(content_type.value(), "text/plain");

    // Content-ID should be absent
    auto content_id = header.content_id<int>(convert_cid);
    BOOST_REQUIRE(!content_id.has_value());
}

BOOST_AUTO_TEST_CASE(test_mime_header_parse_only_content_id) {
    using namespace cloud_storage_clients;

    const char* mime_data = "Content-ID: 0\r\n"
                            "\r\n";

    iobuf buf;
    buf.append(mime_data, strlen(mime_data));
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    auto content_id = header.content_id<int>(convert_cid);
    BOOST_REQUIRE(content_id.has_value());
    BOOST_REQUIRE_EQUAL(content_id.value(), 0);

    // Content-Type should be absent
    auto content_type = header.get(boost::beast::http::field::content_type);
    BOOST_REQUIRE(!content_type.has_value());
}

BOOST_AUTO_TEST_CASE(test_mime_header_parse_empty) {
    using namespace cloud_storage_clients;

    const char* mime_data = "\r\n";

    iobuf buf;
    buf.append(mime_data, strlen(mime_data));
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    // Both fields should be absent
    auto content_type = header.get(boost::beast::http::field::content_type);
    BOOST_REQUIRE(!content_type.has_value());

    auto content_id = header.content_id<int>(convert_cid);
    BOOST_REQUIRE(!content_id.has_value());
}

BOOST_AUTO_TEST_CASE(test_mime_header_parse_extra_headers) {
    using namespace cloud_storage_clients;

    const char* mime_data = "Content-Type: application/http\r\n"
                            "Content-ID: 5\r\n"
                            "X-Custom-Header: custom-value\r\n"
                            "\r\n";

    iobuf buf;
    buf.append(mime_data, strlen(mime_data));
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    // Standard fields should still work
    auto content_type = header.get(boost::beast::http::field::content_type);
    BOOST_REQUIRE(content_type.has_value());
    BOOST_REQUIRE_EQUAL(content_type.value(), "application/http");

    auto content_id = header.content_id<int>(convert_cid);
    BOOST_REQUIRE(content_id.has_value());
    BOOST_REQUIRE_EQUAL(content_id.value(), 5);
}

// ============================================================================
// multipart_response_parser tests
// ============================================================================

BOOST_AUTO_TEST_CASE(test_multipart_parser_single_part) {
    using namespace cloud_storage_clients;

    const char* multipart_data = "--boundary\r\n"
                                 "Content-Type: text/plain\r\n"
                                 "\r\n"
                                 "Hello World\r\n"
                                 "--boundary--\r\n";

    iobuf buf;
    buf.append(multipart_data, strlen(multipart_data));

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Get first part
    auto part1 = parser.get_part();
    BOOST_REQUIRE(part1.has_value());

    // Verify part contains the expected data
    iobuf_parser part_parser(std::move(part1.value()));
    auto content = part_parser.read_string(part_parser.bytes_left());
    BOOST_REQUIRE(
      content.find("Content-Type: text/plain") != ss::sstring::npos);
    BOOST_REQUIRE(content.find("Hello World") != ss::sstring::npos);

    // Should be no more parts
    auto part2 = parser.get_part();
    BOOST_REQUIRE(!part2.has_value());
}

BOOST_AUTO_TEST_CASE(test_multipart_parser_multiple_parts) {
    using namespace cloud_storage_clients;

    const char* multipart_data = "--boundary\r\n"
                                 "Content-ID: 0\r\n"
                                 "\r\n"
                                 "First part\r\n"
                                 "--boundary\r\n"
                                 "Content-ID: 1\r\n"
                                 "\r\n"
                                 "Second part\r\n"
                                 "--boundary\r\n"
                                 "Content-ID: 2\r\n"
                                 "\r\n"
                                 "Third part\r\n"
                                 "--boundary--\r\n";

    iobuf buf;
    buf.append(multipart_data, strlen(multipart_data));

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Get all three parts
    auto part1 = parser.get_part();
    BOOST_REQUIRE(part1.has_value());

    auto part2 = parser.get_part();
    BOOST_REQUIRE(part2.has_value());

    auto part3 = parser.get_part();
    BOOST_REQUIRE(part3.has_value());

    // Verify content
    iobuf_parser p1(std::move(part1.value()));
    auto content1 = p1.read_string(p1.bytes_left());
    BOOST_REQUIRE(content1.find("First part") != ss::sstring::npos);

    iobuf_parser p2(std::move(part2.value()));
    auto content2 = p2.read_string(p2.bytes_left());
    BOOST_REQUIRE(content2.find("Second part") != ss::sstring::npos);

    iobuf_parser p3(std::move(part3.value()));
    auto content3 = p3.read_string(p3.bytes_left());
    BOOST_REQUIRE(content3.find("Third part") != ss::sstring::npos);

    // Should be no more parts
    auto part4 = parser.get_part();
    BOOST_REQUIRE(!part4.has_value());
}

BOOST_AUTO_TEST_CASE(test_multipart_parser_empty_parts) {
    using namespace cloud_storage_clients;

    const char* multipart_data = "--boundary\r\n"
                                 "\r\n"
                                 "--boundary\r\n"
                                 "\r\n"
                                 "--boundary--\r\n";

    iobuf buf;
    buf.append(multipart_data, strlen(multipart_data));

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Even with empty parts, parser should handle gracefully
    auto part1 = parser.get_part();
    BOOST_REQUIRE(!part1.has_value());

    auto part2 = parser.get_part();
    BOOST_REQUIRE(!part2.has_value());

    BOOST_REQUIRE(!parser.get_part().has_value());
}

BOOST_AUTO_TEST_CASE(test_multipart_parser_no_end_boundary) {
    using namespace cloud_storage_clients;

    const char* multipart_data = "--boundary\r\n"
                                 "Content-ID: 0\r\n"
                                 "\r\n"
                                 "Data without end"
                                 "--bound";

    iobuf buf;
    buf.append(multipart_data, strlen(multipart_data));

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Should handle missing/incomplete end boundary gracefully
    auto part1 = parser.get_part();
    BOOST_REQUIRE(!part1.has_value());
}

BOOST_AUTO_TEST_CASE(test_multipart_parser_empty_buffer) {
    using namespace cloud_storage_clients;

    iobuf buf;
    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Empty buffer should return no parts
    auto part = parser.get_part();
    BOOST_REQUIRE(!part.has_value());
}

// ============================================================================
// multipart_subresponse tests
// ============================================================================

BOOST_AUTO_TEST_CASE(test_multipart_subresponse_parse_success) {
    using namespace cloud_storage_clients;

    const char* http_response = "HTTP/1.1 202 Accepted\r\n"
                                "x-ms-request-id: abc-123\r\n"
                                "x-ms-version: 2023-01-03\r\n"
                                "Content-Length: 0\r\n"
                                "\r\n";

    iobuf buf;
    buf.append(http_response, strlen(http_response));
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    // Check status
    BOOST_REQUIRE_EQUAL(
      subresponse.result(), boost::beast::http::status::accepted);
    BOOST_REQUIRE(subresponse.is_ok());

    // Should have no error
    auto error = subresponse.error("x-ms-error-code");
    BOOST_REQUIRE(!error.has_value());
}

BOOST_AUTO_TEST_CASE(test_multipart_subresponse_parse_ok) {
    using namespace cloud_storage_clients;

    const char* http_response = "HTTP/1.1 200 OK\r\n"
                                "Content-Type: application/json\r\n"
                                "Content-Length: 0\r\n"
                                "\r\n";

    iobuf buf;
    buf.append(http_response, strlen(http_response));
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    BOOST_REQUIRE_EQUAL(subresponse.result(), boost::beast::http::status::ok);
    BOOST_REQUIRE(subresponse.is_ok());
}

BOOST_AUTO_TEST_CASE(test_multipart_subresponse_parse_not_found) {
    using namespace cloud_storage_clients;

    const char* http_response = "HTTP/1.1 404 Not Found\r\n"
                                "x-ms-error-code: BlobNotFound\r\n"
                                "Content-Length: 0\r\n"
                                "\r\n";

    iobuf buf;
    buf.append(http_response, strlen(http_response));
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    BOOST_REQUIRE_EQUAL(
      subresponse.result(), boost::beast::http::status::not_found);
    // 404 is considered "ok" for delete operations
    BOOST_REQUIRE(subresponse.is_ok());

    // Even though 404 is "ok", error() should return nullopt
    auto error = subresponse.error("x-ms-error-code");
    BOOST_REQUIRE(!error.has_value());
}

BOOST_AUTO_TEST_CASE(test_multipart_subresponse_parse_error) {
    using namespace cloud_storage_clients;

    const char* http_response = "HTTP/1.1 403 Forbidden\r\n"
                                "x-ms-error-code: AuthenticationFailed\r\n"
                                "Content-Length: 0\r\n"
                                "\r\n";

    iobuf buf;
    buf.append(http_response, strlen(http_response));
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    BOOST_REQUIRE_EQUAL(
      subresponse.result(), boost::beast::http::status::forbidden);
    BOOST_REQUIRE(!subresponse.is_ok());

    // Should extract error message
    auto error = subresponse.error("x-ms-error-code");
    BOOST_REQUIRE(error.has_value());
    BOOST_REQUIRE(error.value().find("403") != ss::sstring::npos);
    BOOST_REQUIRE(
      error.value().find("AuthenticationFailed") != ss::sstring::npos);
}

BOOST_AUTO_TEST_CASE(test_multipart_subresponse_parse_error_no_code) {
    using namespace cloud_storage_clients;

    const char* http_response = "HTTP/1.1 500 Internal Server Error\r\n"
                                "Content-Length: 0\r\n"
                                "\r\n";

    iobuf buf;
    buf.append(http_response, strlen(http_response));
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    BOOST_REQUIRE_EQUAL(
      subresponse.result(), boost::beast::http::status::internal_server_error);
    BOOST_REQUIRE(!subresponse.is_ok());

    // Error should still be returned with "Unknown" reason
    auto error = subresponse.error("x-ms-error-code");
    BOOST_REQUIRE(error.has_value());
    BOOST_REQUIRE(error.value().find("500") != ss::sstring::npos);
    BOOST_REQUIRE(error.value().find("Unknown") != ss::sstring::npos);
}

BOOST_AUTO_TEST_CASE(test_multipart_subresponse_parse_no_content) {
    using namespace cloud_storage_clients;

    const char* http_response = "HTTP/1.1 204 No Content\r\n"
                                "\r\n";

    iobuf buf;
    buf.append(http_response, strlen(http_response));
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    BOOST_REQUIRE_EQUAL(
      subresponse.result(), boost::beast::http::status::no_content);
    BOOST_REQUIRE(subresponse.is_ok());
}

// ============================================================================
// Integration tests - full multipart response parsing
// ============================================================================

BOOST_AUTO_TEST_CASE(test_full_multipart_parsing_success) {
    using namespace cloud_storage_clients;

    // Simulate Azure Batch API response with multiple successful deletes
    const char* batch_response = "--batch_boundary\r\n"
                                 "Content-Type: application/http\r\n"
                                 "Content-ID: 0\r\n"
                                 "\r\n"
                                 "HTTP/1.1 202 Accepted\r\n"
                                 "x-ms-request-id: req-0\r\n"
                                 "\r\n"
                                 "--batch_boundary\r\n"
                                 "Content-Type: application/http\r\n"
                                 "Content-ID: 1\r\n"
                                 "\r\n"
                                 "HTTP/1.1 202 Accepted\r\n"
                                 "x-ms-request-id: req-1\r\n"
                                 "\r\n"
                                 "--batch_boundary--\r\n";

    iobuf buf;
    buf.append(batch_response, strlen(batch_response));

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--batch_boundary"));

    int successful_parts = 0;

    std::optional<iobuf> part;
    while ((part = parser.get_part()).has_value()) {
        iobuf_parser part_parser(std::move(part).value());

        // Parse MIME headers
        auto mime = util::mime_header::from(part_parser);
        auto content_id = mime.content_id<int>(convert_cid);
        BOOST_REQUIRE(content_id.has_value());

        // Parse HTTP response
        auto subresponse = util::multipart_subresponse::from(part_parser);
        BOOST_REQUIRE(subresponse.is_ok());
        BOOST_REQUIRE_EQUAL(
          subresponse.result(), boost::beast::http::status::accepted);

        successful_parts++;
    }

    BOOST_REQUIRE_EQUAL(successful_parts, 2);
}

BOOST_AUTO_TEST_CASE(test_full_multipart_parsing_with_errors) {
    using namespace cloud_storage_clients;

    // Simulate response with mixed success and errors
    const char* batch_response = "--batch_boundary\r\n"
                                 "Content-Type: application/http\r\n"
                                 "Content-ID: 0\r\n"
                                 "\r\n"
                                 "HTTP/1.1 202 Accepted\r\n"
                                 "x-ms-request-id: req-0\r\n"
                                 "\r\n"
                                 "--batch_boundary\r\n"
                                 "Content-Type: application/http\r\n"
                                 "Content-ID: 1\r\n"
                                 "\r\n"
                                 "HTTP/1.1 403 Forbidden\r\n"
                                 "x-ms-error-code: InvalidCredentials\r\n"
                                 "\r\n"
                                 "--batch_boundary\r\n"
                                 "Content-Type: application/http\r\n"
                                 "Content-ID: 2\r\n"
                                 "\r\n"
                                 "HTTP/1.1 404 Not Found\r\n"
                                 "x-ms-error-code: BlobNotFound\r\n"
                                 "\r\n"
                                 "--batch_boundary--\r\n";

    iobuf buf;
    buf.append(batch_response, strlen(batch_response));

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--batch_boundary"));

    std::vector<bool> is_ok_results;
    std::vector<std::optional<size_t>> content_ids;

    std::optional<iobuf> part;
    while ((part = parser.get_part()).has_value()) {
        iobuf_parser part_parser(std::move(part).value());

        auto mime = util::mime_header::from(part_parser);
        content_ids.push_back(mime.content_id<int>(convert_cid));

        auto subresponse = util::multipart_subresponse::from(part_parser);
        is_ok_results.push_back(subresponse.is_ok());
    }

    BOOST_REQUIRE_EQUAL(is_ok_results.size(), 3);
    BOOST_REQUIRE_EQUAL(content_ids.size(), 3);

    // First one should be successful
    BOOST_REQUIRE_EQUAL(content_ids[0].value(), 0);
    BOOST_REQUIRE(is_ok_results[0]);

    // Second one should be error
    BOOST_REQUIRE_EQUAL(content_ids[1].value(), 1);
    BOOST_REQUIRE(!is_ok_results[1]);

    // Third one (404) should be ok for deletes
    BOOST_REQUIRE_EQUAL(content_ids[2].value(), 2);
    BOOST_REQUIRE(is_ok_results[2]);
}
