#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage_clients/util.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(Util, AllPathsToFile) {
    using namespace cloud_storage_clients;

    auto result1 = util::all_paths_to_file(object_key{"a/b/c/log.txt"});
    auto expected1 = std::vector<object_key>{
      object_key{"a"},
      object_key{"a/b"},
      object_key{"a/b/c"},
      object_key{"a/b/c/log.txt"}};
    EXPECT_EQ(result1, expected1);

    auto result2 = util::all_paths_to_file(object_key{"a/b/c/"});
    EXPECT_EQ(result2, std::vector<object_key>{});

    auto result3 = util::all_paths_to_file(object_key{""});
    EXPECT_EQ(result3, std::vector<object_key>{});

    auto result4 = util::all_paths_to_file(object_key{"foo"});
    EXPECT_EQ(result4, std::vector<object_key>{object_key{"foo"}});
}

// ============================================================================
// mime_header tests
// ============================================================================

namespace {
constexpr auto convert_cid = [](std::string_view raw) {
    return std::make_optional<int>(std::stoi(std::string{raw}));
};
} // namespace

TEST(MimeHeader, ParseBasic) {
    using namespace cloud_storage_clients;

    std::string_view mime_data = "Content-Type: application/http\r\n"
                                 "Content-ID: 42\r\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    // Check Content-Type
    auto content_type = header.get(boost::beast::http::field::content_type);
    EXPECT_TRUE(content_type.has_value());
    EXPECT_EQ(content_type.value(), "application/http");

    // Check Content-ID
    auto content_id = header.content_id<int>(convert_cid);
    EXPECT_TRUE(content_id.has_value());
    EXPECT_EQ(content_id.value(), 42);

    // Nonexistent header returns nullopt
    auto xfer_encoding = header.get(
      boost::beast::http::field::content_transfer_encoding);
    EXPECT_FALSE(xfer_encoding.has_value());
}

TEST(MimeHeader, ParseNoContentId) {
    using namespace cloud_storage_clients;

    std::string_view mime_data = "Content-Type: text/plain\r\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    // Content-ID should be absent, but nothing blows up
    auto content_id = header.content_id<int>(convert_cid);
    EXPECT_FALSE(content_id.has_value());
}

TEST(MimeHeader, ParseExtraHeaders) {
    using namespace cloud_storage_clients;

    std::string_view mime_data = "Content-Type: application/http\r\n"
                                 "Content-ID: 5\r\n"
                                 "X-Custom-Header: custom-value\r\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    // Standard fields should still work
    auto content_type = header.get(boost::beast::http::field::content_type);
    EXPECT_TRUE(content_type.has_value());
    EXPECT_EQ(content_type.value(), "application/http");

    auto content_id = header.content_id<int>(convert_cid);
    EXPECT_TRUE(content_id.has_value());
    EXPECT_EQ(content_id.value(), 5);
}

TEST(MimeHeader, MixedCaseName) {
    using namespace cloud_storage_clients;
    std::string_view mime_data = "cOnTeNt-TyPe: application/http\r\n"
                                 "CONTENT-id: BIGTEXT-5\r\n"
                                 "\r\n";
    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);

    // fields should still be legible
    EXPECT_TRUE(
      header.get(boost::beast::http::field::content_type).has_value());
    EXPECT_TRUE(header.get(boost::beast::http::field::content_id).has_value());

    // but the case of the _value_ should be untouched
    EXPECT_EQ(
      header.get(boost::beast::http::field::content_id).value(), "BIGTEXT-5");
}

// ============================================================================
// multipart_response_parser tests
// ============================================================================

TEST(MultipartParser, SinglePart) {
    using namespace cloud_storage_clients;

    std::string_view multipart_data = "--boundary\r\n"
                                      "Content-Type: text/plain\r\n"
                                      "\r\n"
                                      "Hello World\r\n"
                                      "\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Get first part
    auto part1 = parser.get_part();
    EXPECT_TRUE(part1.has_value());

    auto content = part1.value().linearize_to_string();
    EXPECT_THAT(content, testing::HasSubstr("Content-Type: text/plain"));
    EXPECT_THAT(content, testing::HasSubstr("Hello World"));

    // Should be no more parts
    auto part2 = parser.get_part();
    EXPECT_FALSE(part2.has_value());
}

TEST(MultipartParser, EmptyBody) {
    using namespace cloud_storage_clients;

    std::string_view multipart_data = "--boundary\r\n"
                                      "Content-Type: application/json\r\n"
                                      "\r\n"
                                      "\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Get first part
    auto part1 = parser.get_part();
    EXPECT_TRUE(part1.has_value());

    auto content = part1.value().linearize_to_string();
    EXPECT_THAT(content, testing::HasSubstr("Content-Type: application/json"));

    // Should be no more parts
    auto part2 = parser.get_part();
    EXPECT_FALSE(part2.has_value());
}

TEST(MultipartParser, MultipleParts) {
    using namespace cloud_storage_clients;

    std::string_view multipart_data = "--boundary\r\n"
                                      "Content-ID: 0\r\n"
                                      "\r\n"
                                      "First part\r\n"
                                      "\r\n"
                                      "--boundary\r\n"
                                      "Content-ID: 1\r\n"
                                      "\r\n"
                                      "Second part\r\n"
                                      "\r\n"
                                      "--boundary\r\n"
                                      "Content-ID: 2\r\n"
                                      "\r\n"
                                      "Third part\r\n"
                                      "\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Get all three parts
    auto part1 = parser.get_part();
    EXPECT_TRUE(part1.has_value());

    auto part2 = parser.get_part();
    EXPECT_TRUE(part2.has_value());

    auto part3 = parser.get_part();
    EXPECT_TRUE(part3.has_value());

    // Verify content
    EXPECT_THAT(
      part1.value().linearize_to_string(), testing::HasSubstr("First part"));
    EXPECT_THAT(
      part2.value().linearize_to_string(), testing::HasSubstr("Second part"));
    EXPECT_THAT(
      part3.value().linearize_to_string(), testing::HasSubstr("Third part"));

    // Should be no more parts
    auto part4 = parser.get_part();
    EXPECT_FALSE(part4.has_value());
}

TEST(MultipartParser, EmptyParts) {
    using namespace cloud_storage_clients;

    std::string_view multipart_data = "--boundary\r\n"
                                      "\r\n"
                                      "--boundary\r\n"
                                      "\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Even with empty parts, parser should handle gracefully
    auto part1 = parser.get_part();
    EXPECT_FALSE(part1.has_value());

    auto part2 = parser.get_part();
    EXPECT_FALSE(part2.has_value());

    EXPECT_FALSE(parser.get_part().has_value());
}

TEST(MultipartParser, NoEndBoundary) {
    using namespace cloud_storage_clients;

    std::string_view multipart_data = "--boundary\r\n"
                                      "Content-ID: 0\r\n"
                                      "\r\n"
                                      "Data without end"
                                      "--bound";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Should handle missing/incomplete end boundary gracefully
    auto part1 = parser.get_part();
    EXPECT_FALSE(part1.has_value());
}

TEST(MultipartParser, EmptyBuffer) {
    using namespace cloud_storage_clients;

    iobuf buf;
    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Empty buffer should return no parts
    auto part = parser.get_part();
    EXPECT_FALSE(part.has_value());
}

// ============================================================================
// multipart_subresponse tests
// ============================================================================

TEST(MultipartSubresponse, ParseSuccess) {
    using namespace cloud_storage_clients;

    std::string_view http_response = "HTTP/1.1 202 Accepted\r\n"
                                     "x-ms-request-id: abc-123\r\n"
                                     "x-ms-version: 2023-01-03\r\n"
                                     "Content-Length: 0\r\n"
                                     "\r\n";

    auto buf = iobuf::from(http_response);
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    // Check status
    EXPECT_EQ(subresponse.result(), boost::beast::http::status::accepted);
    EXPECT_TRUE(subresponse.is_ok());

    // Should have no error
    auto error = subresponse.error("x-ms-error-code");
    EXPECT_FALSE(error.has_value());
    error = subresponse.error([](iobuf b) { return b.linearize_to_string(); });
    EXPECT_FALSE(error.has_value());
}

TEST(MultipartSubresponse, ParseNotFound) {
    using namespace cloud_storage_clients;

    std::string_view http_response = "HTTP/1.1 404 Not Found\r\n"
                                     "x-ms-error-code: BlobNotFound\r\n"
                                     "Content-Length: 0\r\n"
                                     "\r\n";

    auto buf = iobuf::from(http_response);
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    EXPECT_EQ(subresponse.result(), boost::beast::http::status::not_found);
    // 404 is considered "ok" for delete operations
    EXPECT_TRUE(subresponse.is_ok());

    // So error() should return nullopt, regardless of header contents
    auto error = subresponse.error("x-ms-error-code");
    EXPECT_FALSE(error.has_value());
}

TEST(MultipartSubresponse, ParseErrorHeader) {
    using namespace cloud_storage_clients;

    std::string_view http_response = "HTTP/1.1 403 Forbidden\r\n"
                                     "x-ms-error-code: AuthenticationFailed\r\n"
                                     "Content-Length: 0\r\n"
                                     "\r\n";

    auto buf = iobuf::from(http_response);
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    EXPECT_EQ(subresponse.result(), boost::beast::http::status::forbidden);
    EXPECT_FALSE(subresponse.is_ok());

    // Should extract error message
    auto error = subresponse.error("x-ms-error-code");
    EXPECT_TRUE(error.has_value());
    EXPECT_THAT(error.value(), testing::HasSubstr("403"));
    EXPECT_THAT(
      error.value(),
      testing::HasSubstr(fmt::format("{}", subresponse.result())));
    EXPECT_THAT(error.value(), testing::HasSubstr("AuthenticationFailed"));
}

TEST(MultipartSubresponse, ParseErrorNoMessage) {
    using namespace cloud_storage_clients;

    std::string_view http_response = "HTTP/1.1 500 Internal Server Error\r\n"
                                     "Content-Length: 0\r\n"
                                     "\r\n";

    auto buf = iobuf::from(http_response);
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    EXPECT_EQ(
      subresponse.result(), boost::beast::http::status::internal_server_error);
    EXPECT_FALSE(subresponse.is_ok());

    // Error should still be returned with "Unknown" reason
    auto error = subresponse.error("x-ms-error-code");
    EXPECT_TRUE(error.has_value());
    EXPECT_THAT(error.value(), testing::HasSubstr("500"));
    EXPECT_THAT(
      error.value(),
      testing::HasSubstr(fmt::format("{}", subresponse.result())));
    EXPECT_THAT(error.value(), testing::HasSubstr("Unknown"));
}

TEST(MultipartSubresponse, ParseErrorInBody) {
    using namespace cloud_storage_clients;

    std::string_view http_response = "HTTP/1.1 500 Internal Server Error\r\n"
                                     "Content-Length: 4\r\n"
                                     "\r\n"
                                     "OOPS\r\n"
                                     "\r\n";

    auto buf = iobuf::from(http_response);
    iobuf_parser parser(std::move(buf));

    auto subresponse = util::multipart_subresponse::from(parser);

    EXPECT_EQ(
      subresponse.result(), boost::beast::http::status::internal_server_error);
    EXPECT_FALSE(subresponse.is_ok());

    // Error should still be returned with "Unknown" reason
    auto error = subresponse.error(
      [](iobuf b) { return b.linearize_to_string(); });
    EXPECT_TRUE(error.has_value());
    EXPECT_THAT(error.value(), testing::HasSubstr("500"));
    EXPECT_THAT(
      error.value(),
      testing::HasSubstr(fmt::format("{}", subresponse.result())));
    EXPECT_THAT(error.value(), testing::HasSubstr("OOPS"));
}

// ============================================================================
// Integration tests - full multipart response parsing
// ============================================================================

TEST(FullMultipartParsing, Success) {
    using namespace cloud_storage_clients;

    // Simulate Azure Batch API response with multiple successful deletes
    std::string_view batch_response = "--batch_boundary\r\n"
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

    auto buf = iobuf::from(batch_response);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--batch_boundary"));

    int successful_parts = 0;

    std::optional<iobuf> part;
    while ((part = parser.get_part()).has_value()) {
        iobuf_parser part_parser(std::move(part).value());

        // Parse MIME headers
        auto mime = util::mime_header::from(part_parser);
        auto content_id = mime.content_id<int>(convert_cid);
        EXPECT_TRUE(content_id.has_value());

        // Parse HTTP response
        auto subresponse = util::multipart_subresponse::from(part_parser);
        EXPECT_TRUE(subresponse.is_ok());
        EXPECT_EQ(subresponse.result(), boost::beast::http::status::accepted);

        successful_parts++;
    }

    EXPECT_EQ(successful_parts, 2);
}

TEST(FullMultipartParsing, WithErrors) {
    using namespace cloud_storage_clients;

    // Simulate response with mixed success and errors
    std::string_view batch_response = "--batch_boundary\r\n"
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

    auto buf = iobuf::from(batch_response);

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

    EXPECT_EQ(is_ok_results.size(), 3);
    EXPECT_EQ(content_ids.size(), 3);

    // First one should be successful
    EXPECT_EQ(content_ids[0].value(), 0);
    EXPECT_TRUE(is_ok_results[0]);

    // Second one should be error
    EXPECT_EQ(content_ids[1].value(), 1);
    EXPECT_FALSE(is_ok_results[1]);

    // Third one (404) should be ok for deletes
    EXPECT_EQ(content_ids[2].value(), 2);
    EXPECT_TRUE(is_ok_results[2]);
}

// ----------------------------------------------------------------------------
// Malformed MIME header tests
// ----------------------------------------------------------------------------

TEST(MimeHeaderMalformed, MalformedHeaders) {
    using namespace cloud_storage_clients;

    // Header without colon separator
    std::string_view mime_data = "Content-Type application/http\r\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Missing separator, parser will throw
    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

TEST(MimeHeaderMalformed, MissingSeparatorSpace) {
    using namespace cloud_storage_clients;

    // Header with colon but missing space after it
    std::string_view mime_data = "Content-Type:application/http\r\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Should parse but the value won't match due to missing space
    auto header = util::mime_header::from(parser);
    auto content_type = header.get(boost::beast::http::field::content_type);
    // The field won't be found because we look for ": " separator
    EXPECT_FALSE(content_type.has_value());
}

TEST(MimeHeaderMalformed, OnlyLfLineEndings) {
    using namespace cloud_storage_clients;

    // Using LF only instead of CRLF (malformed per HTTP spec)
    std::string_view mime_data = "Content-Type: application/http\n"
                                 "Content-ID: 10\n"
                                 "\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Should throw because parser expects CRLF
    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

TEST(MimeHeaderMalformed, MixedLineEndings) {
    using namespace cloud_storage_clients;

    // Mix of CRLF and LF
    std::string_view mime_data = "Content-Type: application/http\r\n"
                                 "Content-ID: 10\n" // LF only
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Should throw due to inconsistent line endings
    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

TEST(MimeHeaderMalformed, ExceedsMaxBuffer) {
    using namespace cloud_storage_clients;

    // Create a header that exceeds max_buf (1024 bytes)
    std::string long_value(1100, 'x');
    std::string mime_data = fmt::format(
      "Content-Type: {}\r\n"
      "\r\n",
      long_value);

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Should throw due to exceeding buffer size
    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

TEST(MimeHeaderMalformed, MissingFinalCrlf) {
    using namespace cloud_storage_clients;

    // Header without final CRLF CRLF (truncated)
    std::string_view mime_data = "Content-Type: application/http\r\n"
                                 "Content-ID: 5\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Parser consumes all data but doesn't find the final CRLF CRLF
    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
    EXPECT_EQ(parser.bytes_left(), 0);
}

TEST(MimeHeaderMalformed, CrWithoutLf) {
    using namespace cloud_storage_clients;

    // CR not followed by LF
    std::string_view mime_data = "Content-Type: application/http\rX\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Should throw due to malformed line ending
    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

TEST(MimeHeaderMalformed, LfWithoutCr) {
    using namespace cloud_storage_clients;

    // LF not preceded by CR
    std::string_view mime_data = "Content-Type: application/http\n\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    // Should throw due to malformed line ending
    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

TEST(MimeHeaderMalformed, MultipleColons) {
    using namespace cloud_storage_clients;

    // Header with multiple colons (valid HTTP - should take first)
    std::string_view mime_data = "Content-Type: text/plain: with: colons\r\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    auto header = util::mime_header::from(parser);
    auto content_type = header.get(boost::beast::http::field::content_type);
    EXPECT_TRUE(content_type.has_value());
    // Should include everything after first ": "
    EXPECT_THAT(
      content_type.value(), testing::HasSubstr("text/plain: with: colons"));
}

TEST(MimeHeaderMalformed, ParseEmpty) {
    using namespace cloud_storage_clients;

    std::string_view mime_data = "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

TEST(MimeHeaderMalformed, ParseLeadingCrlf) {
    using namespace cloud_storage_clients;

    std::string_view mime_data = "\r\n"
                                 "Content-Type: application/http\r\n"
                                 "\r\n";

    auto buf = iobuf::from(mime_data);
    iobuf_parser parser(std::move(buf));

    EXPECT_THROW(util::mime_header::from(parser), std::runtime_error);
}

// ----------------------------------------------------------------------------
// Malformed multipart boundary tests
// ----------------------------------------------------------------------------

TEST(MultipartMalformed, MissingStartBoundary) {
    using namespace cloud_storage_clients;

    // Content without opening boundary
    std::string_view multipart_data = "Content-Type: text/plain\r\n"
                                      "\r\n"
                                      "Some content\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Should not find any parts without start boundary
    auto part = parser.get_part();
    EXPECT_FALSE(part.has_value());
}

TEST(MultipartMalformed, TruncatedBoundary) {
    using namespace cloud_storage_clients;

    // Boundary cut off in the middle
    std::string_view multipart_data = "--boundary\r\n"
                                      "Content\r\n"
                                      "\r\n"
                                      "--boun"; // Truncated

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Should return nullopt due to truncation
    auto part = parser.get_part();
    EXPECT_FALSE(part.has_value());
}

TEST(MultipartMalformed, BoundaryWithExtraDashes) {
    using namespace cloud_storage_clients;

    // Boundary with extra dashes
    std::string_view multipart_data = "---boundary\r\n" // Three dashes
                                      "Content\r\n"
                                      "\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Parser expects exactly "--boundary" at the very start of the buffer
    auto part = parser.get_part();
    EXPECT_FALSE(part.has_value());
}

TEST(MultipartMalformed, MissingCrlfBeforeBoundary) {
    using namespace cloud_storage_clients;

    // Missing CRLF before boundary delimiter
    std::string_view multipart_data = "--boundary\r\n"
                                      "First part\r\n"
                                      "--boundary\r\n" // No CRLF before this
                                      "Second part\r\n"
                                      "\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Missing CRLF before boundary is equivalent to finding the boundary in the
    // message, which is illegal per RFC 2046
    auto part1 = parser.get_part();
    EXPECT_FALSE(part1.has_value());
}

TEST(MultipartMalformed, BoundaryInContent) {
    using namespace cloud_storage_clients;

    // Boundary string appears in content
    // NOTE: Multipart parsers cannot distinguish between boundary delimiters
    // and boundary strings in content. This is why RFC 2046 requires that
    // boundaries must not appear in the content, or content must be encoded.
    std::string_view multipart_data = "--boundary\r\n"
                                      "Content-Type: text/plain\r\n"
                                      "\r\n"
                                      "This contains --boundary in text\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Parser should fail when the boundary appears in the message
    EXPECT_FALSE(parser.get_part().has_value());
}

TEST(MultipartMalformed, EmptyBoundary) {
    using namespace cloud_storage_clients;

    std::string_view multipart_data = "--boundary\r\n"
                                      "Content\r\n"
                                      "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    // Empty boundary string
    util::multipart_response_parser parser(std::move(buf), ss::sstring(""));

    // Should not find parts with empty boundary
    auto part = parser.get_part();
    EXPECT_FALSE(part.has_value());
}

TEST(MultipartMalformed, OnlyEndBoundary) {
    using namespace cloud_storage_clients;

    // Only end boundary, no opening boundary
    std::string_view multipart_data = "--boundary--\r\n";

    auto buf = iobuf::from(multipart_data);

    util::multipart_response_parser parser(
      std::move(buf), ss::sstring("--boundary"));

    // Should not find any parts
    auto part = parser.get_part();
    EXPECT_FALSE(part.has_value());
}

// NOTE: multipart_subresponse is effectively a thin wrapper around beast
// request parsers, so we rely on the correctness (and known limitations) of
// their implementation rather than any limited edge case testing we could
// accomplish here. As long as multipart_response_parser returns iobuf parts
// that themselves contain properly trimmed HTTP responses (header + body w/
// correct line endings), then multipart_subresponse should work as expected.

// ============================================================================
// find_multipart_boundary tests
// ============================================================================

TEST(FindMultipartBoundary, ValidBoundary) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    headers.insert(
      boost::beast::http::field::content_type,
      "multipart/mixed; boundary=batch_boundary_123");

    auto result = util::find_multipart_boundary(headers);

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "batch_boundary_123");
}

TEST(FindMultipartBoundary, BoundaryWithWhitespace) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    headers.insert(
      boost::beast::http::field::content_type,
      "multipart/mixed; boundary =\t batch_boundary_123");

    auto result = util::find_multipart_boundary(headers);

    EXPECT_TRUE(result.has_value());
    // Whitespace should be stripped
    EXPECT_EQ(result.value(), "batch_boundary_123");
}

TEST(FindMultipartBoundary, BoundaryWithQuotes) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    headers.insert(
      boost::beast::http::field::content_type,
      R"(multipart/mixed; boundary="batch_boundary_123")");

    auto result = util::find_multipart_boundary(headers);

    EXPECT_TRUE(result.has_value());
    // Quotes should be stripped
    EXPECT_EQ(result.value(), "batch_boundary_123");
}

TEST(FindMultipartBoundary, MissingContentType) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    // No Content-Type header set

    auto result = util::find_multipart_boundary(headers);

    EXPECT_FALSE(result.has_value()) << result.value();
    EXPECT_THAT(result.error(), testing::HasSubstr("Content-Type missing"));
}

TEST(FindMultipartBoundary, NotMultipartContentType) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    headers.insert(boost::beast::http::field::content_type, "application/json");

    auto result = util::find_multipart_boundary(headers);

    EXPECT_FALSE(result.has_value()) << result.value();
    EXPECT_THAT(
      result.error(), testing::HasSubstr("Expected multipart Content-Type"));
}

TEST(FindMultipartBoundary, MissingBoundaryParameter) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    headers.insert(boost::beast::http::field::content_type, "multipart/mixed");

    auto result = util::find_multipart_boundary(headers);

    EXPECT_FALSE(result.has_value()) << result.value();
    EXPECT_THAT(
      result.error(), testing::HasSubstr("Boundary missing from multipart"));
}

TEST(FindMultipartBoundary, MissingBoundaryEqualSign) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    headers.insert(
      boost::beast::http::field::content_type,
      "multipart/mixed; boundary   foobar");

    auto result = util::find_multipart_boundary(headers);

    EXPECT_FALSE(result.has_value()) << result.value();
    EXPECT_THAT(
      result.error(), testing::HasSubstr("Boundary missing from multipart"));
}

TEST(FindMultipartBoundary, EmptyBoundaryParameter) {
    using namespace cloud_storage_clients;

    http::client::response_header headers;
    headers.insert(
      boost::beast::http::field::content_type, R"(multipart/mixed; boundary=)");

    auto result = util::find_multipart_boundary(headers);

    EXPECT_FALSE(result.has_value()) << result.value();
    EXPECT_THAT(
      result.error(), testing::HasSubstr("Boundary missing from multipart"));
}
