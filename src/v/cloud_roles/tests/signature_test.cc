/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/signature.h"
#include "http/client.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <sstream>

std::chrono::time_point<std::chrono::system_clock>
parse_time(std::string const& timestr) {
    std::tm tm = {};
    std::stringstream ss(timestr + "0");
    ss >> std::get_time(&tm, "%Y%m%dT%H%M%SZ%Z");
    return std::chrono::system_clock::from_time_t(timegm(&tm));
}

/// Test is based on 1st example here
/// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
SEASTAR_THREAD_TEST_CASE(test_signature_computation_1) {
    cloud_roles::public_key_str access_key("AKIAIOSFODNN7EXAMPLE");
    cloud_roles::private_key_str secret_key(
      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    cloud_roles::aws_region_name region("us-east-1");
    std::string host = "examplebucket.s3.amazonaws.com";
    std::string target = "/test.txt";
    std::string sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca4959"
                         "91b7852b855"; // empty hash

    auto tp = parse_time("20130524T000000Z");
    cloud_roles::signature_v4 sign(
      region, access_key, secret_key, cloud_roles::time_source(tp));

    http::client::request_header header;
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::range, "bytes=0-9");

    sign.sign_header(header, sha256);

    std::string expected
      = "AWS4-HMAC-SHA256 "
        "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"
        "Signature="
        "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";

    BOOST_REQUIRE_EQUAL(
      header.at(boost::beast::http::field::authorization), expected);
}

/// Test is based on 2nd example here
/// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
SEASTAR_THREAD_TEST_CASE(test_signature_computation_2) {
    cloud_roles::public_key_str access_key("AKIAIOSFODNN7EXAMPLE");
    cloud_roles::private_key_str secret_key(
      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    cloud_roles::aws_region_name region("us-east-1");
    std::string host = "examplebucket.s3.amazonaws.com";
    std::string target = "test$file.text";
    std::string sha256
      = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072";

    auto tp = parse_time("20130524T000000Z");
    cloud_roles::signature_v4 sign(
      region, access_key, secret_key, cloud_roles::time_source(tp));

    http::client::request_header header;
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert("x-amz-storage-class", "REDUCED_REDUNDANCY");
    header.insert(boost::beast::http::field::host, host);
    header.insert(
      boost::beast::http::field::date, "Fri, 24 May 2013 00:00:00 GMT");

    sign.sign_header(header, sha256);

    std::string expected
      = "AWS4-HMAC-SHA256 "
        "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-"
        "class,"
        "Signature="
        "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd";

    BOOST_REQUIRE_EQUAL(
      header.at(boost::beast::http::field::authorization), expected);
}

/// Test is based on 3rd example here
/// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
SEASTAR_THREAD_TEST_CASE(test_signature_computation_3) {
    cloud_roles::public_key_str access_key("AKIAIOSFODNN7EXAMPLE");
    cloud_roles::private_key_str secret_key(
      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    cloud_roles::aws_region_name region("us-east-1");
    std::string host = "examplebucket.s3.amazonaws.com";
    std::string target = "?lifecycle";
    std::string sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca4959"
                         "91b7852b855"; // empty hash

    auto tp = parse_time("20130524T000000Z");
    cloud_roles::signature_v4 sign(
      region, access_key, secret_key, cloud_roles::time_source(tp));

    http::client::request_header header;
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    sign.sign_header(header, sha256);

    std::string expected
      = "AWS4-HMAC-SHA256 "
        "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature="
        "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543";

    BOOST_REQUIRE_EQUAL(
      header.at(boost::beast::http::field::authorization), expected);
}

/// Test is based on 4th example here
/// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
SEASTAR_THREAD_TEST_CASE(test_signature_computation_4) {
    cloud_roles::public_key_str access_key("AKIAIOSFODNN7EXAMPLE");
    cloud_roles::private_key_str secret_key(
      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    cloud_roles::aws_region_name region("us-east-1");
    std::string host = "examplebucket.s3.amazonaws.com";
    std::string target = "?max-keys=2&prefix=J";
    std::string sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca4959"
                         "91b7852b855"; // empty hash

    auto tp = parse_time("20130524T000000Z");
    cloud_roles::signature_v4 sign(
      region, access_key, secret_key, cloud_roles::time_source(tp));

    http::client::request_header header;
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    sign.sign_header(header, sha256);

    std::string expected
      = "AWS4-HMAC-SHA256 "
        "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature="
        "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7";

    BOOST_REQUIRE_EQUAL(
      header.at(boost::beast::http::field::authorization), expected);
}

SEASTAR_THREAD_TEST_CASE(test_abs_signature_computation) {
    cloud_roles::private_key_str shared_key{
      "fszSAFOYI+AH3Befu2gnvip9B9QKZA9i8+"
      "vbwsGtgA29ouezYToMSW2eR0PtSw7ZMPh2cmpsGFnU+AStcy+jUg=="};
    cloud_roles::storage_account storage_acc{"vladstorageaccount123"};
    // Get Blob Request
    std::string host = "vladstorageaccount123.blob.core.windows.net";
    std::string target = "/vlad-container/one-blob?timeout=10";

    auto tp = parse_time("20221220T110100Z");
    cloud_roles::signature_abs sign(
      storage_acc, shared_key, cloud_roles::time_source(tp));

    http::client::request_header header;
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    sign.sign_header(header);

    std::string expected
      = "SharedKey "
        "vladstorageaccount123:rDsCDHPhdsr7SMkt81ofyrnNNxL3VKFW7ydsDQeVPIM=";

    BOOST_REQUIRE_EQUAL(
      header.at(boost::beast::http::field::authorization), expected);
}

SEASTAR_THREAD_TEST_CASE(test_abs_signature_computation_many_query_params) {
    cloud_roles::private_key_str shared_key{
      "fszSAFOYI+AH3Befu2gnvip9B9QKZA9i8+"
      "vbwsGtgA29ouezYToMSW2eR0PtSw7ZMPh2cmpsGFnU+AStcy+jUg=="};
    cloud_roles::storage_account storage_acc{"vladstorageaccount123"};
    std::string host = "vladstorageaccount123.blob.core.windows.net";
    // List Containers Request
    std::string target = "/?comp=list&timeout=20";

    auto tp = parse_time("20221220T111700Z");
    cloud_roles::signature_abs sign(
      storage_acc, shared_key, cloud_roles::time_source(tp));

    http::client::request_header header;
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    sign.sign_header(header);

    std::string expected
      = "SharedKey "
        "vladstorageaccount123:hWp74AgakkrSYVzYBKSabfLP4NWVa410CNRm/dMxX2M=";

    BOOST_REQUIRE_EQUAL(
      header.at(boost::beast::http::field::authorization), expected);
}

/// Test is based on this example
/// https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
SEASTAR_THREAD_TEST_CASE(test_gnutls) {
    std::string ksecret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    std::string date = "20150830";
    std::string region = "us-east-1";
    std::string service = "iam";
    auto digest = cloud_roles::signature_v4::gen_sig_key(
      ksecret, date, region, service);
    std::array<uint8_t, 32> result{};
    std::memcpy(result.data(), digest.data(), 32);
    BOOST_REQUIRE_EQUAL(
      to_hex(bytes_view{result.data(), 32}),
      "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9");
}
