/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "s3/client.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_istreambuf.h"
#include "hashing/secure.h"
#include "s3/error.h"
#include "s3/logger.h"
#include "s3/signature.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

#include <boost/beast/core/error.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <fmt/core.h>
#include <gnutls/crypto.h>

#include <exception>

namespace s3 {

struct aws_header_names {
    static constexpr boost::beast::string_view prefix = "prefix";
    static constexpr boost::beast::string_view start_after = "start-after";
    static constexpr boost::beast::string_view max_keys = "max-keys";
    static constexpr boost::beast::string_view x_amz_content_sha256
      = "x-amz-content-sha256";
};

struct aws_header_values {
    static constexpr boost::beast::string_view user_agent
      = "redpanda.vectorized.io";
    static constexpr boost::beast::string_view text_plain = "text/plain";
};

// configuration //

ss::future<configuration> configuration::make_configuration(
  const public_key_str& pkey,
  const private_key_str& skey,
  const aws_region_name& region) {
    configuration client_cfg;
    ss::tls::credentials_builder cred_builder;
    const auto endpoint_uri = fmt::format("s3.{}.amazonaws.com", region());
    // Setup credentials for TLS
    ss::tls::credentials_builder builder;
    client_cfg.access_key = pkey;
    client_cfg.secret_key = skey;
    client_cfg.region = region;
    client_cfg.uri = access_point_uri(endpoint_uri);
    // NOTE: this is a pre-defined gnutls priority string that
    // picks the the ciphersuites with 128-bit ciphers which
    // leads to up to 10x improvement in upload speed, compared
    // to 256-bit ciphers
    cred_builder.set_priority_string("PERFORMANCE");
    co_await cred_builder.set_system_trust();
    client_cfg.credentials
      = co_await cred_builder.build_reloadable_certificate_credentials();
    auto addr = co_await ss::net::dns::resolve_name(
      client_cfg.uri(), ss::net::inet_address::family::INET);
    constexpr uint16_t port = 443;
    client_cfg.server_addr = ss::socket_address(addr, port);
    co_return client_cfg;
}

// request_creator //

request_creator::request_creator(const configuration& conf)
  : _ap(conf.uri)
  , _sign(conf.region, conf.access_key, conf.secret_key) {}

result<http::client::request_header> request_creator::make_get_object_request(
  bucket_name const& name, object_key const& key) {
    http::client::request_header header{};
    // GET /{object-id} HTTP/1.1
    // Host: {bucket-name}.s3.amazonaws.com
    // x-amz-date:{req-datetime}
    // Authorization:{signature}
    // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/{}", key());
    std::string emptysig
      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    header.insert(aws_header_names::x_amz_content_sha256, emptysig);
    auto ec = _sign.sign_header(header, emptysig);
    if (ec) {
        return ec;
    }
    return header;
}

result<http::client::request_header>
request_creator::make_unsigned_put_object_request(
  bucket_name const& name, object_key const& key, size_t payload_size_bytes) {
    // PUT /my-image.jpg HTTP/1.1
    // Host: myBucket.s3.<Region>.amazonaws.com
    // Date: Wed, 12 Oct 2009 17:50:00 GMT
    // Authorization: authorization string
    // Content-Type: text/plain
    // Content-Length: 11434
    // x-amz-meta-author: Janet
    // Expect: 100-continue
    // [11434 bytes of object data]
    http::client::request_header header{};
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/{}", key());
    std::string sig = "UNSIGNED-PAYLOAD";
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(
      boost::beast::http::field::content_type, aws_header_values::text_plain);
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(payload_size_bytes));
    header.insert(aws_header_names::x_amz_content_sha256, sig);
    auto ec = _sign.sign_header(header, sig);
    if (ec) {
        return ec;
    }
    return header;
}

result<http::client::request_header>
request_creator::make_list_objects_v2_request(
  const bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys) {
    // GET /?list-type=2&prefix=photos/2006/&delimiter=/ HTTP/1.1
    // Host: example-bucket.s3.<Region>.amazonaws.com
    // x-amz-date: 20160501T000433Z
    // Authorization: authorization string
    http::client::request_header header{};
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/?list-type=2");
    std::string emptysig
      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    header.insert(aws_header_names::x_amz_content_sha256, emptysig);
    if (prefix) {
        header.insert(
          aws_header_names::prefix,
          boost::beast::string_view{(*prefix)().data(), (*prefix)().size()});
    }
    if (start_after) {
        header.insert(
          aws_header_names::start_after,
          boost::beast::string_view{
            (*start_after)().data(), (*start_after)().size()});
    }
    if (max_keys) {
        header.insert(aws_header_names::start_after, std::to_string(*max_keys));
    }
    auto ec = _sign.sign_header(header, emptysig);
    vlog(s3_log.trace, "ListObjectsV2:\n {}", header);
    if (ec) {
        return ec;
    }
    return header;
}

result<http::client::request_header>
request_creator::make_delete_object_request(
  bucket_name const& name, object_key const& key) {
    http::client::request_header header{};
    // DELETE /{object-id} HTTP/1.1
    // Host: {bucket-name}.s3.amazonaws.com
    // x-amz-date:{req-datetime}
    // Authorization:{signature}
    // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    //
    // NOTE: x-amz-mfa, x-amz-bypass-governance-retention are not used for now
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/{}", key());
    std::string emptysig
      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    header.insert(aws_header_names::x_amz_content_sha256, emptysig);
    auto ec = _sign.sign_header(header, emptysig);
    if (ec) {
        return ec;
    }
    return header;
}

// client //

/// Convert iobuf that contains xml data to boost::property_tree
static boost::property_tree::ptree iobuf_to_ptree(iobuf&& buf) {
    namespace pt = boost::property_tree;
    try {
        iobuf_istreambuf strbuf(buf);
        std::istream stream(&strbuf);
        pt::ptree res;
        pt::read_xml(stream, res);
        return res;
    } catch (...) {
        vlog(s3_log.error, "!!parsing error {}", std::current_exception());
        throw;
    }
}

/// Parse timestamp in format that S3 uses
static std::chrono::system_clock::time_point
parse_timestamp(std::string_view sv) {
    std::tm tm = {};
    std::stringstream ss({sv.data(), sv.size()});
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.Z%Z");
    return std::chrono::system_clock::from_time_t(timegm(&tm));
}

static client::list_bucket_result iobuf_to_list_bucket_result(iobuf&& buf) {
    try {
        for (auto& frag : buf) {
            vlog(
              s3_log.trace,
              "iobuf_to_list_bucket_result part {}",
              ss::sstring{frag.get(), frag.size()});
        }
        client::list_bucket_result result;
        auto root = iobuf_to_ptree(std::move(buf));
        for (const auto& [tag, value] : root.get_child("ListBucketResult")) {
            if (tag == "Contents") {
                client::list_bucket_item item;
                for (const auto& [item_tag, item_value] : value) {
                    if (item_tag == "Key") {
                        item.key = item_value.get_value<ss::sstring>();
                    } else if (item_tag == "Size") {
                        item.size_bytes = item_value.get_value<size_t>();
                    } else if (item_tag == "LastModified") {
                        item.last_modified = parse_timestamp(
                          item_value.get_value<ss::sstring>());
                    }
                }
                result.contents.push_back(std::move(item));
            } else if (tag == "IsTruncated") {
                // read value as bool
                result.is_truncated = value.get_value<bool>();
            } else if (tag == "Prefix") {
                result.prefix = value.get_value<ss::sstring>("");
            }
        }
        return result;
    } catch (...) {
        vlog(s3_log.error, "!!error parse result {}", std::current_exception());
        throw;
    }
}

template<class ResultT = void>
ss::future<ResultT> parse_rest_error_response(iobuf&& buf) {
    try {
        auto resp = iobuf_to_ptree(std::move(buf));
        constexpr const char* empty = "";
        auto code = resp.get<ss::sstring>("Error.Code", empty);
        auto msg = resp.get<ss::sstring>("Error.Message", empty);
        auto rid = resp.get<ss::sstring>("Error.RequestId", empty);
        auto res = resp.get<ss::sstring>("Error.Resource", empty);
        rest_error_response err(code, msg, rid, res);
        return ss::make_exception_future<ResultT>(err);
    } catch (...) {
        vlog(s3_log.error, "!!error parse error {}", std::current_exception());
        throw;
    }
}

static ss::future<iobuf>
drain_response_stream(http::client::response_stream_ref resp) {
    return ss::do_with(
      iobuf(), [resp = std::move(resp)](iobuf& outbuf) mutable {
          return ss::do_until(
                   [resp] { return resp->is_done(); },
                   [resp, &outbuf] {
                       return resp->recv_some().then([&outbuf](iobuf&& chunk) {
                           outbuf.append(std::move(chunk));
                       });
                   })
            .then([&outbuf] {
                return ss::make_ready_future<iobuf>(std::move(outbuf));
            });
      });
}

client::client(const configuration& conf)
  : _requestor(conf)
  , _client(conf) {}

ss::future<> client::shutdown() { return _client.shutdown(); }
ss::future<http::client::response_stream_ref>
client::get_object(bucket_name const& name, object_key const& key) {
    auto header = _requestor.make_get_object_request(name, key);
    if (!header) {
        return ss::make_exception_future<http::client::response_stream_ref>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return _client.request(std::move(header.value()))
      .then([](http::client::response_stream_ref&& ref) {
          // here we didn't receive any bytes from the socket and
          // ref->is_header_done() is 'false', we need to prefetch
          // the header first
          return ref->prefetch_headers().then([ref = std::move(ref)]() mutable {
              vassert(ref->is_header_done(), "Header is not received");
              if (
                ref->get_headers().result() != boost::beast::http::status::ok) {
                  // Got error response, consume the response body and produce
                  // rest api error
                  return drain_response_stream(std::move(ref))
                    .then([](iobuf&& res) {
                        return parse_rest_error_response<
                          http::client::response_stream_ref>(std::move(res));
                    });
              }
              return ss::make_ready_future<http::client::response_stream_ref>(
                std::move(ref));
          });
      });
}

ss::future<> client::put_object(
  bucket_name const& name,
  object_key const& id,
  size_t payload_size,
  ss::input_stream<char>&& body) {
    auto header = _requestor.make_unsigned_put_object_request(
      name, id, payload_size);
    if (!header) {
        return ss::make_exception_future<>(std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return ss::do_with(
      std::move(body),
      [this, header = std::move(header)](ss::input_stream<char>& body) mutable {
          return _client.request(std::move(header.value()), body)
            .then([](const http::client::response_stream_ref& ref) {
                return drain_response_stream(ref).then([ref](iobuf&& res) {
                    auto status = ref->get_headers().result();
                    if (status != boost::beast::http::status::ok) {
                        return parse_rest_error_response<>(std::move(res));
                    }
                    return ss::now();
                });
            });
      });
}

ss::future<client::list_bucket_result> client::list_objects_v2(
  const bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys) {
    auto header = _requestor.make_list_objects_v2_request(
      name, std::move(prefix), std::move(start_after), max_keys);
    if (!header) {
        return ss::make_exception_future<list_bucket_result>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return _client.request(std::move(header.value()))
      .then([](const http::client::response_stream_ref& resp) mutable {
          // chunked encoding is used so we don't know output size in
          // advance
          return ss::do_with(
            resp->as_input_stream(),
            iobuf(),
            [resp](ss::input_stream<char>& stream, iobuf& outbuf) mutable {
                return ss::do_until(
                         [&stream] { return stream.eof(); },
                         [&stream, &outbuf] {
                             return stream.read().then(
                               [&outbuf](ss::temporary_buffer<char>&& chunk) {
                                   outbuf.append(std::move(chunk));
                               });
                         })
                  .then([&outbuf, resp] {
                      const auto& header = resp->get_headers();
                      if (header.result() != boost::beast::http::status::ok) {
                          // We received error response so the outbuf contains
                          // error digest instead of the result of the query
                          return parse_rest_error_response<
                            client::list_bucket_result>(std::move(outbuf));
                      }
                      auto res = iobuf_to_list_bucket_result(std::move(outbuf));
                      return ss::make_ready_future<list_bucket_result>(
                        std::move(res));
                  });
            });
      });
}

ss::future<>
client::delete_object(const bucket_name& bucket, const object_key& key) {
    auto header = _requestor.make_delete_object_request(bucket, key);
    if (!header) {
        return ss::make_exception_future<>(std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return _client.request(std::move(header.value()))
      .then([](const http::client::response_stream_ref& ref) {
          return drain_response_stream(ref).then([ref](iobuf&& res) {
              auto status = ref->get_headers().result();
              if (
                status != boost::beast::http::status::ok
                && status
                     != boost::beast::http::status::no_content) { // expect 204
                  return parse_rest_error_response<>(std::move(res));
              }
              return ss::now();
          });
      });
}

} // namespace s3
