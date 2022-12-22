/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/s3_client.h"

#include "bytes/iobuf_istreambuf.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/s3_error.h"
#include "hashing/secure.h"
#include "http/client.h"
#include "net/connection.h"
#include "net/tls.h"
#include "net/types.h"
#include "ssx/sformat.h"
#include "utils/base64.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/log.hh>

#include <boost/beast/core/error.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <gnutls/crypto.h>

#include <exception>
#include <utility>

namespace cloud_storage_clients {

// Close all connections that were used more than 5 seconds ago.
// AWS S3 endpoint has timeout of 10 seconds. But since we're supporting
// not only AWS S3 it makes sense to set timeout value a bit lower.
static constexpr ss::lowres_clock::duration default_max_idle_time
  = std::chrono::seconds(5);

struct aws_header_names {
    static constexpr boost::beast::string_view prefix = "prefix";
    static constexpr boost::beast::string_view start_after = "start-after";
    static constexpr boost::beast::string_view max_keys = "max-keys";
    static constexpr boost::beast::string_view x_amz_tagging = "x-amz-tagging";
    static constexpr boost::beast::string_view x_amz_request_id
      = "x-amz-request-id";
};

struct aws_header_values {
    static constexpr boost::beast::string_view user_agent
      = "redpanda.vectorized.io";
    static constexpr boost::beast::string_view text_plain = "text/plain";
};

// configuration //

static ss::sstring make_endpoint_url(
  const cloud_roles::aws_region_name& region,
  const std::optional<endpoint_url>& url_override) {
    if (url_override) {
        return url_override.value();
    }
    return ssx::sformat("s3.{}.amazonaws.com", region());
}

ss::future<s3_configuration> s3_configuration::make_configuration(
  const std::optional<cloud_roles::public_key_str>& pkey,
  const std::optional<cloud_roles::private_key_str>& skey,
  const cloud_roles::aws_region_name& region,
  const default_overrides& overrides,
  net::metrics_disabled disable_metrics,
  net::public_metrics_disabled disable_public_metrics) {
    s3_configuration client_cfg;
    const auto endpoint_uri = make_endpoint_url(region, overrides.endpoint);
    client_cfg.tls_sni_hostname = endpoint_uri;
    // Setup credentials for TLS
    client_cfg.access_key = pkey;
    client_cfg.secret_key = skey;
    client_cfg.region = region;
    client_cfg.uri = access_point_uri(endpoint_uri);
    ss::tls::credentials_builder cred_builder;
    if (overrides.disable_tls == false) {
        // NOTE: this is a pre-defined gnutls priority string that
        // picks the ciphersuites with 128-bit ciphers which
        // leads to up to 10x improvement in upload speed, compared
        // to 256-bit ciphers
        cred_builder.set_priority_string("PERFORMANCE");
        if (overrides.trust_file.has_value()) {
            auto file = overrides.trust_file.value();
            vlog(s3_log.info, "Use non-default trust file {}", file());
            co_await cred_builder.set_x509_trust_file(
              file().string(), ss::tls::x509_crt_format::PEM);
        } else {
            // Use GnuTLS defaults, might not work on all systems
            auto ca_file = co_await net::find_ca_file();
            if (ca_file) {
                vlog(
                  s3_log.info,
                  "Use automatically discovered trust file {}",
                  ca_file.value());
                co_await cred_builder.set_x509_trust_file(
                  ca_file.value(), ss::tls::x509_crt_format::PEM);
            } else {
                vlog(
                  s3_log.info,
                  "Trust file can't be detected automatically, using GnuTLS "
                  "default");
                co_await cred_builder.set_system_trust();
            }
        }
        client_cfg.credentials
          = co_await cred_builder.build_reloadable_certificate_credentials();
    }

    constexpr uint16_t default_port = 443;

    client_cfg.server_addr = net::unresolved_address(
      client_cfg.uri(),
      overrides.port ? *overrides.port : default_port,
      ss::net::inet_address::family::INET);
    client_cfg.disable_metrics = disable_metrics;
    client_cfg.disable_public_metrics = disable_public_metrics;
    client_cfg._probe = ss::make_shared<client_probe>(
      disable_metrics, disable_public_metrics, region(), endpoint_uri);
    client_cfg.max_idle_time = overrides.max_idle_time
                                 ? *overrides.max_idle_time
                                 : default_max_idle_time;
    co_return client_cfg;
}

std::ostream& operator<<(std::ostream& o, const s3_configuration& c) {
    o << "{access_key:"
      << c.access_key.value_or(cloud_roles::public_key_str{""})
      << ",region:" << c.region() << ",secret_key:****"
      << ",access_point_uri:" << c.uri() << ",server_addr:" << c.server_addr
      << ",max_idle_time:"
      << std::chrono::duration_cast<std::chrono::milliseconds>(c.max_idle_time)
           .count()
      << "}";
    return o;
}

// request_creator //

request_creator::request_creator(
  const s3_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _ap(conf.uri)
  , _apply_credentials{std::move(apply_credentials)} {}

result<http::client::request_header> request_creator::make_get_object_request(
  bucket_name const& name, object_key const& key) {
    http::client::request_header header{};
    // GET /{object-id} HTTP/1.1
    // Host: {bucket-name}.s3.amazonaws.com
    // x-amz-date:{req-datetime}
    // Authorization:{signature}
    // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/{}", key().string());
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    return header;
}

result<http::client::request_header> request_creator::make_head_object_request(
  bucket_name const& name, object_key const& key) {
    http::client::request_header header{};
    // HEAD /{object-id} HTTP/1.1
    // Host: {bucket-name}.s3.amazonaws.com
    // x-amz-date:{req-datetime}
    // Authorization:{signature}
    // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/{}", key().string());
    header.method(boost::beast::http::verb::head);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    return header;
}

result<http::client::request_header>
request_creator::make_unsigned_put_object_request(
  bucket_name const& name,
  object_key const& key,
  size_t payload_size_bytes,
  const object_tag_formatter& tags) {
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
    auto target = fmt::format("/{}", key().string());
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

    if (!tags.empty()) {
        header.insert(aws_header_names::x_amz_tagging, tags.str());
    }

    auto ec = _apply_credentials->add_auth(header);
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
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");

    if (prefix) {
        header.insert(aws_header_names::prefix, (*prefix)().string());
    }
    if (start_after) {
        header.insert(aws_header_names::start_after, (*start_after)().string());
    }
    if (max_keys) {
        header.insert(aws_header_names::start_after, std::to_string(*max_keys));
    }

    auto ec = _apply_credentials->add_auth(header);
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
    auto target = fmt::format("/{}", key().string());
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    return header;
}

struct delete_objects_body : public ss::data_source_impl {
    ss::temporary_buffer<char> data;
    explicit delete_objects_body(std::string_view body) noexcept
      : data{body.data(), body.size()} {}
    auto get() -> ss::future<ss::temporary_buffer<char>> override {
        return ss::make_ready_future<ss::temporary_buffer<char>>(
          std::exchange(data, {}));
    }
};

result<std::tuple<http::client::request_header, ss::input_stream<char>>>
request_creator::make_delete_objects_request(
  const bucket_name& name, std::span<const object_key> keys) {
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    // will generate this request:
    //
    // POST /?delete HTTP/1.1
    // Host: <Bucket>.s3.amazonaws.com
    // Content-MD5: <Computer from body>
    // Authorization: <applied by _requestor>
    // Content-Length: <...>
    //
    // <?xml version="1.0" encoding="UTF-8"?>
    // <Delete>
    //     <Object>
    //         <Key>object_key</Key>
    //     </Object>
    //     <Object>
    //         <Key>object_key</Key>
    //     </Object>
    //      ...
    //     <Quiet>true</Quiet>
    // </Delete>
    //
    // note:
    //  - Delete.Quiet true will generate a response that reports only failures
    //  to delete or errors
    //  - the actual xml might not be pretty-printed
    //  - with clang15 and ranges, xml generation could be a one-liner + a
    //  custom formatter for xml escaping

    auto body = [&] {
        auto delete_tree = boost::property_tree::ptree{};
        // request a quiet response
        delete_tree.put("Delete.Quiet", true);
        // add an array of Object.Key=key to the Delete root
        for (auto key_tree = boost::property_tree::ptree{};
             auto const& k : keys) {
            key_tree.put("Key", k().c_str());
            delete_tree.add_child("Delete.Object", key_tree);
        }

        auto out = std::ostringstream{};
        boost::property_tree::write_xml(out, delete_tree);
        return out.str();
    }();

    auto body_md5 = [&] {
        // compute md5 and produce a base64 encoded signature for body
        auto hash = internal::hash<GNUTLS_DIG_MD5, 16>{};
        hash.update(body);
        auto bin_digest = hash.reset();
        return bytes_to_base64(
          {reinterpret_cast<const uint8_t*>(bin_digest.data()),
           bin_digest.size()});
    }();

    auto header = http::client::request_header{};
    header.method(boost::beast::http::verb::post);
    header.target("/?delete");
    header.insert(
      boost::beast::http::field::host, fmt::format("{}.{}", name(), _ap()));
    // from experiments, minio is sloppy in checking this field. It will check
    // that it's valid base64, but seems not to actually check the value
    header.insert(
      boost::beast::http::field::content_md5,
      {body_md5.data(), body_md5.size()});

    header.insert(
      boost::beast::http::field::content_length,
      fmt::format("{}", body.size()));

    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }

    return {
      std::move(header),
      ss::input_stream<char>{ss::data_source{
        std::make_unique<delete_objects_body>(std::move(body))}}};
}

// client //

static void log_buffer_with_rate_limiting(const char* msg, iobuf& buf) {
    static constexpr int buffer_size = 0x100;
    static constexpr auto rate_limit = std::chrono::seconds(1);
    thread_local static ss::logger::rate_limit rate(rate_limit);
    auto log_with_rate_limit = [](ss::logger::format_info fmt, auto... args) {
        s3_log.log(ss::log_level::warn, rate, fmt, args...);
    };
    iobuf_istreambuf strbuf(buf);
    std::istream stream(&strbuf);
    std::array<char, buffer_size> str{};
    auto sz = stream.readsome(str.data(), buffer_size);
    auto sview = std::string_view(str.data(), sz);
    vlog(log_with_rate_limit, "{}: {}", msg, sview);
}

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
        log_buffer_with_rate_limiting("unexpected reply", buf);
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

static s3_client::list_bucket_result iobuf_to_list_bucket_result(iobuf&& buf) {
    try {
        for (auto& frag : buf) {
            vlog(
              s3_log.trace,
              "iobuf_to_list_bucket_result part {}",
              ss::sstring{frag.get(), frag.size()});
        }
        s3_client::list_bucket_result result;
        auto root = iobuf_to_ptree(std::move(buf));
        for (const auto& [tag, value] : root.get_child("ListBucketResult")) {
            if (tag == "Contents") {
                s3_client::list_bucket_item item;
                for (const auto& [item_tag, item_value] : value) {
                    if (item_tag == "Key") {
                        item.key = item_value.get_value<ss::sstring>();
                    } else if (item_tag == "Size") {
                        item.size_bytes = item_value.get_value<size_t>();
                    } else if (item_tag == "LastModified") {
                        item.last_modified = parse_timestamp(
                          item_value.get_value<ss::sstring>());
                    } else if (item_tag == "ETag") {
                        item.etag = item_value.get_value<ss::sstring>();
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
ss::future<ResultT>
parse_rest_error_response(boost::beast::http::status result, iobuf&& buf) {
    if (buf.empty()) {
        // AWS errors occasionally come with an empty body
        // (See https://github.com/redpanda-data/redpanda/issues/6061)
        // Without a proper code, we treat it as a hint to gracefully retry
        // (synthesize the slow_down code).
        rest_error_response err(
          fmt::format("{}", cloud_storage_clients::s3_error_code::slow_down),
          fmt::format("Empty error response, status code {}", result),
          "",
          "");
        return ss::make_exception_future<ResultT>(err);

    } else {
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
            vlog(
              s3_log.error, "!!error parse error {}", std::current_exception());
            throw;
        }
    }
}

/// Head response doesn't give us an XML encoded error object in
/// the body. This method uses headers to generate an error object.
template<class ResultT = void>
ss::future<ResultT> parse_head_error_response(
  const http::http_response::header_type& hdr, const object_key& key) {
    try {
        ss::sstring code;
        ss::sstring msg;
        if (hdr.result() == boost::beast::http::status::not_found) {
            code = "NoSuchKey";
            msg = "Not found";
        } else {
            code = "Unknown";
            msg = ss::sstring(hdr.reason().data(), hdr.reason().size());
        }
        auto rid = hdr.at(aws_header_names::x_amz_request_id);
        rest_error_response err(
          code, msg, ss::sstring(rid.data(), rid.size()), key().native());
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

template<typename T>
ss::future<result<T, error_outcome>> s3_client::send_request(
  ss::future<T> request_future,
  const bucket_name& bucket,
  const object_key& key) {
    auto outcome = error_outcome::retry;

    try {
        co_return co_await std::move(request_future);
    } catch (const rest_error_response& err) {
        if (err.code() == s3_error_code::no_such_key) {
            // Unexpected 404s are logged by 'request_future' at warn
            // level, so only log at debug level here.
            vlog(s3_log.debug, "NoSuchKey response received {}", key);
            outcome = error_outcome::key_not_found;
        } else if (err.code() == s3_error_code::no_such_bucket) {
            vlog(
              s3_log.error,
              "The specified S3 bucket, {}, could not be found. Ensure that "
              "your bucket exists and that the cloud_storage_bucket and "
              "cloud_storage_region cluster configs are correct.",
              bucket());

            outcome = error_outcome::bucket_not_found;
        } else if (
          err.code() == s3_error_code::slow_down
          || err.code() == s3_error_code::internal_error) {
            // This can happen when we're dealing with high request rate to
            // the manifest's prefix. Backoff algorithm should be applied.
            // In principle only slow_down should occur, but in practice
            // AWS S3 does return internal_error as well sometimes.
            vlog(s3_log.warn, "{} response received {}", err.code(), bucket);
            outcome = error_outcome::retry_slowdown;
        } else {
            // Unexpected REST API error, we can't recover from this
            // because the issue is not temporary (e.g. bucket doesn't
            // exist)
            vlog(
              s3_log.error,
              "Accessing {}, unexpected REST API error \"{}\" detected, "
              "code: "
              "{}, request_id: {}, resource: {}",
              bucket,
              err.message(),
              err.code_string(),
              err.request_id(),
              err.resource());
            outcome = error_outcome::fail;
        }
    } catch (const std::system_error& cerr) {
        // The system_error is type erased and not convenient for selective
        // handling. The following errors should be retried:
        // - connection refused, timed out or reset by peer
        // - network temporary unavailable
        // Shouldn't be retried
        // - any filesystem error
        // - broken-pipe
        // - any other network error (no memory, bad socket, etc)
        if (net::is_reconnect_error(cerr)) {
            vlog(
              s3_log.warn,
              "System error susceptible for retry {}",
              cerr.what());
        } else {
            vlog(s3_log.error, "System error {}", cerr);
            outcome = error_outcome::fail;
        }
    } catch (const ss::timed_out_error& terr) {
        // This should happen when the connection pool was disconnected
        // from the S3 endpoint and subsequent connection attmpts failed.
        vlog(s3_log.warn, "Connection timeout {}", terr.what());
    } catch (const boost::system::system_error& err) {
        if (err.code() != boost::beast::http::error::short_read) {
            vlog(s3_log.warn, "Connection failed {}", err.what());
            outcome = error_outcome::fail;
        } else {
            // This is a short read error that can be caused by the abrupt TLS
            // shutdown. The content of the received buffer is discarded in this
            // case and http client receives an empty buffer.
            vlog(
              s3_log.info,
              "Server disconnected: '{}', retrying HTTP request",
              err.what());
        }
    } catch (const ss::abort_requested_exception&) {
        vlog(s3_log.debug, "Abort requested");
        throw;
    } catch (...) {
        vlog(s3_log.error, "Unexpected error {}", std::current_exception());
        outcome = error_outcome::fail;
    }

    co_return outcome;
}

s3_client::s3_client(
  const s3_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _requestor(conf, std::move(apply_credentials))
  , _client(conf)
  , _probe(conf._probe) {}

s3_client::s3_client(
  const s3_configuration& conf,
  const ss::abort_source& as,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _requestor(conf, std::move(apply_credentials))
  , _client(conf, &as, conf._probe, conf.max_idle_time)
  , _probe(conf._probe) {}

ss::future<> s3_client::stop() { return _client.stop(); }

void s3_client::shutdown() { _client.shutdown(); }

ss::future<result<http::client::response_stream_ref, error_outcome>>
s3_client::get_object(
  bucket_name const& name,
  object_key const& key,
  const ss::lowres_clock::duration& timeout,
  bool expect_no_such_key) {
    return send_request(
      do_get_object(name, key, timeout, expect_no_such_key), name, key);
}

ss::future<http::client::response_stream_ref> s3_client::do_get_object(
  bucket_name const& name,
  object_key const& key,
  const ss::lowres_clock::duration& timeout,
  bool expect_no_such_key) {
    auto header = _requestor.make_get_object_request(name, key);
    if (!header) {
        return ss::make_exception_future<http::client::response_stream_ref>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
      .then([expect_no_such_key](http::client::response_stream_ref&& ref) {
          // here we didn't receive any bytes from the socket and
          // ref->is_header_done() is 'false', we need to prefetch
          // the header first
          return ref->prefetch_headers().then(
            [ref = std::move(ref), expect_no_such_key]() mutable {
                vassert(ref->is_header_done(), "Header is not received");
                const auto result = ref->get_headers().result();
                if (result != boost::beast::http::status::ok) {
                    // Got error response, consume the response body and produce
                    // rest api error
                    if (
                      expect_no_such_key
                      && result == boost::beast::http::status::not_found) {
                        vlog(
                          s3_log.debug,
                          "S3 replied with expected error: {}",
                          ref->get_headers());
                    } else {
                        vlog(
                          s3_log.warn,
                          "S3 replied with error: {}",
                          ref->get_headers());
                    }
                    return drain_response_stream(std::move(ref))
                      .then([result](iobuf&& res) {
                          return parse_rest_error_response<
                            http::client::response_stream_ref>(
                            result, std::move(res));
                      });
                }
                return ss::make_ready_future<http::client::response_stream_ref>(
                  std::move(ref));
            });
      });
}

ss::future<result<s3_client::head_object_result, error_outcome>>
s3_client::head_object(
  bucket_name const& name,
  object_key const& key,
  const ss::lowres_clock::duration& timeout) {
    return send_request(do_head_object(name, key, timeout), name, key);
}

ss::future<s3_client::head_object_result> s3_client::do_head_object(
  bucket_name const& name,
  object_key const& key,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_head_object_request(name, key);
    if (!header) {
        return ss::make_exception_future<s3_client::head_object_result>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
      .then(
        [key](const http::client::response_stream_ref& ref)
          -> ss::future<head_object_result> {
            return ref->prefetch_headers().then(
              [ref, key]() -> ss::future<head_object_result> {
                  auto status = ref->get_headers().result();
                  if (status == boost::beast::http::status::not_found) {
                      vlog(
                        s3_log.debug,
                        "Object not available, error: {}",
                        ref->get_headers());
                      return parse_head_error_response<head_object_result>(
                        ref->get_headers(), key);
                  } else if (status != boost::beast::http::status::ok) {
                      vlog(
                        s3_log.warn,
                        "S3 replied with error: {}",
                        ref->get_headers());
                      return parse_head_error_response<head_object_result>(
                        ref->get_headers(), key);
                  }
                  auto len = boost::lexical_cast<uint64_t>(
                    ref->get_headers().at(
                      boost::beast::http::field::content_length));
                  auto etag = ref->get_headers().at(
                    boost::beast::http::field::etag);
                  head_object_result results{
                    .object_size = len,
                    .etag = ss::sstring(etag.data(), etag.length()),
                  };
                  return ss::make_ready_future<head_object_result>(
                    std::move(results));
              });
        })
      .handle_exception_type([this](const rest_error_response& err) {
          _probe->register_failure(err.code());
          return ss::make_exception_future<head_object_result>(err);
      });
}

ss::future<result<s3_client::no_response, error_outcome>> s3_client::put_object(
  bucket_name const& name,
  object_key const& key,
  size_t payload_size,
  ss::input_stream<char>&& body,
  const object_tag_formatter& tags,
  const ss::lowres_clock::duration& timeout) {
    return send_request(
      do_put_object(name, key, payload_size, std::move(body), tags, timeout)
        .then(
          []() { return ss::make_ready_future<no_response>(no_response{}); }),
      name,
      key);
}

ss::future<> s3_client::do_put_object(
  bucket_name const& name,
  object_key const& id,
  size_t payload_size,
  ss::input_stream<char>&& body,
  const object_tag_formatter& tags,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_unsigned_put_object_request(
      name, id, payload_size, tags);
    if (!header) {
        return body.close().then([header] {
            return ss::make_exception_future<>(
              std::system_error(header.error()));
        });
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return ss::do_with(
      std::move(body),
      [this, timeout, header = std::move(header)](
        ss::input_stream<char>& body) mutable {
          auto make_request = [this, &header, &body, &timeout]() {
              return _client.request(std::move(header.value()), body, timeout);
          };

          return ss::futurize_invoke(make_request)
            .then([](const http::client::response_stream_ref& ref) {
                return drain_response_stream(ref).then([ref](iobuf&& res) {
                    auto status = ref->get_headers().result();
                    if (status != boost::beast::http::status::ok) {
                        vlog(
                          s3_log.warn,
                          "S3 replied with error: {}",
                          ref->get_headers());
                        return parse_rest_error_response<>(
                          status, std::move(res));
                    }
                    return ss::now();
                });
            })
            .handle_exception_type(
              [](const ss::abort_requested_exception&) { return ss::now(); })
            .handle_exception_type([this](const rest_error_response& err) {
                _probe->register_failure(err.code());
                return ss::make_exception_future<>(err);
            })
            .handle_exception([](std::exception_ptr eptr) {
                vlog(s3_log.warn, "S3 request failed with error: {}", eptr);
                return ss::make_exception_future<>(eptr);
            })
            .finally([&body]() { return body.close(); });
      });
}

ss::future<result<s3_client::list_bucket_result, error_outcome>>
s3_client::list_objects(
  const bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  const ss::lowres_clock::duration& timeout) {
    return send_request(
      do_list_objects_v2(name, prefix, start_after, max_keys, timeout),
      name,
      object_key{""});
}

ss::future<s3_client::list_bucket_result> s3_client::do_list_objects_v2(
  const bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_list_objects_v2_request(
      name, std::move(prefix), std::move(start_after), max_keys);
    if (!header) {
        return ss::make_exception_future<list_bucket_result>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
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
                          vlog(
                            s3_log.warn, "S3 replied with error: {}", header);
                          return parse_rest_error_response<
                            s3_client::list_bucket_result>(
                            header.result(), std::move(outbuf));
                      }
                      auto res = iobuf_to_list_bucket_result(std::move(outbuf));
                      return ss::make_ready_future<list_bucket_result>(
                        std::move(res));
                  });
            });
      });
}

ss::future<result<s3_client::no_response, error_outcome>>
s3_client::delete_object(
  const bucket_name& bucket,
  const object_key& key,
  const ss::lowres_clock::duration& timeout) {
    using ret_t = result<s3_client::no_response, error_outcome>;

    return send_request(
             do_delete_object(bucket, key, timeout).then([] {
                 return ss::make_ready_future<no_response>(no_response{});
             }),
             bucket,
             key)
      .then([&bucket, &key](const ret_t& result) {
          // Google's implementation of S3 returns a NoSuchKey error
          // when the object to be deleted does not exist. We return
          // no_response{} in this case in order to get the same behaviour of
          // AWS S3.
          //
          // TODO: Subclass cloud_storage_clients::client with a GCS client
          // implementation where this edge case is handled.
          if (!result && result.error() == error_outcome::key_not_found) {
              vlog(
                s3_log.debug,
                "Object to be deleted was not found in cloud storage: "
                "object={}, bucket={}. Ignoring ...",
                bucket,
                key);
              return ss::make_ready_future<ret_t>(no_response{});
          } else {
              return ss::make_ready_future<ret_t>(result);
          }
      });
}

ss::future<> s3_client::do_delete_object(
  const bucket_name& bucket,
  const object_key& key,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_delete_object_request(bucket, key);
    if (!header) {
        return ss::make_exception_future<>(std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
      .then([](const http::client::response_stream_ref& ref) {
          return drain_response_stream(ref).then([ref](iobuf&& res) {
              auto status = ref->get_headers().result();
              if (
                status != boost::beast::http::status::ok
                && status
                     != boost::beast::http::status::no_content) { // expect 204
                  vlog(
                    s3_log.warn,
                    "S3 replied with error: {}",
                    ref->get_headers());
                  return parse_rest_error_response<>(status, std::move(res));
              }
              return ss::now();
          });
      });
}

static auto iobuf_to_delete_objects_result(iobuf&& buf) {
    auto root = iobuf_to_ptree(std::move(buf));
    auto result = client::delete_objects_result{};
    try {
        for (auto const& [tag, value] : root.get_child("DeleteResult")) {
            if (tag != "Error") {
                continue;
            }
            auto code = value.get_optional<ss::sstring>("Code");
            auto key = value.get_optional<ss::sstring>("Key");
            auto message = value.get_optional<ss::sstring>("Message");
            auto version_id = value.get_optional<ss::sstring>("VersionId");
            vlog(
              s3_log.trace,
              R"(delete_objects_result::undeleted_keys Key:"{}" Code: "{}" Message:"{}" VersionId:"{}")",
              key.value_or("[no key present]"),
              code.value_or("[no error code present]"),
              message.value_or("[no error message present]"),
              version_id.value_or("[no version id present]"));
            if (key.has_value()) {
                result.undeleted_keys.push_back({
                  object_key{key.value()},
                  code.value_or("[no error code present]"),
                });
            } else {
                vlog(
                  s3_log.warn,
                  "an DeleteResult.Error does not contain the Key tag");
            }
        }
    } catch (...) {
        vlog(
          s3_log.error,
          "DeleteObjects response parse failed: {}",
          std::current_exception());
        throw;
    }
    return result;
}

auto s3_client::do_delete_objects(
  bucket_name const& bucket,
  std::span<const object_key> keys,
  ss::lowres_clock::duration timeout)
  -> ss::future<client::delete_objects_result> {
    auto request = _requestor.make_delete_objects_request(bucket, keys);
    if (!request) {
        return ss::make_exception_future<delete_objects_result>(
          std::system_error(request.error()));
    }
    auto& [header, body] = request.value();
    vlog(s3_log.trace, "send DeleteObjects request:\n{}", header);

    return ss::do_with(
             std::move(body),
             [&_client = _client, header = std::move(header), timeout](
               auto& to_delete) mutable {
                 return _client.request(std::move(header), to_delete, timeout)
                   .finally([&] { return to_delete.close(); });
             })
      .then([](http::client::response_stream_ref const& response) {
          return drain_response_stream(response).then([response](iobuf&& res) {
              auto status = response->get_headers().result();
              if (status != boost::beast::http::status::ok) {
                  return parse_rest_error_response<delete_objects_result>(
                    status, std::move(res));
              }
              return ss::make_ready_future<delete_objects_result>(
                iobuf_to_delete_objects_result(std::move(res)));
          });
      });
}

auto s3_client::delete_objects(
  const bucket_name& bucket,
  std::vector<object_key> keys,
  ss::lowres_clock::duration timeout)
  -> ss::future<result<delete_objects_result, error_outcome>> {
    return send_request(
      do_delete_objects(bucket, keys, timeout), bucket, object_key{""});
}
} // namespace cloud_storage_clients
