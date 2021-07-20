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
#include "rpc/types.h"
#include "s3/error.h"
#include "s3/logger.h"
#include "s3/signature.h"
#include "ssx/sformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

#include <boost/beast/core/error.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <gnutls/crypto.h>

#include <exception>

namespace s3 {

struct aws_header_names {
    static constexpr boost::beast::string_view prefix = "prefix";
    static constexpr boost::beast::string_view start_after = "start-after";
    static constexpr boost::beast::string_view max_keys = "max-keys";
    static constexpr boost::beast::string_view x_amz_content_sha256
      = "x-amz-content-sha256";
    static constexpr boost::beast::string_view x_amz_tagging = "x-amz-tagging";
};

struct aws_header_values {
    static constexpr boost::beast::string_view user_agent
      = "redpanda.vectorized.io";
    static constexpr boost::beast::string_view text_plain = "text/plain";
};

// configuration //

static ss::sstring make_endpoint_url(
  const aws_region_name& region,
  const std::optional<endpoint_url>& url_override) {
    if (url_override) {
        return url_override.value();
    }
    return ssx::sformat("s3.{}.amazonaws.com", region());
}

ss::future<configuration> configuration::make_configuration(
  const public_key_str& pkey,
  const private_key_str& skey,
  const aws_region_name& region,
  const default_overrides& overrides,
  rpc::metrics_disabled disable_metrics) {
    configuration client_cfg;
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
        // picks the the ciphersuites with 128-bit ciphers which
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
            co_await cred_builder.set_system_trust();
        }
        client_cfg.credentials
          = co_await cred_builder.build_reloadable_certificate_credentials();
    }
    constexpr uint16_t default_port = 443;
    client_cfg.server_addr = unresolved_address(
      client_cfg.uri(),
      overrides.port ? *overrides.port : default_port,
      ss::net::inet_address::family::INET);
    client_cfg.disable_metrics = disable_metrics;
    client_cfg._probe = ss::make_shared<client_probe>(
      disable_metrics, region(), endpoint_uri);
    co_return client_cfg;
}

std::ostream& operator<<(std::ostream& o, const configuration& c) {
    o << "{access_key:" << c.access_key << ",region:" << c.region()
      << ",secret_key:****"
      << ",access_point_uri:" << c.uri() << ",server_addr:" << c.server_addr
      << "}";
    return o;
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
    auto target = fmt::format("/{}", key().string());
    std::string emptysig
      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    header.insert(aws_header_names::x_amz_content_sha256, emptysig);
    _sign.update_credentials_if_outdated();
    auto ec = _sign.sign_header(header, emptysig);
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
  const std::vector<object_tag>& tags) {
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
    if (!tags.empty()) {
        std::stringstream tstr;
        for (const auto& [key, val] : tags) {
            tstr << fmt::format("&{}={}", key, val);
        }
        header.insert(aws_header_names::x_amz_tagging, tstr.str().substr(1));
    }
    _sign.update_credentials_if_outdated();
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
        header.insert(aws_header_names::prefix, (*prefix)().string());
    }
    if (start_after) {
        header.insert(aws_header_names::start_after, (*start_after)().string());
    }
    if (max_keys) {
        header.insert(aws_header_names::start_after, std::to_string(*max_keys));
    }
    _sign.update_credentials_if_outdated();
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
    auto target = fmt::format("/{}", key().string());
    std::string emptysig
      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    header.insert(aws_header_names::x_amz_content_sha256, emptysig);
    _sign.update_credentials_if_outdated();
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
  , _client(conf)
  , _probe(conf._probe) {}

client::client(const configuration& conf, const ss::abort_source& as)
  : _requestor(conf)
  , _client(conf, &as, conf._probe)
  , _probe(conf._probe) {}

ss::future<> client::stop() { return _client.stop(); }

ss::future<> client::shutdown() {
    _client.shutdown();
    return ss::now();
}

ss::future<http::client::response_stream_ref> client::get_object(
  bucket_name const& name,
  object_key const& key,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_get_object_request(name, key);
    if (!header) {
        return ss::make_exception_future<http::client::response_stream_ref>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return _client.request(std::move(header.value()), timeout)
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
  ss::input_stream<char>&& body,
  const std::vector<object_tag>& tags,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_unsigned_put_object_request(
      name, id, payload_size, tags);
    if (!header) {
        return ss::make_exception_future<>(std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return ss::do_with(
      std::move(body),
      [this, timeout, header = std::move(header)](
        ss::input_stream<char>& body) mutable {
          return _client.request(std::move(header.value()), body, timeout)
            .then([](const http::client::response_stream_ref& ref) {
                return drain_response_stream(ref).then([ref](iobuf&& res) {
                    auto status = ref->get_headers().result();
                    if (status != boost::beast::http::status::ok) {
                        return parse_rest_error_response<>(std::move(res));
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
            .finally([&body]() { return body.close(); });
      });
}

ss::future<client::list_bucket_result> client::list_objects_v2(
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
    vlog(s3_log.trace, "send https request:\n{}", header);
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

ss::future<> client::delete_object(
  const bucket_name& bucket,
  const object_key& key,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_delete_object_request(bucket, key);
    if (!header) {
        return ss::make_exception_future<>(std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return _client.request(std::move(header.value()), timeout)
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

client_pool::client_pool(
  size_t size, configuration conf, client_pool_overdraft_policy policy)
  : _size(size)
  , _config(std::move(conf))
  , _policy(policy) {
    init();
}

ss::future<> client_pool::stop() {
    _as.request_abort();
    _cvar.broken();
    // Wait until all leased objects are returned
    co_await _gate.close();
}

/// \brief Acquire http client from the pool.
///
/// \note it's guaranteed that the client can only be acquired once
///       before it gets released (release happens implicitly, when
///       the lifetime of the pointer ends).
/// \return client pointer (via future that can wait if all clients
///         are in use)
ss::future<client_pool::client_lease> client_pool::acquire() {
    gate_guard guard(_gate);
    try {
        while (_pool.empty() && !_gate.is_closed()) {
            if (_policy == client_pool_overdraft_policy::wait_if_empty) {
                co_await _cvar.wait();
            } else {
                auto cl = ss::make_shared<client>(_config, _as);
                _pool.emplace_back(std::move(cl));
            }
        }
    } catch (const ss::broken_condition_variable&) {
    }
    if (_gate.is_closed() || _as.abort_requested()) {
        throw ss::gate_closed_exception();
    }
    vassert(!_pool.empty(), "'acquire' invariant is broken");
    auto client = _pool.back();
    _pool.pop_back();
    co_return client_lease{
      .client = client,
      .deleter = ss::make_deleter(
        [pool = weak_from_this(), client, g = std::move(guard)] {
            if (pool) {
                pool->release(client);
            }
        })};
}
size_t client_pool::size() const noexcept { return _pool.size(); }
void client_pool::init() {
    for (size_t i = 0; i < _size; i++) {
        auto cl = ss::make_shared<client>(_config, _as);
        _pool.emplace_back(std::move(cl));
    }
}
void client_pool::release(ss::shared_ptr<client> leased) {
    if (_pool.size() == _size) {
        return;
    }
    _pool.emplace_back(std::move(leased));
    _cvar.signal();
}

} // namespace s3
