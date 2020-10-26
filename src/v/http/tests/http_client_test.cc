#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "http/chunk_encoding.h"
#include "http/client.h"
#include "rpc/transport.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/routes.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tcp.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/beast/http/field.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <exception>
#include <optional>

static const uint16_t httpd_port_number = 8128;
static const char* httpd_host_name = "127.0.0.1";
static const char* httpd_server_reply
  = "The Hypertext Transfer Protocol (HTTP) is an "
    "application-level protocol "
    "for distributed, collaborative, hypermedia "
    "information systems. It is a "
    "generic, stateless, protocol which can be used "
    "for many tasks beyond its "
    "use for hypertext, such as name servers and "
    "distributed object management "
    "systems, through extension of its request "
    "methods, error codes and headers "
    "[47]. A feature of HTTP is the typing and "
    "negotiation of data representation, "
    "allowing systems to be built independently of "
    "the data being transferred.";

void set_routes(ss::httpd::routes& r) {
    using namespace ss::httpd;
    auto echo_handler = new function_handler(
      [](const_req req) { return req.content; });
    r.add(operation_type::POST, url("/echo"), echo_handler);

    auto fail_handler = new function_handler([](const_req req) -> ss::sstring {
        throw std::runtime_error(req.get_url());
    });
    r.add(operation_type::GET, url("/fail-status-500"), fail_handler);

    auto get_handler = new function_handler(
      [](const_req) -> ss::sstring { return httpd_server_reply; });
    r.add(operation_type::GET, url("/get"), get_handler);

    auto put_handler = new function_handler([](const_req req) {
        // Return empty body
        BOOST_REQUIRE_EQUAL(ss::sstring(httpd_server_reply), req.content);
        return ss::sstring();
    });
    r.add(operation_type::PUT, url("/put"), put_handler);
}

/// Http server and client
struct configured_test_pair {
    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::shared_ptr<http::client> client;
};

rpc::base_transport::configuration transport_configuration() {
    ss::ipv4_addr ip_addr = {httpd_host_name, httpd_port_number};
    ss::socket_address server_addr(ip_addr);
    rpc::base_transport::configuration conf{.server_addr = server_addr};
    return conf;
}

/// Create server and client, server is initialized with default
/// testing paths and listening.
configured_test_pair
started_client_and_server(const rpc::base_transport::configuration& conf) {
    auto client = ss::make_shared<http::client>(conf);
    auto server = ss::make_shared<ss::httpd::http_server_control>();
    server->start().get();
    server->set_routes(set_routes).get();
    server->listen(conf.server_addr).get();
    return {
      .server = server,
      .client = client,
    };
}

/// Test success path (should run in ss::async)
template<class Func>
void test_http_request(
  const rpc::base_transport::configuration& conf,
  http::client::request_header&& header,
  std::optional<ss::sstring> request_data,
  const Func& check_reply) {
    auto [server, client] = started_client_and_server(conf);
    auto [req_stream, resp_stream]
      = client->make_request(std::move(header)).get0();

    // Send request
    iobuf body;
    if (request_data) {
        body.append(request_data->data(), request_data->size());
    }
    req_stream->send_some(std::move(body)).get();
    req_stream->send_eof().get();

    // Receive response
    iobuf response_body;
    while (!resp_stream->is_done()) {
        iobuf res = resp_stream->recv_some().get0();
        response_body.append(std::move(res));
    }

    // Check response
    check_reply(resp_stream->get_headers(), std::move(response_body));

    server->stop().get();
}

SEASTAR_TEST_CASE(test_http_POST_roundtrip) {
    return ss::async([] {
        auto config = transport_configuration();
        http::client::request_header header;
        header.method(boost::beast::http::verb::post);
        header.target("/echo");
        header.insert(
          boost::beast::http::field::content_length,
          std::strlen(httpd_server_reply));
        header.insert(boost::beast::http::field::host, config.server_addr);
        header.insert(
          boost::beast::http::field::content_type, "application/json");
        test_http_request(
          config,
          std::move(header),
          ss::sstring(httpd_server_reply),
          [](http::client::response_header const& header, iobuf&& body) {
              BOOST_REQUIRE_EQUAL(
                header.result(), boost::beast::http::status::ok);

              iobuf_parser parser(std::move(body));
              std::string actual = parser.read_string(parser.bytes_left());
              std::string expected
                = "\"" + std::string(httpd_server_reply)
                  + "\""; // sestar will return json string containing the
              BOOST_REQUIRE_EQUAL(expected, actual);
          });
    });
}

SEASTAR_TEST_CASE(test_error_500) {
    return ss::async([] {
        auto config = transport_configuration();
        http::client::request_header header;
        header.method(boost::beast::http::verb::get);
        header.target("/fail-status-500");
        header.insert(boost::beast::http::field::host, config.server_addr);
        header.insert(
          boost::beast::http::field::content_type, "application/json");
        test_http_request(
          config,
          std::move(header),
          std::nullopt,
          [](http::client::response_header const& header, iobuf&& body) {
              BOOST_REQUIRE_EQUAL(
                header.result(),
                boost::beast::http::status::internal_server_error);
              iobuf_parser parser(std::move(body));
              std::string actual = parser.read_string(parser.bytes_left());
              BOOST_REQUIRE(
                actual.find("/fail-status-500") != std::string::npos);
          });
    });
}

SEASTAR_TEST_CASE(test_http_GET_roundtrip) {
    // No request data
    return ss::async([] {
        auto config = transport_configuration();
        http::client::request_header header;
        header.method(boost::beast::http::verb::get);
        header.target("/get");
        header.insert(boost::beast::http::field::host, config.server_addr);
        header.insert(
          boost::beast::http::field::content_type, "application/json");
        test_http_request(
          config,
          std::move(header),
          std::nullopt,
          [](http::client::response_header const& header, iobuf&& body) {
              BOOST_REQUIRE_EQUAL(
                header.result(), boost::beast::http::status::ok);
              iobuf_parser parser(std::move(body));
              std::string actual = parser.read_string(parser.bytes_left());
              std::string expected = "\"" + std::string(httpd_server_reply)
                                     + "\"";
              BOOST_REQUIRE_EQUAL(expected, actual);
          });
    });
}

SEASTAR_TEST_CASE(test_http_PUT_roundtrip) {
    // Send data and recv empty response
    return ss::async([] {
        auto config = transport_configuration();
        http::client::request_header header;
        header.method(boost::beast::http::verb::put);
        header.target("/put");
        header.insert(
          boost::beast::http::field::content_length,
          std::strlen(httpd_server_reply));
        header.insert(boost::beast::http::field::host, config.server_addr);
        header.insert(
          boost::beast::http::field::content_type, "application/json");
        test_http_request(
          config,
          std::move(header),
          ss::sstring(httpd_server_reply),
          [](http::client::response_header const& header, iobuf&& body) {
              BOOST_REQUIRE_EQUAL(
                header.result(), boost::beast::http::status::ok);
              iobuf_parser parser(std::move(body));
              std::string actual = parser.read_string(parser.bytes_left());
              std::string expected = "\"\"";
              BOOST_REQUIRE_EQUAL(expected, actual);
          });
    });
}

/// Simple tcp server that can receive pre-defined request and
/// reply with pre-defined response.
/// Seastar.Httpd doesn't support chunked encoding at the moment
/// so to test chunked encoding parsing end-to-end we can simulate
/// http server using this class.
/// Supposed to be used with async.
class http_server_impostor {
public:
    http_server_impostor(ss::sstring req, ss::sstring resp)
      : _socket()
      , _expected_data(std::move(req))
      , _response(std::move(resp)) {}

    void listen(ss::socket_address server_addr) {
        auto conf = transport_configuration();
        ss::server_socket ss;
        ss::listen_options lo{};
        lo.reuse_address = true;
        _server_socket = ss::engine().listen(server_addr, lo);
        (void)ss::with_gate(_gate, [this] {
            return ss::async([this] {
                auto [connection, remoteaddr] = _server_socket.accept().get0();
                _socket = std::move(connection);
                _fin = _socket.input();
                _fout = _socket.output();
                // read request and send response
                _socket.set_nodelay(true);
                _socket.set_keepalive(true);
                iobuf request = do_read_request();
                do_send_response();
            });
        });
    }

    void stop() {
        _server_socket.abort_accept();
        _socket.shutdown_input();
        _socket.shutdown_output();
        _gate.close().get();
    }

private:
    iobuf do_read_request() {
        // Read until _expected_data is fetched
        iobuf buffer;
        int it = 0;
        const int max_iter = 100;
        while (it++ < max_iter) {
            auto tmpbuf = _fin.read().get0();
            buffer.append(std::move(tmpbuf));
            if (buffer.size_bytes() > _expected_data.size()) {
                iobuf_parser parser(buffer.copy());
                ss::sstring body = parser.read_string(parser.bytes_left());
                if (body.find(_expected_data) != ss::sstring::npos) {
                    _fin.close().get();
                    return std::move(buffer);
                }
            }
        }
        throw std::runtime_error("Can't read request body");
    }

    void do_send_response() {
        _fout.write(_response).get();
        _fout.flush().get();
        _fout.close().get();
    }

    ss::server_socket _server_socket;
    ss::connected_socket _socket;
    ss::input_stream<char> _fin;
    ss::output_stream<char> _fout;
    ss::sstring _expected_data;
    ss::sstring _response;
    ss::gate _gate;
};

struct impostor_test_pair {
    ss::shared_ptr<http_server_impostor> server;
    ss::shared_ptr<http::client> client;
};

/// Create server and client, server is initialized with default
/// testing paths and listening.
impostor_test_pair started_client_and_impostor(
  const rpc::base_transport::configuration& conf,
  ss::sstring request_data,
  ss::sstring response_data) {
    auto client = ss::make_shared<http::client>(conf);
    auto server = ss::make_shared<http_server_impostor>(
      request_data, response_data);
    server->listen(conf.server_addr);
    return {
      .server = server,
      .client = client,
    };
}

ss::sstring get_response_header(const http::client::response_header& resp_hdr) {
    std::stringstream fmt;
    fmt << resp_hdr;
    return fmt.str();
}

template<class OKFunc, class ErrFunc = std::function<void(std::exception_ptr)>>
void test_impostor_request(
  const rpc::base_transport::configuration& conf,
  http::client::request_header&& header,
  const ss::sstring& request_data,
  const ss::sstring& response_data,
  const OKFunc& check_reply,
  const ErrFunc check_error = &std::rethrow_exception) {
    auto [server, client] = started_client_and_impostor(
      conf, request_data, response_data);
    auto [req_stream, resp_stream]
      = client->make_request(std::move(header)).get0();

    bool failure_occured = false;
    iobuf body;
    iobuf response_body;
    try {
        // Send request
        body.append(request_data.data(), request_data.size());
        req_stream->send_some(std::move(body)).get();
        req_stream->send_eof().get();

        // Receive response
        while (!resp_stream->is_done()) {
            iobuf res = resp_stream->recv_some().get0();
            response_body.append(std::move(res));
        }
    } catch (...) {
        failure_occured = true;
        check_error(std::current_exception());
    }

    // Check response
    if (!failure_occured) {
        check_reply(resp_stream->get_headers(), std::move(response_body));
    }

    server->stop();
}

SEASTAR_TEST_CASE(test_http_via_impostor) {
    // Send data and recv response
    return ss::async([] {
        auto config = transport_configuration();

        // Generate client request
        http::client::request_header request_header;
        request_header.method(boost::beast::http::verb::post);
        request_header.target("/");
        request_header.insert(
          boost::beast::http::field::content_length,
          std::strlen(httpd_server_reply));
        request_header.insert(
          boost::beast::http::field::host, config.server_addr);
        request_header.insert(
          boost::beast::http::field::content_type, "application/json");

        // Generate server response
        ss::sstring response_data = httpd_server_reply;
        http::client::response_header resp_hdr;
        resp_hdr.result(boost::beast::http::status::ok);
        resp_hdr.insert(boost::beast::http::field::host, config.server_addr);
        resp_hdr.insert(
          boost::beast::http::field::content_length, response_data.size());
        auto full_response = get_response_header(resp_hdr) + response_data;

        // Run test case
        test_impostor_request(
          config,
          std::move(request_header),
          ss::sstring(httpd_server_reply),
          full_response,
          [](http::client::response_header const& header, iobuf&& body) {
              BOOST_REQUIRE_EQUAL(
                header.result(), boost::beast::http::status::ok);
              iobuf_parser parser(std::move(body));
              std::string actual = parser.read_string(parser.bytes_left());
              std::string expected = ss::sstring(httpd_server_reply);
              BOOST_REQUIRE_EQUAL(expected, actual);
          });
    });
}

SEASTAR_TEST_CASE(test_http_via_impostor_incorrect_reply) {
    // Send data and recv error message
    return ss::async([] {
        auto config = transport_configuration();

        // Client request
        http::client::request_header request_header;
        request_header.method(boost::beast::http::verb::post);
        request_header.target("/");
        request_header.insert(
          boost::beast::http::field::content_length,
          std::strlen(httpd_server_reply));
        request_header.insert(
          boost::beast::http::field::host, config.server_addr);
        request_header.insert(
          boost::beast::http::field::content_type, "application/json");

        // Generate server response
        ss::sstring full_response = "not-correct-http-reply";

        test_impostor_request(
          config,
          std::move(request_header),
          ss::sstring(httpd_server_reply),
          full_response,
          [](http::client::response_header const&, iobuf&&) {
              BOOST_FAIL("Exception expected");
          },
          [](std::exception_ptr const& eptr) {
              try {
                  std::rethrow_exception(eptr);
              } catch (boost::system::system_error const& e) {
                  BOOST_REQUIRE(
                    e.code() == boost::beast::http::error::bad_version);
              }
          });
    });
}

SEASTAR_TEST_CASE(test_http_via_impostor_chunked_encoding) {
    // Send data and recv chunked response
    return ss::async([] {
        auto config = transport_configuration();

        // Generate request
        http::client::request_header request_header;
        request_header.method(boost::beast::http::verb::post);
        request_header.target("/");
        request_header.insert(
          boost::beast::http::field::content_length,
          std::strlen(httpd_server_reply));
        request_header.insert(
          boost::beast::http::field::host, config.server_addr);
        request_header.insert(
          boost::beast::http::field::content_type, "application/json");

        // Generate response
        http::chunked_encoder encoder{false};
        ss::sstring response_data = httpd_server_reply;
        http::client::response_header resp_hdr;
        resp_hdr.result(boost::beast::http::status::ok);
        resp_hdr.insert(
          boost::beast::http::field::transfer_encoding, "chunked");
        resp_hdr.insert(boost::beast::http::field::host, config.server_addr);
        auto header_str = get_response_header(resp_hdr);
        iobuf outbuf;
        outbuf.append(header_str.data(), header_str.size());
        iobuf bodybuf;
        bodybuf.append(response_data.data(), response_data.size());
        outbuf.append(encoder.encode(std::move(bodybuf)));
        outbuf.append(encoder.encode_eof());
        iobuf_parser bufparser(std::move(outbuf));

        test_impostor_request(
          config,
          std::move(request_header),
          ss::sstring(httpd_server_reply),
          bufparser.read_string(bufparser.bytes_left()),
          [](http::client::response_header const& header, iobuf&& body) {
              BOOST_REQUIRE_EQUAL(
                header.result(), boost::beast::http::status::ok);
              iobuf_parser parser(std::move(body));
              std::string actual = parser.read_string(parser.bytes_left());
              std::string expected = ss::sstring(httpd_server_reply);
              BOOST_REQUIRE_EQUAL(expected, actual);
          });
    });
}

SEASTAR_TEST_CASE(test_http_via_impostor_no_content_length) {
    // Send data and recv response (no content length header)
    return ss::async([] {
        auto config = transport_configuration();

        // Generate client request
        http::client::request_header request_header;
        request_header.method(boost::beast::http::verb::post);
        request_header.target("/");
        request_header.insert(
          boost::beast::http::field::content_length,
          std::strlen(httpd_server_reply));
        request_header.insert(
          boost::beast::http::field::host, config.server_addr);
        request_header.insert(
          boost::beast::http::field::content_type, "application/json");

        // Generate server response
        ss::sstring response_data = httpd_server_reply;
        http::client::response_header resp_hdr;
        resp_hdr.result(boost::beast::http::status::ok);
        resp_hdr.insert(boost::beast::http::field::host, config.server_addr);
        auto full_response = get_response_header(resp_hdr) + response_data;

        // Run test case
        test_impostor_request(
          config,
          std::move(request_header),
          ss::sstring(httpd_server_reply),
          full_response,
          [](http::client::response_header const& header, iobuf&& body) {
              // Expect normal reply despite the absence of content-length
              // header
              BOOST_REQUIRE_EQUAL(
                header.result(), boost::beast::http::status::ok);
              iobuf_parser parser(std::move(body));
              std::string actual = parser.read_string(parser.bytes_left());
              std::string expected = ss::sstring(httpd_server_reply);
              BOOST_REQUIRE_EQUAL(expected, actual);
          });
    });
}