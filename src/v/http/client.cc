// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"

#include "bytes/details/io_iterator_consumer.h"
#include "bytes/iobuf.h"
#include "http/logger.h"
#include "ssx/sformat.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/tls.hh>

#include <boost/beast/core/buffer_traits.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/core/impl/error.hpp>
#include <boost/system/detail/error_code.hpp>

#include <chrono>
#include <exception>
#include <limits>
#include <stdexcept>

namespace http {

// client implementation //
static constexpr ss::lowres_clock::duration default_max_idle_time = 1s;

client::client(const net::base_transport::configuration& cfg)
  : client(cfg, nullptr, nullptr, default_max_idle_time) {}

client::client(
  const net::base_transport::configuration& cfg, const ss::abort_source& as)
  : client(cfg, &as, nullptr, default_max_idle_time) {}

client::client(
  const net::base_transport::configuration& cfg,
  const ss::abort_source* as,
  ss::shared_ptr<client_probe> probe,
  ss::lowres_clock::duration max_idle_time)
  : net::base_transport(cfg)
  , _connect_gate()
  , _as(as)
  , _probe(std::move(probe))
  , _max_idle_time(max_idle_time) {
    if (!_probe) {
        _probe = ss::make_shared<client_probe>();
    }
}

void client::check() const {
    if (_as) {
        _as->check();
    }
}

ss::future<client::request_response_t> client::make_request(
  client::request_header&& header, ss::lowres_clock::duration timeout) {
    auto verb = header.method();
    auto target = header.target();
    ss::sstring target_str(target.data(), target.size());
    auto req = ss::make_shared<request_stream>(this, std::move(header));
    auto res = ss::make_shared<response_stream>(this, verb, target_str);
    prefix_logger ctxlog(http_log, ssx::sformat("[{}]", target_str));
    vlog(ctxlog.trace, "client.make_request {}", header);
    auto now = ss::lowres_clock::now();
    auto age = _last_response == ss::lowres_clock::time_point::min()
                 ? ss::lowres_clock::duration::max()
                 : now - _last_response;
    if (is_valid()) {
        if (age < _max_idle_time) {
            // Reuse connection
            vlog(ctxlog.debug, "reusing connection, age {}", age.count());
            return ss::make_ready_future<request_response_t>(
              std::make_tuple(req, res));
        } else {
            vlog(ctxlog.debug, "shutdown connection, age {}", age.count());
            // Connection is too old and likeley already received
            // RST packet from the server. If we will try to use
            // it the broken pipe (32) error will be triggered.
            shutdown();
        }
    }
    return get_connected(timeout, ctxlog)
      .then([req, res, target, ctxlog](reconnect_result_t r) {
          if (r == reconnect_result_t::timed_out) {
              vlog(
                ctxlog.warn,
                "make_request timed-out connection attempt {}",
                target);
              ss::timed_out_error err;
              return ss::make_exception_future<client::request_response_t>(err);
          }
          return ss::make_ready_future<request_response_t>(
            std::make_tuple(req, res));
      })
      .handle_exception_type([this](ss::tls::verification_error err) {
          return stop().then([err = std::move(err)] {
              return ss::make_exception_future<client::request_response_t>(err);
          });
      });
}

ss::future<reconnect_result_t> client::get_connected(
  ss::lowres_clock::duration timeout, prefix_logger ctxlog) {
    vlog(
      ctxlog.debug,
      "about to start connecting, {}, is-closed {}",
      is_valid(),
      _dispatch_gate.is_closed());
    auto current = ss::lowres_clock::now();
    const auto deadline = current + timeout;
    const auto interval = 1s; // 500ms;
    while (!_connect_gate.is_closed() && current < deadline) {
        if (_as != nullptr) {
            _as->check();
        }
        // Reconnect attempts have to stop if:
        // - shutdown method was called
        // - abort was requested
        // - unrecoverable error occured
        // - timeout reached
        try {
            // base_transport::connect calls _dispatcher_gate.close
            // on every reconnect. Because of that concurrent call
            // to base_transport::stop could lead to failure because
            // _dispatcher_gate is already closed. We need to synchronize
            // this loop with the `stop` call.
            ss::gate::holder gg(_connect_gate);
            co_await connect(current + interval);
            break;
        } catch (const std::system_error& err) {
            vlog(ctxlog.trace, "connection refused {}", err);
        } catch (const ss::timed_out_error&) {
            vlog(ctxlog.trace, "connection timeout");
        }
        current = ss::lowres_clock::now();
        // Any TLS error have to be propagated because it's not
        // transient. It won't help to try once again.
    }
    vlog(ctxlog.debug, "connected, {}", is_valid());
    co_return is_valid() ? reconnect_result_t::connected
                         : reconnect_result_t::timed_out;
}

ss::future<> client::stop() {
    co_await _connect_gate.close();
    // Can safely stop base_transport
    co_return co_await base_transport::stop();
}

void client::fail_outstanding_futures() noexcept { shutdown(); }

ss::future<ss::temporary_buffer<char>> client::receive() {
    return _in.read()
      .then([this](ss::temporary_buffer<char>&& tmpbuf) {
          _probe->add_inbound_bytes(tmpbuf.size());
          return std::move(tmpbuf);
      })
      .handle_exception([this](std::exception_ptr e) {
          _probe->register_transport_error();
          return ss::make_exception_future<ss::temporary_buffer<char>>(e);
      })
      .finally([this] { _last_response = ss::lowres_clock::now(); });
}

ss::future<> client::send(ss::scattered_message<char> msg) {
    _probe->add_outbound_bytes(msg.size());
    return _out.write(std::move(msg))
      .handle_exception([this](std::exception_ptr e) {
          _probe->register_transport_error();
          return ss::make_exception_future<>(e);
      });
}

// response_stream implementation //

static client_probe::verb convert_to_pverb(client::response_stream::verb v) {
    switch (v) {
    case client::response_stream::verb::get:
        return client_probe::verb::get;
    case client::response_stream::verb::put:
        return client_probe::verb::put;
    default:
        break;
    }
    return client_probe::verb::other;
}

client::response_stream::response_stream(
  client* client, client::response_stream::verb v, ss::sstring target)
  : _client(client)
  , _ctxlog(http_log, ssx::sformat("{{}}", std::move(target)))
  , _parser()
  , _buffer()
  , _sprobe(client->_probe->create_request_subprobe(convert_to_pverb(v))) {
    _parser.body_limit(std::numeric_limits<uint64_t>::max());
    _parser.eager(true);
}

bool client::response_stream::is_done() const {
    return _prefetch.empty() && _parser.is_done();
}

/// Return true if the header parsing is done
bool client::response_stream::is_header_done() const {
    return _parser.is_header_done();
}

/// Access response headers (should only be called if is_header_done() == true)
const client::response_header& client::response_stream::get_headers() const {
    return _parser.get();
}

/// Convert iobuf to const_buffer sequence
static std::vector<boost::asio::const_buffer>
iobuf_to_constbufseq(const iobuf& iobuf) {
    std::vector<boost::asio::const_buffer> seq;
    for (auto const& fragm : iobuf) {
        boost::asio::const_buffer cbuf{fragm.get(), fragm.size()};
        seq.push_back(cbuf);
    }
    return seq;
}

ss::future<> client::response_stream::shutdown() { return _client->stop(); }

/// Return failed future if ec is set, otherwise return future in ready state
static ss::future<iobuf>
fail_on_error(prefix_logger& ctxlog, const boost::beast::error_code& ec) {
    if (!ec.failed()) {
        return ss::make_ready_future<iobuf>(iobuf());
    }
    vlog(ctxlog.debug, "'{}' error triggered", ec);
    boost::system::system_error except(ec);
    return ss::make_exception_future<iobuf>(except);
}

ss::future<> client::response_stream::prefetch_headers() {
    if (is_header_done()) {
        return ss::now();
    }
    return ss::do_with(iobuf(), [this](iobuf& head) {
        return ss::do_until(
                 [this] { return is_header_done(); },
                 [this, &head] {
                     return recv_some().then(
                       [&head](iobuf buf) { head.append(std::move(buf)); });
                 })
          .then([this, &head] { _prefetch = std::move(head); });
    });
}

ss::future<iobuf> client::response_stream::recv_some() {
    _client->check();
    if (!_prefetch.empty()) {
        // This code will only be executed if 'prefetch_headers' was called. It
        // can only be called once.
        return ss::make_ready_future<iobuf>(std::move(_prefetch));
    }
    return _client->receive()
      .then([this](ss::temporary_buffer<char> chunk) mutable {
          vlog(_ctxlog.trace, "chunk received, chunk length {}", chunk.size());
          if (chunk.empty()) {
              // NOTE: to make the parser stop we need to use the 'put_eof'
              // method, because it will handle situation when the data is
              // incomplete (e.g. headers are not received yet).
              // If we will use the flag to indicate that the data is received
              // we'll have to manually check the headers and the last chunk
              // status.
              boost::beast::error_code ec;
              if (!_parser.is_header_done()) {
                  // We can't put EOF before all header bytes are received
                  ec = boost::beast::http::make_error_code(
                    boost::beast::http::error::short_read);
              } else {
                  _parser.put_eof(ec);
              }
              return fail_on_error(_ctxlog, ec);
          }
          _buffer.append(std::move(chunk));
          _parser.get().body().set_temporary_source(_buffer);
          // Feed the parser
          if (_parser.is_done()) {
              vlog(
                _ctxlog.error,
                "parser done, remaining input {}",
                _buffer.size_bytes());
              // this is an error, shouldn't get here if parser is done
              std::runtime_error err("received more data than expected");
              return ss::make_exception_future<iobuf>(err);
          }
          auto bufseq = iobuf_to_constbufseq(_buffer);
          boost::beast::error_code ec;
          size_t noctets = _parser.put(bufseq, ec);
          if (ec == boost::beast::http::error::need_more) {
              // The parser is in the eager mode. This means
              // that the data will be produced (iobuf_body::value_type::append
              // will be called) anyway. The parser won't cache any data
              // internally and produce partial results which is an expected
              // behaviour. Without eager mode the parser caches partial results
              // and returns 'need_more'. The data is produced after subsequent
              // 'put' call(s) is made.
              ec = {};
          }
          if (ec) {
              // Parser error, response doesn't make sence
              vlog(
                _ctxlog.error,
                "parser returned error-code {}, remaining bytes {}",
                ec,
                _buffer.size_bytes());
              _buffer.clear();
              return fail_on_error(_ctxlog, ec);
          }
          auto out = _parser.get().body().consume();
          _buffer.trim_front(noctets);
          if (!_buffer.empty()) {
              vlog(
                _ctxlog.trace,
                "not all consumed, noctets {}, input size {}, output size "
                "{}, ec {}",
                noctets,
                _buffer.size_bytes(),
                out.size_bytes(),
                ec);
          }
          return ss::make_ready_future<iobuf>(std::move(out));
      })
      .handle_exception_type([this](const ss::tls::verification_error& err) {
          _client->_probe->register_transport_error();
          return _client->stop().then(
            [err] { return ss::make_exception_future<iobuf>(err); });
      })
      .handle_exception_type([this](const std::system_error& ec) {
          vlog(_ctxlog.error, "receive error {}", ec);
          _client->shutdown();
          return ss::make_exception_future<iobuf>(ec);
      });
}

// request_stream implementation //

client::request_stream::request_stream(client* client, request_header&& hdr)
  : _client(client)
  , _ctxlog(http_log, ssx::sformat("{{}}", hdr.target()))
  , _request(std::move(hdr))
  , _serializer{_request}
  , _chunk_encode(true, max_chunk_size) {
    _serializer.split(true);
}

/// Visitor class to be used in conjunction with http_serializer
struct parser_visitor {
    template<class ConstBufferSeq>
    void operator()(boost::beast::error_code&, const ConstBufferSeq& seq) {
        // update outbuf with new data from 'seq'
        for (auto it = boost::asio::buffer_sequence_begin(seq);
             it != boost::asio::buffer_sequence_end(seq);
             it++) {
            // generic implementation for header buffers
            boost::asio::const_buffer buffer = *it;
            outbuf.append(
              static_cast<const uint8_t*>(buffer.data()), buffer.size());
        }
        serializer.consume(boost::asio::buffer_size(seq));
    }

    iobuf& outbuf;
    http_serializer& serializer;
};

ss::future<>
client::request_stream::send_some(ss::temporary_buffer<char>&& buf) {
    iobuf tmp;
    tmp.append(std::move(buf));
    return send_some(std::move(tmp));
}

ss::future<> client::request_stream::send_some(iobuf&& seq) {
    _client->check();
    vlog(_ctxlog.trace, "request_stream.send_some {}", seq.size_bytes());
    if (_serializer.is_header_done()) {
        // Fast path
        return ss::with_gate(_gate, [this, seq = std::move(seq)]() mutable {
            vlog(_ctxlog.trace, "header is done, bypass protocol serializer");
            return forward(_client, _chunk_encode(std::move(seq)));
        });
    }
    // Deal with the header first
    boost::beast::error_code error_code = {};
    iobuf outbuf;
    parser_visitor visitor{outbuf, _serializer};
    _serializer.next(error_code, visitor);
    if (error_code) {
        vlog(_ctxlog.error, "serialization error {}", error_code);
        boost::system::system_error except(error_code);
        return ss::make_exception_future<>(except);
    }
    auto scattered = iobuf_as_scattered(std::move(outbuf));
    return ss::with_gate(
      _gate,
      [this, seq = std::move(seq), scattered = std::move(scattered)]() mutable {
          return _client->send(std::move(scattered))
            .then([this, seq = std::move(seq)]() mutable {
                return forward(_client, _chunk_encode(std::move(seq)));
            })
            .handle_exception_type(
              [this](const ss::tls::verification_error& err) {
                  return _client->stop().then(
                    [err] { return ss::make_exception_future<>(err); });
              })
            .handle_exception_type([this](const std::system_error& ec) {
                vlog(_ctxlog.error, "send error {}", ec);
                _client->shutdown();
                return ss::make_exception_future<>(ec);
            });
      });
}

bool client::request_stream::is_done() { return _serializer.is_done(); }

// Wait until remaining data will be transmitted
ss::future<> client::request_stream::send_eof() { return _gate.close(); }

static ss::temporary_buffer<char> iobuf_as_tmpbuf(iobuf&& buf) {
    if (buf.begin() == buf.end()) {
        // empty buf
        return ss::temporary_buffer<char>();
    }
    auto full_size_bytes = buf.size_bytes();
    if (auto it = buf.begin(); full_size_bytes == it->size()) {
        // fast path
        return std::move(*it).release();
    }
    // linearize the iobuf
    auto consumer = iobuf::iterator_consumer(buf.begin(), buf.end());
    ss::temporary_buffer<char> res(full_size_bytes);
    consumer.consume_to(full_size_bytes, res.get_write());
    return res;
}
/// Represents response body as a data source for ss::input_stream
struct response_data_source final : ss::data_source_impl {
    explicit response_data_source(client::response_stream_ref resp)
      : _io(std::move(resp)) {}
    ss::future<> close() final {
        _done = true;
        return ss::now();
    }
    ss::future<ss::temporary_buffer<char>> skip(uint64_t n) final {
        _skip += n;
        return get();
    }
    ss::future<ss::temporary_buffer<char>> get() final {
        return ss::do_with(
          ss::temporary_buffer<char>(),
          [this](ss::temporary_buffer<char>& result) {
              return ss::repeat([this, &result] {
                         if (_done || _io->is_done()) {
                             return ss::make_ready_future<ss::stop_iteration>(
                               ss::stop_iteration::yes);
                         }
                         return _io->recv_some().then([this, &result](
                                                        iobuf&& bufseq) {
                             if (_skip) {
                                 auto n = std::min(bufseq.size_bytes(), _skip);
                                 bufseq.trim_front(n);
                                 _skip -= n;
                             }
                             if (bufseq.begin() == bufseq.end()) {
                                 return ss::make_ready_future<
                                   ss::stop_iteration>(
                                   _io->is_done() ? ss::stop_iteration::yes
                                                  : ss::stop_iteration::no);
                             }
                             result = iobuf_as_tmpbuf(std::move(bufseq));
                             return ss::make_ready_future<ss::stop_iteration>(
                               ss::stop_iteration::yes);
                         });
                     })
                .then([&result] {
                    return ss::make_ready_future<ss::temporary_buffer<char>>(
                      std::move(result));
                });
          });
    }
    client::response_stream_ref _io;
    size_t _skip{0};
    bool _done{false};
};
struct request_data_sink final : ss::data_sink_impl {
    explicit request_data_sink(client::request_stream_ref req)
      : _io(std::move(req)) {}
    ss::future<> put(ss::net::packet data) final { return put(data.release()); }
    ss::future<> put(std::vector<ss::temporary_buffer<char>> all) final {
        return ss::do_with(
          std::move(all), [this](std::vector<ss::temporary_buffer<char>>& all) {
              return ss::do_for_each(
                all, [this](ss::temporary_buffer<char>& buf) {
                    return put(std::move(buf));
                });
          });
    }
    ss::future<> put(ss::temporary_buffer<char> buf) final {
        return _io->send_some(std::move(buf));
    }
    ss::future<> flush() final { return ss::now(); }
    ss::future<> close() final { return _io->send_eof(); }
    client::request_stream_ref _io;
};

ss::future<client::response_stream_ref> client::request(
  client::request_header&& header,
  ss::input_stream<char>& input,
  ss::lowres_clock::duration timeout) {
    bool empty_input_stream = false;
    auto plen = header.find(boost::beast::http::field::content_length);
    if (plen != header.cend()) {
        empty_input_stream = plen->value() == "0";
    }
    return make_request(std::move(header), timeout)
      .then([&input, empty_input_stream](request_response_t reqresp) mutable {
          auto [request, response] = std::move(reqresp);
          auto fsend = ss::now();
          if (empty_input_stream) {
              // special case - empty input stream, don't use ss::copy
              // since it won't invoke the underlying implementation
              fsend = request->send_some(iobuf()).then(
                [request = request]() { return request->send_eof(); });
          } else {
              // since the input stream has some data we can use
              // output_stream interace of the request
              fsend = ss::do_with(
                request->as_output_stream(),
                [&input](ss::output_stream<char>& output) {
                    return ss::copy(input, output).then([&output] {
                        return output.close();
                    });
                });
          }
          return fsend.then([response = response]() {
              return ss::make_ready_future<response_stream_ref>(response);
          });
      });
}

ss::future<client::response_stream_ref> client::request(
  client::request_header&& header, ss::lowres_clock::duration timeout) {
    return make_request(std::move(header), timeout)
      .then([](request_response_t reqresp) mutable {
          auto [request, response] = std::move(reqresp);
          return request->send_some(iobuf())
            .then([request = request]() { return request->send_eof(); })
            .then([response = response] {
                return ss::make_ready_future<response_stream_ref>(response);
            });
      });
}

ss::output_stream<char> client::request_stream::as_output_stream() {
    auto ds = ss::data_sink(
      std::make_unique<request_data_sink>(shared_from_this()));
    return ss::output_stream<char>(std::move(ds), max_chunk_size);
}

ss::input_stream<char> client::response_stream::as_input_stream() {
    auto ds = ss::data_source(
      std::make_unique<response_data_source>(shared_from_this()));
    return ss::input_stream<char>(std::move(ds));
}

} // namespace http
