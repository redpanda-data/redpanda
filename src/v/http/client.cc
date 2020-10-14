#include "http/client.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <boost/beast/core/buffer_traits.hpp>

namespace http {

// client implementation //

client::client(const rpc::base_transport::configuration& cfg)
  : rpc::base_transport(cfg) {}

ss::future<client::request_response_t>
client::make_request(client::request_header&& header) {
    vlog(http_log.trace, "client.make_request {}", header);
    auto req = ss::make_shared<request_stream>(this, std::move(header));

    auto res = ss::make_shared<response_stream>(this);
    if (is_valid()) {
        // already connected
        return ss::make_ready_future<request_response_t>(
          std::make_tuple(req, res));
    }
    return connect().then([req, res] {
        return ss::make_ready_future<request_response_t>(
          std::make_tuple(req, res));
    });
}

// response_stream implementation //

client::response_stream::response_stream(client* client)
  : _client(client)
  , _parser()
  , _buffer() {
    _parser.eager(true);
}

bool client::response_stream::is_done() const { return _parser.is_done(); }

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

ss::future<iobuf> client::response_stream::recv_some() {
    return ss::do_with(iobuf(), [this](iobuf& result) {
        return do_recv_some(result).then([&result]() {
            return ss::make_ready_future<iobuf>(std::move(result));
        });
    });
}

/// Return failed future if ec is set, otherwise return future in ready state
static ss::future<> fail_on_error(const boost::beast::error_code& ec) {
    if (!ec) {
        return ss::make_ready_future<>();
    }
    vlog(http_log.error, "'{}' error triggered", ec);
    boost::system::system_error except(ec);
    return ss::make_exception_future<>(except);
}

ss::future<> client::response_stream::do_recv_some(iobuf& result) {
    return _client->_in.read().then(
      [this, &result](ss::temporary_buffer<char> chunk) mutable {
          vlog(http_log.trace, "chunk received, chunk length {}", chunk.size());
          if (chunk.empty()) {
              // NOTE: to make the parser stop we need to use the 'put_eof'
              // method, because it will handle situation when the data is
              // incomplete (e.g. headers are not received yet).
              // If we will use the flag to indicate that the data is received
              // we'll have to manually check the headers and the last chunk
              // status.
              boost::beast::error_code ec;
              _parser.put_eof(ec);
              return fail_on_error(ec);
          }
          _buffer.append(std::move(chunk));
          // Feed the parser
          if (_parser.is_done()) {
              vlog(
                http_log.error,
                "parser done, remaining input {}",
                _buffer.size_bytes());
              // this is an error, shouldn't get here if parser is done
              std::runtime_error err("received more data than expected");
              return ss::make_exception_future<>(err);
          }
          auto bufseq = iobuf_to_constbufseq(_buffer);
          boost::beast::error_code ec;
          size_t noctets = _parser.put(bufseq, ec);
          if (ec == boost::beast::http::error::need_more) {
              // The only way to progress for the parser is to add data to
              // the _buffer which means that we have to read the socket
              // next
              vlog(
                http_log.trace,
                "need_more, noctents {}, size {}, ec {}",
                noctets,
                _buffer.size_bytes(),
                ec);
              return ss::make_ready_future<>();
          }
          if (ec) {
              // Parser error, response doesn't make sence
              vlog(
                http_log.error,
                "parser returned error-code {}, remaining bytes {}",
                ec,
                _buffer.size_bytes());
              _buffer.clear();
              boost::system::system_error error(ec);
              return ss::make_exception_future<>(std::move(error));
          }
          auto out = _parser.get().body().consume(_buffer, noctets);

          result.append(std::move(out));
          _buffer.trim_front(noctets);
          if (!_buffer.empty()) {
              vlog(
                http_log.trace,
                "not all consumed, noctets {}, input size {}, output size "
                "{}, ec {}",
                noctets,
                _buffer.size_bytes(),
                result.size_bytes(),
                ec);
          }
          return ss::make_ready_future<>();
      });
}

// request_stream implementation //

client::request_stream::request_stream(client* client, request_header&& hdr)
  : _client(client)
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

ss::future<> client::request_stream::send_some(iobuf&& seq) {
    vlog(http_log.trace, "request_stream.send_sone {}", seq.size_bytes());
    if (_serializer.is_header_done()) {
        // Fast path
        return ss::with_gate(_gate, [this, seq = std::move(seq)]() mutable {
            vlog(http_log.trace, "header is done, bypass protocol serializer");
            return forward(_client->_out, _chunk_encode(std::move(seq)));
        });
    }
    // Deal with the header first
    boost::beast::error_code error_code = {};
    iobuf outbuf;
    parser_visitor visitor{outbuf, _serializer};
    _serializer.next(error_code, visitor);
    if (error_code) {
        vlog(http_log.error, "serialization error {}", error_code);
        boost::system::system_error except(error_code);
        return ss::make_exception_future<>(except);
    }
    auto scattered = iobuf_as_scattered(std::move(outbuf));
    return ss::with_gate(
      _gate,
      [this, seq = std::move(seq), scattered = std::move(scattered)]() mutable {
          return _client->_out.write(std::move(scattered))
            .then([this, seq = std::move(seq)]() mutable {
                return forward(_client->_out, _chunk_encode(std::move(seq)));
            });
      });
}

bool client::request_stream::is_done() { return _serializer.is_done(); }

// Wait until remaining data will be transmitted
ss::future<> client::request_stream::send_eof() { return _gate.close(); }

} // namespace http