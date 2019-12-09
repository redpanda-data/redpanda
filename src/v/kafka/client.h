#include "kafka/requests/api_versions_request.h"
#include "kafka/requests/fetch_request.h"
#include "kafka/server.h"
#include "rpc/client.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

namespace kafka {

/**
 * \brief Kafka client.
 *
 * Restrictions:
 *  - don't dispatch concurrent requests.
 */
class client : public rpc::base_client {
private:
    /*
     * send a request message and process the reply. note that the kafka
     * protocol requires that replies be sent in the same order they are
     * received at the server. in a future version of this client we will also
     * want to do some form of request/response correlation so that multiple
     * requests can be in flight.
     */
    template<typename Func>
    auto send_recv(Func&& func) {
        // size prefixed buffer for request
        iobuf buf;
        auto ph = buf.reserve(sizeof(int32_t));
        auto start_size = buf.size_bytes();
        response_writer wr(buf);

        // encode request
        func(wr);

        // finalize by filling in the size prefix
        int32_t total_size = buf.size_bytes() - start_size;
        auto be_total_size = cpu_to_be(total_size);
        auto* raw_size = reinterpret_cast<const char*>(&be_total_size);
        ph.write(raw_size, sizeof(be_total_size));

        return _out.write(iobuf_as_scattered(std::move(buf))).then([this] {
            return _in.read_exactly(sizeof(kafka::size_type))
              .then([this](temporary_buffer<char> buf) {
                  auto size = kafka::kafka_server::connection::process_size(
                    _in, std::move(buf));
                  return _in.read_exactly(sizeof(correlation_type))
                    .then([this, size](temporary_buffer<char> buf) {
                        // drops the correlation id on the floor
                        auto remaining = size - sizeof(correlation_type);
                        return read_iobuf_exactly(_in, remaining);
                    });
              });
        });
    }

public:
    using rpc::base_client::base_client;

    future<api_versions_response> api_versions() {
        return send_recv([this](response_writer& wr) mutable {
                   // just the header: the api version request is empty
                   write_header(wr, api_versions_api::key, api_version(0));
               })
          .then([](iobuf buf) {
              api_versions_response r;
              r.decode(std::move(buf), api_version(0));
              return r;
          });
    }

    future<fetch_response> fetch(fetch_request r) {
        return send_recv([this, r = std::move(r)](response_writer& wr) mutable {
                   write_header(wr, fetch_api::key, api_version(4));
                   r.encode(wr, api_version(4));
               })
          .then([](iobuf buf) {
              fetch_response r;
              r.decode(std::move(buf), api_version(4));
              return r;
          });
    }

private:
    void write_header(response_writer& wr, api_key key, api_version version) {
        wr.write(int16_t(key()));
        wr.write(int16_t(version()));
        wr.write(int32_t(correlation_id_));
        wr.write(std::string_view("test_client"));
        correlation_id_++;
    }

    correlation_type correlation_id_{0};
};

} // namespace kafka
