#include "kafka/requests/api_versions_request.h"
#include "kafka/requests/create_topics_request.h"
#include "kafka/requests/fetch_request.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/requests.h"
#include "kafka/server.h"
#include "rpc/transport.h"
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
class client : public rpc::base_transport {
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
    using rpc::base_transport::base_transport;

    /*
     * TODO: the concept here can be improved once we convert all of the request
     * types to encode their type relationships between api/request/response.
     */
    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    future<typename T::api_type::response_type> dispatch(
      T r, api_version request_version, api_version response_version) {
        return send_recv([this, request_version, r = std::move(r)](
                           response_writer& wr) mutable {
                   write_header(wr, T::api_type::key, request_version);
                   r.encode(wr, request_version);
               })
          .then([response_version](iobuf buf) {
              using response_type = typename T::api_type::response_type;
              response_type r;
              r.decode(std::move(buf), response_version);
              return make_ready_future<response_type>(std::move(r));
          });
    }

    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    future<typename T::api_type::response_type> dispatch(T r, api_version ver) {
        return dispatch(r, ver, ver);
    }

    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    future<typename T::api_type::response_type> dispatch(T r) {
        return dispatch(r, T::api_type::max_supported);
    }

    /*
     * TODO: the concept here can be improved once we convert all of the request
     * types to encode their type relationships between api/request/response.
     */
    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    future<typename T::api_type::response_type> dispatch(
      T r, api_version request_version, api_version response_version) {
        return send_recv([this, request_version, r = std::move(r)](
                           response_writer& wr) mutable {
                   write_header(wr, T::api_type::key, request_version);
                   r.encode(wr, request_version);
               })
          .then([response_version](iobuf buf) {
              using response_type = typename T::api_type::response_type;
              response_type r;
              r.decode(std::move(buf), response_version);
              return make_ready_future<response_type>(std::move(r));
          });
    }

    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    future<typename T::api_type::response_type> dispatch(T r, api_version ver) {
        return dispatch(r, ver, ver);
    }

    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    future<typename T::api_type::response_type> dispatch(T r) {
        return dispatch(r, T::api_type::max_supported);
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
