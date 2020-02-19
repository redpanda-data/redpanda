#include "kafka/protocol_utils.h"

namespace kafka {
// TODO: move to iobuf_parser
ss::future<std::optional<request_header>>
parse_header(ss::input_stream<char>& src) {
    constexpr int16_t no_client_id = -1;
    return src.read_exactly(sizeof(raw_request_header))
      .then([&src](ss::temporary_buffer<char> buf) {
          if (src.eof()) {
              return ss::make_ready_future<std::optional<request_header>>();
          }
          auto client_id_size = be_to_cpu(
            reinterpret_cast<const raw_request_header*>(buf.get())
              ->client_id_size);
          auto* raw_header = reinterpret_cast<const raw_request_header*>(
            buf.get());
          auto header = request_header{
            api_key(ss::net::ntoh(raw_header->api_key)),
            api_version(ss::net::ntoh(raw_header->api_version)),
            correlation_id(ss::net::ntoh(raw_header->correlation))};

          if (client_id_size == 0) {
              header.client_id = std::string_view();
              return ss::make_ready_future<std::optional<request_header>>(
                std::move(header));
          }
          if (client_id_size == no_client_id) {
              return ss::make_ready_future<std::optional<request_header>>(
                std::move(header));
          }
          return src.read_exactly(client_id_size)
            .then([&src, header = std::move(header)](
                    ss::temporary_buffer<char> buf) mutable {
                if (src.eof()) {
                    throw std::runtime_error(
                      fmt::format("Unexpected EOF for client ID"));
                }
                header.client_id_buffer = std::move(buf);
                header.client_id = std::string_view(
                  header.client_id_buffer.get(),
                  header.client_id_buffer.size());
                validate_utf8(*header.client_id);
                return ss::make_ready_future<std::optional<request_header>>(
                  std::move(header));
            });
      });
}

size_t parse_size_buffer(ss::temporary_buffer<char>& buf) {
    auto* raw = ss::unaligned_cast<const int32_t*>(buf.get());
    int32_t size = be_to_cpu(*raw);
    if (size < 0) {
        throw std::runtime_error("kafka::parse_size_buffer is negative");
    }
    return size_t(size);
}

ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>& src) {
    return src.read_exactly(sizeof(int32_t))
      .then([](ss::temporary_buffer<char> buf) -> std::optional<size_t> {
          if (!buf) {
              return std::nullopt;
          }
          return parse_size_buffer(buf);
      });
}

ss::scattered_message<char>
response_as_scattered(response_ptr response, correlation_id correlation) {
    ss::sstring header(
      ss::sstring::initialized_later(), sizeof(raw_response_header));
    auto* raw_header = reinterpret_cast<raw_response_header*>(header.begin());
    auto size = int32_t(sizeof(correlation_id) + response->buf().size_bytes());
    raw_header->size = ss::cpu_to_be(size);
    raw_header->correlation = ss::cpu_to_be(correlation());

    ss::scattered_message<char> msg;
    msg.append(std::move(header));
    for (auto&& chunk : response->buf()) {
        msg.append_static(
          reinterpret_cast<const char*>(chunk.get()), chunk.size());
    }
    msg.on_delete([response = std::move(response)] {});
    return msg;
}

} // namespace kafka
