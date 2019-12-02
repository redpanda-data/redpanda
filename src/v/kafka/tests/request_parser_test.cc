#include "kafka/requests/fetch_request.h"
#include "kafka/requests/metadata_request.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"
#include "utils/file_io.h"

/*
 * reads the next request from the input source as an iobuf
 */
static future<iobuf> get_request(input_stream<char>& input) {
    return input.read_exactly(sizeof(kafka::size_type))
      .then([&input](temporary_buffer<char> buf) {
          if (!buf) { // eof?
              return make_ready_future<iobuf>(iobuf());
          }
          auto size = kafka::kafka_server::connection::process_size(
            input, buf.share());
          return input.read_exactly(size).then(
            [size_buf = std::move(buf)](temporary_buffer<char> buf) mutable {
                iobuf req;
                req.append(std::move(size_buf));
                req.append(std::move(buf));
                return req;
            });
      });
}

/*
 * build a fake request context from stdin
 */
static future<kafka::request_context>
get_request_context(application& app, input_stream<char>&& input) {
    return do_with(std::move(input), [&app](input_stream<char>& input) {
        /*
         * read the request size prefix
         */
        return input.read_exactly(sizeof(kafka::size_type))
          .then([&app, &input](temporary_buffer<char> buf) {
              auto size = kafka::kafka_server::connection::process_size(
                input, std::move(buf));
              /*
               * ready the request header
               */
              return kafka::kafka_server::connection::read_header(input).then(
                [&app, &input, size](kafka::request_header header) {
                    auto remaining = size - sizeof(kafka::raw_request_header)
                                     - header.client_id_buffer.size();
                    /*
                     * read the request body
                     */
                    return read_iobuf_exactly(input, remaining)
                      .then(
                        [&app, header = std::move(header)](iobuf buf) mutable {
                            /*
                             * build the request context
                             */
                            return kafka::request_context(
                              app.metadata_cache,
                              app.cntrl_dispatcher.local(),
                              std::move(header),
                              std::move(buf),
                              std::chrono::milliseconds(0),
                              app.group_router.local(),
                              app.shard_table.local(),
                              app.partition_manager);
                        });
                });
          });
    });
}

static iobuf handle_request(kafka::request_context&& ctx) {
    iobuf os;

    // reserve a spot for the frame size
    auto ph = os.reserve(sizeof(int32_t));
    auto start_size = os.size_bytes();

    // write the header
    kafka::response_writer writer(os);
    writer.write(ctx.header().key);
    writer.write(ctx.header().version);
    writer.write(ctx.header().correlation_id);
    writer.write(ctx.header().client_id);

    // decode and echo
    switch (ctx.header().key) {
    case kafka::join_group_api::key: {
        kafka::join_group_request r;
        r.decode(ctx);
        r.encode(ctx, writer);
        break;
    }

    case kafka::sync_group_api::key: {
        kafka::sync_group_request r;
        r.decode(ctx);
        r.encode(ctx, writer);
        break;
    }

    case kafka::heartbeat_api::key: {
        kafka::heartbeat_request r;
        r.decode(ctx);
        r.encode(ctx, writer);
        break;
    }

    case kafka::leave_group_api::key: {
        kafka::leave_group_request r;
        r.decode(ctx);
        r.encode(ctx, writer);
        break;
    }

    case kafka::fetch_api::key: {
        kafka::fetch_request r;
        r.decode(ctx);
        r.encode(ctx, writer);
        break;
    }

    case kafka::metadata_api::key: {
        kafka::metadata_request r;
        r.decode(ctx);
        r.encode(ctx, writer);
        break;
    }

    default:
        throw std::runtime_error(
          fmt::format("unknown api key: {}", ctx.header().key));
    }

    // write the frame size into the placeholder
    int32_t total_size = os.size_bytes() - start_size;
    auto be_total_size = cpu_to_be(total_size);
    auto* raw_size = reinterpret_cast<const char*>(&be_total_size);
    ph.write(raw_size, sizeof(be_total_size));

    return os;
}

static input_stream<char> get_input() {
    return read_fully("requests.bin")
      .then([](iobuf b) { return make_iobuf_input_stream(std::move(b)); })
      .get0();
}

FIXTURE_TEST(request_test, redpanda_thread_fixture) {
    do_with(get_input(), [this](input_stream<char>& input) {
        return do_until(
          [&input] { return input.eof(); },
          [this, &input] {
              return get_request(input).then([this](iobuf request) {
                  if (request.size_bytes() == 0) { // eof?
                      return make_ready_future<>();
                  }
                  // create an input stream over a share of the input. we'll
                  // use the original input to make a comparison to the
                  // output generated by processing the request.
                  auto req_input = make_iobuf_input_stream(request.copy());
                  return get_request_context(app, std::move(req_input))
                    .then([request = std::move(request)](
                            kafka::request_context ctx) mutable {
                        auto output = handle_request(std::move(ctx));
                        if (output != request) {
                            throw std::runtime_error("test failed");
                        }
                    });
              });
          });
    }).get();
}
