#include "redpanda/application.h"
#include "redpanda/kafka/requests/fetch_request.h"
#include "redpanda/kafka/requests/heartbeat_request.h"
#include "redpanda/kafka/requests/join_group_request.h"
#include "redpanda/kafka/requests/leave_group_request.h"
#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/requests/response_writer.h"
#include "redpanda/kafka/requests/sync_group_request.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/thread.hh>

#include <fstream>
#include <iostream>
#include <streambuf>

/*
 * build an input_stream<char> from stdin
 */
static input_stream<char> get_stdin() {
    std::string input{std::istreambuf_iterator<char>(std::cin),
                      std::istreambuf_iterator<char>()};
    iobuf b;
    b.append(input.data(), input.size());
    return make_iobuf_input_stream(std::move(b));
}

/*
 * make a fake request_context from the data on stdin
 */
static future<kafka::request_context>
make_request_context(kafka::request_header&& header, iobuf&& buf) {
    return async([header = std::move(header), buf = std::move(buf)]() mutable {
        application app;
        app.start_config();
        app.create_groups();
        app.wire_up_services();

        kafka::request_context ctx(
          app.metadata_cache,
          app.cntrl_dispatcher.local(),
          std::move(header),
          std::move(buf),
          std::chrono::milliseconds(0),
          app.group_router.local(),
          app.shard_table.local(),
          app.partition_manager);

        return std::move(ctx);
    });
}

/*
 * build a fake request context from stdin
 */
static future<kafka::request_context> get_request_context() {
    return do_with(get_stdin(), [](input_stream<char>& input) {
        /*
         * read the request size prefix
         */
        return input.read_exactly(sizeof(kafka::size_type))
          .then([&input](temporary_buffer<char> buf) {
              auto size = kafka::kafka_server::connection::process_size(
                input, std::move(buf));
              /*
               * ready the request header
               */
              return kafka::kafka_server::connection::read_header(input).then(
                [&input, size](kafka::request_header header) {
                    auto remaining = size - sizeof(kafka::raw_request_header)
                                     - header.client_id_buffer.size();
                    /*
                     * read the request body
                     */

                    return read_iobuf_exactly(input, remaining)
                      .then([header = std::move(header)](iobuf buf) mutable {
                          /*
                           * build the request context
                           */
                          return make_request_context(
                            std::move(header), std::move(buf));
                      });
                });
          });
    });
}

static future<> handle_request(sstring output, kafka::request_context&& ctx) {
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

    default:
        return make_exception_future<>(std::runtime_error(
          fmt::format("unknown api key: {}", ctx.header().key)));
    }

    // write the frame size into the placeholder
    int32_t total_size = os.size_bytes() - start_size;
    auto be_total_size = cpu_to_be(total_size);
    auto* raw_size = reinterpret_cast<const char*>(&be_total_size);
    ph.write(raw_size, sizeof(be_total_size));

    // send the full message to the output file
    auto flags = open_flags::wo | open_flags::create | open_flags::truncate;
    return open_file_dma(std::move(output), flags)
      .then([os = std::move(os)](file f) mutable {
          auto out = make_lw_shared<output_stream<char>>(
            make_file_output_stream(std::move(f)));
          return do_with(std::move(os), [out](iobuf& os) {
              return do_for_each(
                       os.begin(),
                       os.end(),
                       [out](iobuf::fragment& fragment) {
                           return out->write(fragment.get(), fragment.size());
                       })
                .then([out] { return out->flush(); })
                .then([out] { return out->close(); })
                .finally([out] {});
          });
      });
}

int main(int argc, char** argv) {
    namespace po = boost::program_options;

    seastar::app_template app;
    app.add_options()(
      "output,o", po::value<sstring>()->required(), "output file");

    return app.run(argc, argv, [&]() mutable {
        auto& cfg = app.configuration();
        return get_request_context().then([&cfg](kafka::request_context ctx) {
            auto output = cfg["output"].as<sstring>();
            return handle_request(output, std::move(ctx));
        });
    });
}
