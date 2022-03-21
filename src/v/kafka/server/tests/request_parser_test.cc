// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/fetch.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/sync_group.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"
#include "utils/file_io.h"
#include "vlog.h"

// logger
static ss::logger rlog("request_parser");

/*
 * reads the next request from the input source as an iobuf
 */
static ss::future<iobuf> get_request(ss::input_stream<char>& input) {
    return input.read_exactly(sizeof(int32_t))
      .then([&input](ss::temporary_buffer<char> buf) {
          if (!buf) { // eof?
              return ss::make_ready_future<iobuf>(iobuf());
          }
          auto size = kafka::parse_size_buffer(buf.clone());
          vlog(rlog.info, "read: {}, size of kafka is:{}", buf.size(), size);
          return input.read_exactly(size).then(
            [size_buf = std::move(buf)](
              ss::temporary_buffer<char> buf) mutable {
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
static ss::future<kafka::request_context>
get_request_context(kafka::protocol& proto, ss::input_stream<char>&& input) {
    return do_with(std::move(input), [&proto](ss::input_stream<char>& input) {
        /*
         * read the request size prefix
         */
        return input.read_exactly(sizeof(int32_t))
          .then([&proto, &input](ss::temporary_buffer<char> buf) {
              auto size = kafka::parse_size_buffer(std::move(buf));
              /*
               * ready the request header
               */
              return kafka::parse_header(input).then(
                [&proto, &input, size](
                  std::optional<kafka::request_header> oheader) {
                    auto header = std::move(oheader.value());
                    auto remaining = size - kafka::request_header_size
                                     - header.client_id_buffer.size();
                    /*
                     * read the request body
                     */
                    return read_iobuf_exactly(input, remaining)
                      .then([&proto,
                             header = std::move(header)](iobuf buf) mutable {
                          /*
                           * build the request context
                           */
                          security::sasl_server sasl(
                            security::sasl_server::sasl_state::complete);
                          auto conn
                            = ss::make_lw_shared<kafka::connection_context>(
                              proto,
                              net::server::resources(nullptr, nullptr),
                              std::move(sasl),
                              false);

                          return kafka::request_context(
                            conn,
                            std::move(header),
                            std::move(buf),
                            std::chrono::milliseconds(0));
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
    writer.write(ctx.header().correlation);
    writer.write(ctx.header().client_id);

    // decode and echo
    switch (ctx.header().key) {
    case kafka::join_group_api::key: {
        vlog(rlog.info, "kafka::join_group_api::key");
        kafka::join_group_request r;
        r.decode(ctx.reader(), ctx.header().version);
        r.encode(writer, ctx.header().version);
        break;
    }

    case kafka::sync_group_api::key: {
        vlog(rlog.info, "kafka::sync_group_api::key");
        kafka::sync_group_request r;
        r.decode(ctx.reader(), ctx.header().version);
        r.encode(writer, ctx.header().version);
        break;
    }

    case kafka::heartbeat_api::key: {
        vlog(rlog.info, "kafka::heartbeat_api::key");
        kafka::heartbeat_request r;
        r.decode(ctx.reader(), ctx.header().version);
        r.encode(writer, ctx.header().version);
        break;
    }

    case kafka::leave_group_api::key: {
        vlog(rlog.info, "kafka::leave_group_api::key");
        kafka::leave_group_request r;
        r.decode(ctx.reader(), ctx.header().version);
        r.encode(writer, ctx.header().version);
        break;
    }

    case kafka::fetch_api::key: {
        vlog(rlog.info, "kafka::fetch_api::key");
        kafka::fetch_request r;
        r.decode(ctx.reader(), ctx.header().version);
        r.encode(writer, ctx.header().version);
        break;
    }

    case kafka::metadata_api::key: {
        vlog(rlog.info, "kafka::metadata_api::key");
        kafka::metadata_request r;
        r.decode(ctx.reader(), ctx.header().version);
        r.encode(writer, ctx.header().version);
        break;
    }

    case kafka::produce_api::key: {
        vlog(rlog.info, "kafka::produce_api::key");
        kafka::produce_request r;
        r.decode(ctx.reader(), ctx.header().version);

        // TODO: once we have a tool/utility for rebuilding the
        // batch blob after decoding, swap that in here for extra
        // testing.
        r.encode(writer, ctx.header().version);
        break;
    }

    default:
        throw std::runtime_error(
          fmt::format("unknown api key: {}", ctx.header().key));
    }

    // write the frame size into the placeholder
    int32_t total_size = os.size_bytes() - start_size;
    auto be_total_size = ss::cpu_to_be(total_size);
    auto* raw_size = reinterpret_cast<const char*>(&be_total_size);
    ph.write(raw_size, sizeof(be_total_size));

    return os;
}

static ss::input_stream<char> get_input() {
    return read_fully("requests.bin")
      .then([](iobuf b) { return make_iobuf_input_stream(std::move(b)); })
      .get0();
}

FIXTURE_TEST(request_test, redpanda_thread_fixture) {
    do_with(get_input(), [this](ss::input_stream<char>& input) {
        return ss::do_until(
          [&input] { return input.eof(); },
          [this, &input] {
              return get_request(input).then([this](iobuf request) {
                  if (request.size_bytes() == 0) { // eof?
                      return ss::make_ready_future<>();
                  }
                  // create an input stream over a share of the input. we'll
                  // use the original input to make a comparison to the
                  // output generated by processing the request.
                  auto req_input = make_iobuf_input_stream(request.copy());
                  return get_request_context(*proto, std::move(req_input))
                    .then([request = std::move(request)](
                            kafka::request_context ctx) mutable {
                        auto output = handle_request(std::move(ctx));
                        if (output != request) {
                            throw std::runtime_error(fmt::format(
                              "Test failed. Input({})  not match output({})",
                              request,
                              output));
                        }
                    });
              });
          });
    }).get();
}
