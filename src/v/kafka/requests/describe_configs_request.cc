#include "kafka/requests/describe_configs_request.h"

#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "model/metadata.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

void describe_configs_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();
    auto version = ctx.header().version;

    resources = reader.read_array([](request_reader& reader) {
        return resource{
          .type = reader.read_int8(),
          .name = reader.read_string(),
          .config_names = reader.read_array(
            [](request_reader& reader) { return reader.read_string(); }),
        };
    });

    if (version >= api_version(1)) {
        include_synonyms = reader.read_bool();
    }
}

static std::ostream&
operator<<(std::ostream& o, const describe_configs_request::resource& r) {
    return ss::fmt_print(
      o,
      "type={} name={} config_names={}",
      int32_t(r.type), // prevent printing int8 as char
      r.name,
      r.config_names);
}

std::ostream& operator<<(std::ostream& o, const describe_configs_request& r) {
    return ss::fmt_print(
      o, "inc_syn={} resources={}", r.include_synonyms, r.resources);
}

void describe_configs_response::encode(
  const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    const auto version = ctx.header().version;

    writer.write(int32_t(throttle_time_ms.count()));
    writer.write_array(results, [version](resource& r, response_writer& wr) {
        wr.write(r.error);
        wr.write(r.error_msg);
        wr.write(r.type);
        wr.write(r.name);
        wr.write_array(r.configs, [version](config& c, response_writer& wr) {
            wr.write(c.name);
            wr.write(c.value);
            wr.write(c.read_only);
            if (version == api_version(0)) {
                wr.write(c.is_default);
            }
            if (version >= api_version(1)) {
                wr.write(c.source);
            }
            wr.write(c.is_sensitive);
            if (version >= api_version(1)) {
                wr.write_array(
                  c.synonyms, [](config_synonym& s, response_writer& wr) {
                      wr.write(s.name);
                      wr.write(s.value);
                      wr.write(s.source);
                  });
            }
        });
    });
}

static std::ostream& operator<<(
  std::ostream& os, const describe_configs_response::config_synonym& s) {
    fmt::print(
      os, "name {} value {} src {}", s.name, s.value, int32_t(s.source));
    return os;
}

static std::ostream&
operator<<(std::ostream& os, const describe_configs_response::config& c) {
    fmt::print(
      os,
      "name {} value {} ro {} def {} src {} sen {} syns {}",
      c.name,
      c.value,
      c.read_only,
      c.is_default,
      c.source,
      c.is_sensitive,
      c.synonyms);
    return os;
}

static std::ostream&
operator<<(std::ostream& os, const describe_configs_response::resource& r) {
    fmt::print(
      os,
      "error {} errmsg {} type {} name {} config {}",
      r.error,
      r.error_msg,
      int32_t(r.type),
      r.name,
      r.configs);
    return os;
}

std::ostream& operator<<(std::ostream& os, const describe_configs_response& r) {
    fmt::print(os, "results {}", r.results);
    return os;
}

ss::future<response_ptr> describe_configs_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_configs_request request;
    request.decode(ctx);
    klog.trace("Handling request {}", request);

    return ss::do_with(
      std::move(ctx),
      std::move(request),
      [](request_context& ctx, describe_configs_request&) {
          return ctx.respond(describe_configs_response());
      });
}

} // namespace kafka
