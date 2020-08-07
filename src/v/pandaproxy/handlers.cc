#include "handlers.h"

#include "json/json.h"

namespace pandaproxy {

ss::future<server::reply_t>
get_topics_names(server::request_t rq, server::reply_t rp) {
    rq.req.reset();
    kafka::metadata_request req;
    req.list_all_topics = true;
    return rq.ctx.client.dispatch(std::move(req))
      .then([rp = std::move(rp)](
              kafka::metadata_request::api_type::response_type res) mutable {
          std::vector<model::topic_view> names;
          names.reserve(res.topics.size());

          std::transform(
            res.topics.begin(),
            res.topics.end(),
            std::back_inserter(names),
            [](const kafka::metadata_response::topic& e) {
                return model::topic_view(e.name);
            });

          rapidjson::StringBuffer str_buf;
          rapidjson::Writer<rapidjson::StringBuffer> wrt(str_buf);

          json::rjson_serialize(wrt, names);

          ss::sstring json_rslt{str_buf.GetString()};
          rp.rep->write_body("json", json_rslt);
          return std::move(rp);
      });
}

} // namespace pandaproxy