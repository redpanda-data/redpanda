#pragma once
#include "model/fundamental.h"
#include "redpanda/kafka/requests/response.h"
#include "redpanda/kafka/requests/topics/types.h"
#include "seastarx.h"

#include <seastar/util/bool_class.hh>
namespace kafka {

using include_message = bool_class<struct include_message_tag>;

kafka::response_ptr encode_topic_results(
  const std::vector<topic_op_result>& errors,
  int32_t throttle_time_ms,
  include_message inc_msg = include_message::yes) {
    auto resp = std::make_unique<kafka::response>();
    // throttle time
    if (throttle_time_ms >= 0) {
        resp->writer().write(throttle_time_ms);
    }
    // errors
    resp->writer().write_array(
      errors, [inc_msg](const topic_op_result& r, response_writer& wr) {
          wr.write(r.topic);
          wr.write(r.error_code);
          if (inc_msg) {
              wr.write(r.err_msg);
          }
      });
    return std::move(resp);
}
} // namespace kafka
