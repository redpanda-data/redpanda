#pragma once
#include "kafka/requests/response.h"
#include "kafka/requests/topics/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/util/bool_class.hh>

namespace kafka {

using include_message = ss::bool_class<struct include_message_tag>;

static inline kafka::response_ptr encode_topic_results(
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
          wr.write(r.ec);
          if (inc_msg) {
              wr.write(r.err_msg);
          }
      });
    /*
     * this is nvro, but clang can't do the conversion to foreign_ptr without a
     * copy, while gcc does but also fires off a warning that it is unncessary
     * for the move.
     *
     * The solution si probably to add a foreign_ptr(PtrType&&) overload.
     */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wredundant-move"
    return std::move(resp);
#pragma GCC diagnostic pop
}
} // namespace kafka
