#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/schemata/join_group_request.h"
#include "kafka/requests/schemata/join_group_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <utility>

namespace kafka {

struct join_group_api final {
    static constexpr const char* name = "join group";
    static constexpr api_key key = api_key(11);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(4);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct join_group_request final {
    join_group_request_data data;

    join_group_request() = default;
    explicit join_group_request(request_context& ctx) { decode(ctx); }
    join_group_request(const join_group_request&) = delete;
    join_group_request& operator=(const join_group_request&) = delete;
    join_group_request(join_group_request&&) = default;
    join_group_request& operator=(join_group_request&&) = delete;

    // extra context from request header set in decode
    api_version version;
    std::optional<ss::sstring> client_id;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    /**
     * Convert the request member protocol list into the type used internally to
     * group membership. We maintain two different types because the internal
     * type is also the type stored on disk and we do not want it to be tied to
     * the type produced by code generation.
     */
    std::vector<member_protocol> native_member_protocols() const {
        std::vector<member_protocol> res;
        std::transform(
          data.protocols.cbegin(),
          data.protocols.cend(),
          std::back_inserter(res),
          [](const join_group_request_protocol& p) {
              return member_protocol{p.name, p.metadata};
          });
        return res;
    }

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_context& ctx);
};

static inline std::ostream&
operator<<(std::ostream& os, const join_group_request& r) {
    return os << r.data;
}

static inline const kafka::member_id no_member("");
static inline const kafka::member_id no_leader("");
static constexpr kafka::generation_id no_generation(-1);
static inline const kafka::protocol_name no_protocol("");

struct join_group_response final {
    using api_type = join_group_api;

    join_group_response_data data;

    join_group_response(kafka::member_id member_id, kafka::error_code error)
      : join_group_response(
        error, no_generation, no_protocol, no_leader, member_id) {}

    join_group_response(kafka::error_code error)
      : join_group_response(no_member, error) {}

    join_group_response(const join_group_request& r, kafka::error_code error)
      : join_group_response(r.data.member_id, error) {}

    join_group_response(
      kafka::error_code error,
      kafka::generation_id generation_id,
      kafka::protocol_name protocol_name,
      kafka::member_id leader_id,
      kafka::member_id member_id,
      std::vector<join_group_response_member> members = {}) {
        data.throttle_time_ms = std::chrono::milliseconds(0);
        data.error_code = error;
        data.generation_id = generation_id;
        data.protocol_name = std::move(protocol_name);
        data.leader = std::move(leader_id);
        data.member_id = std::move(member_id);
        data.members = std::move(members);
    }

    void encode(const request_context& ctx, response& resp) {
        data.encode(ctx, resp);
    }
};

static inline join_group_response
_make_join_error(kafka::member_id member_id, error_code error) {
    return join_group_response(
      error, no_generation, no_protocol, no_leader, std::move(member_id));
}

static inline ss::future<join_group_response>
make_join_error(kafka::member_id member_id, error_code error) {
    return ss::make_ready_future<join_group_response>(
      _make_join_error(std::move(member_id), error));
}

static inline std::ostream&
operator<<(std::ostream& os, const join_group_response& r) {
    return os << r.data;
}

// group membership helper to compare a protocol set from the wire with our
// internal type without doing a full type conversion.
static inline bool operator==(
  const std::vector<join_group_request_protocol>& a,
  const std::vector<member_protocol>& b) {
    return std::equal(
      a.cbegin(),
      a.cend(),
      b.cbegin(),
      b.cend(),
      [](const join_group_request_protocol& a, const member_protocol& b) {
          return a.name == b.name && a.metadata == b.metadata;
      });
}

// group membership helper to compare a protocol set from the wire with our
// internal type without doing a full type conversion.
static inline bool operator!=(
  const std::vector<join_group_request_protocol>& a,
  const std::vector<member_protocol>& b) {
    return !(a == b);
}

} // namespace kafka
