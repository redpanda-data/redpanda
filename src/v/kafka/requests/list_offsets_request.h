#pragma once
#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/schemata/list_offset_request.h"
#include "kafka/requests/schemata/list_offset_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_set.h>

namespace kafka {

struct list_offsets_response;

class list_offsets_api final {
public:
    using response_type = list_offsets_response;

    static constexpr const char* name = "list_offsets";
    static constexpr api_key key = api_key(2);
    static constexpr api_version min_supported = api_version(1);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct list_offsets_request final {
    using api_type = list_offsets_api;

    static constexpr model::timestamp earliest_timestamp{-2};
    static constexpr model::timestamp latest_timestamp{-1};

    list_offset_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    absl::btree_set<model::topic_partition> tp_dups;

    void compute_duplicate_topics();

    bool duplicate_tp(const model::topic& t, model::partition_id id) const {
        model::topic_partition tp(t, id);
        return tp_dups.find(tp) != tp_dups.end();
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const list_offsets_request& r) {
    return os << r.data;
}

struct list_offsets_response final {
    using api_type = list_offsets_api;

    list_offset_response_data data;

    static list_offset_partition_response make_partition(
      model::partition_id id,
      error_code error,
      model::timestamp timestamp,
      model::offset offset) {
        return list_offset_partition_response{
          .partition_index = id,
          .error_code = error,
          .timestamp = timestamp,
          .offset = offset,
        };
    }

    static list_offset_partition_response make_partition(
      model::partition_id id,
      model::timestamp timestamp,
      model::offset offset) {
        return make_partition(id, error_code::none, timestamp, offset);
    }

    static list_offset_partition_response
    make_partition(model::partition_id id, error_code error) {
        return make_partition(
          id, error, model::timestamp(-1), model::offset(-1));
    }

    void encode(const request_context& ctx, response& resp) {
        data.encode(ctx, resp);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const list_offsets_response& r) {
    return os << r.data;
}

} // namespace kafka
