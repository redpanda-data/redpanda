#include "redpanda_service.h"

#include "filesystem/wal_core_mapping.h"

#include <smf/futurize_utils.h>

void validate_creates(std::vector<wal_create_request>& creates) {
    for (auto& r : creates) {
        LOG_THROW_IF(
          !wal_create_request::is_valid(r), "Invalid wal_create_request");
    }
}

seastar::future<smf::rpc_typed_envelope<wal_topic_create_reply>>
redpanda_service::create_topic(
  smf::rpc_recv_typed_context<wal_topic_create_request>&& record) {
    if (!record) {
        return smf::futurize_status_for_type<wal_topic_create_reply>(400);
    }
    if (record->smeta()) {
        LOG_WARN("Not fully implemented. Missing paxos");
    }
    auto ptr = record.get();
    return seastar::do_with(
             std::move(record),
             wal_core_mapping::core_assignment(ptr),
             [this](auto& r, auto& fscreates) mutable {
                 validate_creates(fscreates);
                 using iter_t = std::vector<wal_create_request>::iterator;
                 return seastar::map_reduce(
                          // for-all
                          std::move_iterator<iter_t>(fscreates.begin()),
                          std::move_iterator<iter_t>(fscreates.end()),
                          // Mapper function
                          [this](auto r) {
                              auto const core = r.runner_core;
                              return seastar::smp::submit_to(
                                core, [this, r = std::move(r)]() mutable {
                                    return _wal->local().create(std::move(r));
                                });
                          },

                          // Initial State:
                          std::make_unique<wal_create_reply>(),

                          // Reducer function
                          [this](auto acc, auto next) {
                              // TODO(agallego) - combine the responses when
                              // we add the stats per partition
                              return std::move(acc);
                          })
                   .then([](auto reduced) {
                       smf::rpc_typed_envelope<wal_topic_create_reply> data{};
                       data.data = std::move(reduced->data);
                       data.envelope.set_status(200);
                       return seastar::make_ready_future<
                         smf::rpc_typed_envelope<wal_topic_create_reply>>(
                         std::move(data));
                   });
             })
      .handle_exception([](auto eptr) {
          LOG_ERROR("Error saving chains::put(): {}", eptr);
          return smf::futurize_status_for_type<wal_topic_create_reply>(501);
      });
}
