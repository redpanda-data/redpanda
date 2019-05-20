#include "chain_replication_service.h"

#include "filesystem/wal_core_mapping.h"
#include "filesystem/wal_pretty_print_utils.h"
#include "filesystem/wal_requests.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "smf_specializations.h"

#include <smf/futurize_utils.h>

#include <flatbuffers/minireflect.h>

#include <utility>

namespace chains {

void validate_puts(std::vector<wal_write_request>& fputs) {
    for (auto& p : fputs) {
        LOG_THROW_IF(!wal_write_request::is_valid(p), "invalid write request");
    }
}

// clang-format off
seastar::future<smf::rpc_typed_envelope<chain_put_reply>>
chain_replication_service::put(
  smf::rpc_recv_typed_context<chain_put_request>&& record) {
    if (!record) {
        return smf::futurize_status_for_type<chain_put_reply>(400);
    }
    // * Create a core to put map for all partition pairs
    // * Send to all cores and reduce
    DLOG_THROW_IF(!record.verify_fbs(), "Failed to verify put");
    DLOG_TRACE(
      "{}",
      flatbuffers::FlatBufferToString(
        (const uint8_t*)record.ctx.value().payload.get(),
        chain_put_request::MiniReflectTypeTable()));
    auto ptr = record.get();
    do_with(
      std::move(record),
      wal_core_mapping::core_assignment(ptr->put()),
      [this](auto& r, auto& fsputs) mutable {
          validate_puts(fsputs);

          using iter_t = typename std::vector<wal_write_request>::iterator;
          map_reduce(
            // for-all
            std::move_iterator<iter_t>(fsputs.begin()),
            std::move_iterator<iter_t>(fsputs.end()),
            // Mapper function
            [this](auto r) {
                auto const core = r.runner_core;
                return seastar::smp::submit_to(
                  core, [this, r = std::move(r)]() mutable {
                      return _wal->local().append(std::move(r));
                  });
            },

            // Initial State:
            std::make_unique<wal_write_reply>(
              r->put()->ns(), r->put()->topic()),

            // Reducer function
            [this](auto acc, auto next) {
                for (const auto& t : *next) {
                    auto& p = t.second;
                    acc->set_reply_partition_tuple(
                      acc->ns,
                      acc->topic,
                      p->partition,
                      p->start_offset,
                      p->end_offset);
                }
                return std::move(acc);
            })
            .then([](auto reduced) {
                smf::rpc_typed_envelope<chain_put_reply> data{};
                data.data->put = reduced->release();
                data.envelope.set_status(200);
                return seastar::make_ready_future<
                  smf::rpc_typed_envelope<chain_put_reply>>(std::move(data));
            });
      })
      .handle_exception([](auto eptr) {
          LOG_ERROR("Error saving chains::put(): {}", eptr);
          return smf::futurize_status_for_type<chain_put_reply>(501);
      });
}
// clang-format on

seastar::future<smf::rpc_typed_envelope<chain_get_reply>>
chain_replication_service::get(
  smf::rpc_recv_typed_context<chain_get_request>&& record) {
    if (!record) {
        return smf::futurize_status_for_type<chain_get_reply>(400);
    }
    DLOG_THROW_IF(!record.verify_fbs(), "Failed to verify get");
    DLOG_TRACE(
      "{}",
      flatbuffers::FlatBufferToString(
        (const uint8_t*)record.ctx.value().payload.get(),
        chain_get_request::MiniReflectTypeTable()));
    // get the semaphore and then delegate to real method
    auto ptr = record->get();
    return seastar::with_semaphore(
      record.ctx->rpc_server_limits->resources_available,
      ptr->max_bytes(),
      [this, record = std::move(record)]() mutable {
          return do_get(std::move(record));
      });
}

// clang-format off
seastar::future<smf::rpc_typed_envelope<chain_get_reply>>
chain_replication_service::do_get(
  smf::rpc_recv_typed_context<chain_get_request>&& record) {
    auto ptr = record->get();
    int64_t ns = ptr->ns();
    int64_t topic = ptr->topic();
    int64_t partition = ptr->partition();
    int64_t offset = ptr->offset();
    bool validate_payload = ptr->server_validate_payload();
    return seastar::do_with(
      std::move(record),
      wal_core_mapping::core_assignment(ptr),
      [this, offset](auto& record, auto req) {
          LOG_THROW_IF(
            !wal_read_request::is_valid(req), "invalid read request");
          auto core = req.runner_core;
          return seastar::smp::submit_to(core, [this, req]() mutable {
              return _wal->local().get(req);
          }).then([offset](std::unique_ptr<wal_read_reply> r) {
              smf::rpc_typed_envelope<chain_get_reply> data{};
              data.data->get = r->release();
              DLOG_THROW_IF(
                offset > data.data->get->next_offset,
                "Original offset: {} cannot bre greater than "
                "returned offset: {}",
                offset,
                data.data->get->next_offset);
              data.envelope.set_status(200);
              return seastar::make_ready_future<decltype(data)>(
                std::move(data));
          });
      })
      .handle_exception([ns, topic, partition, offset, validate_payload](
                          std::exception_ptr eptr) {
          LOG_ERROR("Error reading chains::get(): {}", eptr);
          wal_read_reply r(ns, topic, partition, offset, validate_payload);
          smf::rpc_typed_envelope<chain_get_reply> data{};
          data.data->get = r.release();
          data.envelope.set_status(501);
          return seastar::make_ready_future<decltype(data)>(std::move(data));
      });
}
// clang-format on

seastar::future<smf::rpc_envelope>
chain_replication_service::raw_get(smf::rpc_recv_context&& c) {
    using inner_t = chains::chain_get_request;
    using input_t = smf::rpc_recv_typed_context<inner_t>;
    return get(input_t(std::move(c))).then([this](auto x) {
        // uses the chain_replication/smf_specializations.h
        const chains::chain_get_replyT& ref = *x.data;
        x.envelope.letter.body = std::move(smf::native_table_as_buffer(ref));
        x.data = nullptr;

        smf::checksum_rpc(
          x.envelope.letter.header,
          x.envelope.letter.body.get(),
          x.envelope.letter.body.size());

        return std::move(x.envelope);
    });
}

} // namespace chains
