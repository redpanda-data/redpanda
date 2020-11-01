#include "coproc/router.h"

#include "coproc/logger.h"
#include "coproc/types.h"
#include "model/limits.h"
#include "storage/types.h"
#include "units.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/loop.hh>
#include <seastar/core/map_reduce.hh>

#include <absl/container/flat_hash_map.h>

namespace coproc {

// Eventually replace this with a nifty consumer impl that can manage
// keeping track of the highest offset
ss::future<std::pair<model::offset, model::record_batch_reader>>
extract_last_offset(model::record_batch_reader reader) {
    return model::consume_reader_to_memory(std::move(reader), model::no_timeout)
      .then([](ss::circular_buffer<model::record_batch> batches) {
          vassert(!batches.empty(), "Batches must never be empty");
          auto offset = batches.back().last_offset();
          return std::make_pair(
            offset, model::make_memory_record_batch_reader(std::move(batches)));
      });
}

router::router(ss::socket_address addr, ss::sharded<storage::api>& api)
  : _api(api)
  , _client({rpc::transport_configuration{
      .server_addr = addr, .credentials = nullptr}}) {}

ss::future<> router::route() {
    return ss::do_until(
      [this] { return _abort_source.abort_requested(); },
      [this] {
          return ss::map_reduce(
                   _sources.begin(),
                   _sources.end(),
                   [this](auto& p) { return route_ntp(p.first, p.second); },
                   std::vector<process_batch_request::data>(),
                   [](
                     std::vector<process_batch_request::data> acc,
                     opt_req_data x) {
                       if (x.has_value()) {
                           acc.push_back(std::move(*x));
                       }
                       return acc;
                   })
            .then([this](std::vector<process_batch_request::data> batch) {
                if (!batch.empty()) {
                    process_batch_request r{.reqs = std::move(batch)};
                    return send_batch(std::move(r));
                }
                return ss::now();
            });
      });
}

ss::future<router::opt_req_data>
router::route_ntp(const model::ntp& ntp, topic_state& ts) {
    return make_reader_cfg(ts.log, ts.head)
      .then(
        [this, ntp, log = ts.log, sids = ts.scripts](opt_cfg config) mutable {
            if (!config) {
                return ss::make_ready_future<opt_req_data>(std::nullopt);
            }
            return log.make_reader(*config)
              .then([](model::record_batch_reader rbr) {
                  return extract_last_offset(std::move(rbr));
              })
              .then([this, ntp = std::move(ntp), sids = std::move(sids)](
                      std::pair<model::offset, model::record_batch_reader>
                        offset_and_rbr) mutable {
                  auto found = _sources.find(ntp);
                  if (found == _sources.end()) {
                      vlog(
                        coproclog.info,
                        "Ntp removed while about to assemble batch: {}",
                        ntp);
                      return opt_req_data(std::nullopt);
                  }
                  found->second.head.dirty = offset_and_rbr.first;
                  std::vector<script_id> ids(
                    std::make_move_iterator(sids.begin()),
                    std::make_move_iterator(sids.end()));
                  return opt_req_data(process_batch_request::data{
                    .ids = std::move(ids),
                    .ntp = std::move(ntp),
                    .reader = std::move(offset_and_rbr.second)});
              });
        });
}

ss::future<> router::send_batch(process_batch_request r) {
    using reply_type = result<rpc::client_context<process_batch_reply>>;
    return _client
      .process_batch(std::move(r), rpc::client_opts(model::no_timeout))
      .then_wrapped([this](ss::future<reply_type> f) {
          try {
              auto reply = f.get0();
              if (reply) {
                  return process_reply(std::move(reply.value().data));
              }
              vlog(
                coproclog.error, "Error on copro request: {}", reply.error());
          } catch (const std::exception& e) {
              vlog(coproclog.error, "Copro request future threw: {}", e.what());
          }
          return ss::now();
      });
}

ss::future<> router::process_reply(process_batch_reply r) {
    if (r.resps.empty()) {
        vlog(coproclog.error, "Erroneous empty response received");
        return ss::now();
    }
    return ss::do_with(
      std::move(r.resps),
      [this](std::vector<process_batch_reply::data>& resps) mutable {
          const auto range = boost::irange<size_t>(0, resps.size());
          return ss::do_for_each(range, [this, &resps](size_t i) {
              return process_reply_one(std::move(resps[i]));
          });
      });
}

void router::bump_offset(const model::ntp& src_ntp, const script_id sid) {
    auto found = _sources.find(src_ntp);
    if (found == _sources.end()) {
        vlog(coproclog.warn, "Ntp removed before offset set: {}", src_ntp);
        return;
    }
    auto fsid = found->second.scripts.find(sid);
    if (fsid == found->second.scripts.end()) {
        vlog(coproclog.warn, "Script id removed before offset set: {}", sid);
        return;
    }
    found->second.head.committed = found->second.head.dirty;
}

ss::future<> router::process_reply_one(process_batch_reply::data e) {
    // Strip the source/dest topics from the materialized topic
    const auto mt = make_materialized_topic(e.ntp.tp.topic);
    if (!mt) {
        // For now this will signify a null response, which means the
        // record_batch was is to be filtered out of the materialized_topic.
        // Mark offset, to continue to next record and do nothing else.
        bump_offset(e.ntp, e.id);
        return ss::now();
    }
    // The original ntp without the .$<destination>$ part of the topic
    model::ntp src_ntp(e.ntp.ns, mt->src, e.ntp.tp.partition);
    // Create the materialized log, the name of the log will be of the
    // format: <src>.$<destination>$
    return get_log(e.ntp).then(
      [this, src_ntp, id = e.id, reader = std::move(e.reader)](
        storage::log log) mutable {
          // Append the requested data to the end of the log
          storage::log_append_config cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout};
          return std::move(reader)
            .for_each_ref(log.make_appender(cfg), model::no_timeout)
            .then([this, src_ntp, id, log](storage::append_result) mutable {
                // Update the offset, as the write was successful
                bump_offset(src_ntp, id);
                return log.flush();
            });
      });
}

ss::future<router::opt_cfg>
router::make_reader_cfg(storage::log log, topic_offsets& head) {
    return ss::get_units(head.sem_, 1)
      .then([this, log, committed = head.committed](auto /*units*/) {
          const storage::offset_stats ostats = log.offsets();
          if (committed >= ostats.committed_offset) {
              // Signifies materialized log is up-to-date with source, there
              // isn't anything more to read
              return opt_cfg(std::nullopt);
          } else if (ostats.dirty_offset == model::offset(0)) {
              // TODO(rob) This is a hack, it avoids the odd issue when the
              // last committed_offset is 0 and we initiate a read, finding
              // a batch of length 0. This is bad because the code continues
              // on the assumption that all batches have a length > 0, so
              // the last_offset can be calculated.
              return opt_cfg(std::nullopt);
          }
          const model::offset start
            = (committed == model::model_limits<model::offset>::min())
                ? model::offset(0)
                : committed + model::offset(1);
          return opt_cfg(
            reader_cfg(start, model::model_limits<model::offset>::max()));
      });
}

ss::future<storage::log> router::get_log(const model::ntp& ntp) {
    auto found = _api.local().log_mgr().get(ntp);
    if (found) {
        return ss::make_ready_future<storage::log>(*found);
    }
    vlog(coproclog.info, "Making new log: {}", ntp);
    return _api.local().log_mgr().manage(
      storage::ntp_config(ntp, _api.local().log_mgr().config().base_dir));
}

errc router::add_source(
  const script_id id,
  const model::topic_namespace& tns,
  topic_ingestion_policy p) {
    // For now only support the 'latest' policy
    if (!is_valid_ingestion_policy(p)) {
        return errc::invalid_ingestion_policy;
    }
    if (script_id_exists(id)) {
        return errc::script_id_already_exists;
    }
    auto logs = _api.local().log_mgr().get(tns);
    if (logs.empty()) {
        return errc::topic_does_not_exist;
    }
    for (auto& [ntp, log] : logs) {
        auto found = _sources.find(ntp);
        if (found == _sources.end()) {
            topic_state ts{
              .log = log, .head = topic_offsets(), .scripts = {id}};
            _sources.emplace(ntp, std::move(ts));
        } else {
            auto& scripts = found->second.scripts;
            auto id_found = scripts.find(id);
            vassert(id_found == scripts.end(), "Failed to detect double id");
            scripts.emplace(id);
        }
        vlog(coproclog.info, "Inserted ntp {} id {}", ntp, id);
    }
    return errc::success;
}

bool router::remove_source(const script_id sid) {
    absl::flat_hash_set<model::ntp> deleted;
    std::for_each(_sources.begin(), _sources.end(), [&deleted, sid](auto& p) {
        auto& scripts = p.second.scripts;
        scripts.erase(sid);
        if (scripts.empty()) {
            deleted.emplace(p.first);
        }
    });
    // If no more scripts are tracking an ntp, remove the ntp
    absl::erase_if(_sources, [&deleted](const auto& p) {
        return deleted.contains(p.first);
    });

    return !deleted.empty();
}

bool router::script_id_exists(const script_id sid) const {
    return std::any_of(_sources.begin(), _sources.end(), [sid](auto& p) {
        return p.second.scripts.contains(sid);
    });
}

storage::log_reader_config
router::reader_cfg(model::offset start, model::offset end) {
    return storage::log_reader_config(
      start,
      end,
      1,
      32_KiB,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      _abort_source);
}

} // namespace coproc
