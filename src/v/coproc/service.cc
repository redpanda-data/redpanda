#include "service.h"

#include "coproc/logger.h"
#include "coproc/types.h"
#include "vlog.h"

namespace coproc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<active_mappings>& mappings,
  ss::sharded<storage::api>& storage)
  : registration_service(sg, ssg)
  , _mappings(mappings)
  , _storage(storage) {}

service::~service() { _mappings.stop().get(); }

/// Generic method that iterates over all of the requests
/// and properly assembles a response, leaving the details left
/// to the 'fn' argument
template<typename T, typename Func>
ss::future<std::vector<T>> update_cache(metadata_info&& req, Func&& fn) {
    std::vector<T> responses;
    responses.resize(req.inputs.size());
    const auto size = req.inputs.size();
    return ss::map_reduce(
      boost::irange<unsigned>(0, size),
      [fn = std::forward<Func>(fn), req = std::move(req)](unsigned i) mutable {
          auto topic = std::move(req.inputs[i]);
          if (is_materialized_topic(topic)) {
              return ss::make_ready_future<std::tuple<unsigned, T>>(
                std::make_tuple(i, T::materialized_topic));
          } else if (model::validate_kafka_topic_name(topic).value() != 0) {
              return ss::make_ready_future<std::tuple<unsigned, T>>(
                std::make_tuple(i, T::invalid_topic));
          }
          return fn(model::topic_namespace(
                      cluster::kafka_namespace, std::move(topic)))
            .then([i](T ack) { return std::make_tuple(i, ack); });
      },
      std::move(responses),
      [](std::vector<T> acc, std::tuple<unsigned, T> x) {
          // Match responses to requests by the index used in the mapper
          auto [idx, response] = x;
          acc[idx] = response;
          return acc;
      });
}

enable_response_code
assemble_response(std::vector<enable_response_code> codes) {
    using erc = enable_response_code;
    const bool all_dne = std::all_of(codes.begin(), codes.end(), [](auto x) {
        return x == erc::topic_does_not_exist;
    });
    if (all_dne) {
        return erc::topic_does_not_exist;
    }
    const bool any_ae = std::any_of(codes.begin(), codes.end(), [](auto x) {
        return x == erc::topic_already_enabled;
    });
    const bool any_success = std::any_of(
      codes.begin(), codes.end(), [](auto x) { return x == erc::success; });

    if (any_ae && any_success) {
        vlog(
          coproclog.error,
          "Upon insertion of coproc tracked ntps inconsistey detected within "
          "ntp store");
        return erc::internal_error;
    } else if (any_ae) {
        return erc::topic_already_enabled;
    }
    return erc::success;
}

ss::future<enable_response_code> service::insert(model::topic_namespace tn) {
    using erc = enable_response_code;
    return _mappings
      .map_reduce0(
        [this, tn = std::move(tn)](active_mappings& ams) {
            auto ntps = _storage.local().log_mgr().get(tn);
            if (ntps.empty()) {
                return erc::topic_does_not_exist;
            }
            const auto size = ams.size();
            ams.merge(ntps);
            return size == ams.size() ? erc::topic_already_enabled
                                      : erc::success;
        },
        std::vector<erc>(),
        [](std::vector<erc> acc, erc x) {
            acc.push_back(x);
            return acc;
        })
      .then(&assemble_response)
      .then([this, tn](erc code) {
          if (code == erc::internal_error) {
              return remove(tn).then(
                [](auto /*drc*/) { return erc::internal_error; });
          }
          return ss::make_ready_future<erc>(code);
      });
}

ss::future<enable_topics_reply>
service::enable_topics(metadata_info&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return update_cache<enable_response_code>(
                   std::move(req),
                   [this](model::topic_namespace tn) {
                       return insert(std::move(tn));
                   })
            .then([](std::vector<enable_response_code> acks) {
                return enable_topics_reply{.acks = std::move(acks)};
            });
      });
}

ss::future<disable_response_code> service::remove(model::topic_namespace tn) {
    return _mappings
      .map_reduce0(
        [tn = std::move(tn)](active_mappings& ams) {
            bool erased = false;
            for (auto it = ams.cbegin(); it != ams.cend();) {
                if (it->first.ns == tn.ns && it->first.tp.topic == tn.tp) {
                    ams.erase(it++);
                    erased = true;
                } else {
                    ++it;
                }
            }
            return erased;
        },
        false,
        std::logical_or<>())
      .then([](bool r) {
          return r ? disable_response_code::success
                   : disable_response_code::topic_never_enabled;
      });
}

ss::future<disable_topics_reply>
service::disable_topics(metadata_info&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return update_cache<disable_response_code>(
                   std::move(req),
                   [this](model::topic_namespace tn) {
                       return remove(std::move(tn));
                   })
            .then([](std::vector<disable_response_code> acks) {
                return disable_topics_reply{.acks = std::move(acks)};
            });
      });
}

} // namespace coproc
