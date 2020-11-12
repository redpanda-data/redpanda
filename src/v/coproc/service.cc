#include "service.h"

#include "cluster/namespace.h"
#include "coproc/logger.h"
#include "coproc/types.h"
#include "ssx/future-util.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/loop.hh>

namespace coproc {

using erc = enable_response_code;

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<router>& router)
  : script_manager_service(sg, ssg)
  , _router(router) {}

enable_response_code
assemble_response(std::vector<enable_response_code> codes) {
    const bool all_dne = std::all_of(codes.begin(), codes.end(), [](auto x) {
        return x == erc::topic_does_not_exist;
    });
    if (all_dne) {
        return erc::topic_does_not_exist;
    }

    const bool any_double_id = std::any_of(
      codes.begin(), codes.end(), [](auto x) {
          return x == erc::script_id_already_exists;
      });
    const bool any_success = std::any_of(
      codes.begin(), codes.end(), [](auto x) { return x == erc::success; });
    if (!any_success && any_double_id) {
        // An attempt to re-register was made
        return erc::script_id_already_exists;
    }
    vassert(any_success, "At least one core should have inserted the source");
    return erc::success;
}

enable_response_code map_code(const coproc::errc error_code) {
    switch (error_code) {
    case errc::success:
        return erc::success;
    case errc::script_id_already_exists:
        return erc::script_id_already_exists;
    case errc::topic_does_not_exist:
        return erc::topic_does_not_exist;
    default:
        return erc::internal_error;
    };
}

ss::future<enable_response_code> service::insert(
  script_id id, model::topic_namespace&& tn, topic_ingestion_policy p) {
    return _router
      .map([id, p, tn = std::move(tn)](router& r) {
          return map_code(r.add_source(id, tn, p));
      })
      .then(&assemble_response);
}

enable_response_code
enable_validator(const model::topic& topic, topic_ingestion_policy p) {
    if (is_materialized_topic(topic)) {
        return erc::materialized_topic;
    } else if (model::validate_kafka_topic_name(topic).value() != 0) {
        return erc::invalid_topic;
    } else if (!is_valid_ingestion_policy(p)) {
        return erc::invalid_ingestion_policy;
    }
    return erc::success;
}

ss::future<enable_copros_reply::ack_id_pair> service::evaluate_topics(
  const script_id id,
  std::vector<enable_copros_request::data::topic_mode> topics) {
    vlog(
      coproclog.info,
      "Incoming request to enable new coprocessor with script_id {} and "
      "topics: {}",
      id,
      topics);
    if (topics.empty()) {
        vlog(
          coproclog.warn,
          "Request to enable coprocessor {} failed due to empty topics "
          "list",
          id);
        return ss::make_ready_future<enable_copros_reply::ack_id_pair>(
          std::make_pair(
            id, std::vector<enable_response_code>{erc::invalid_topic}));
    }
    return copro_exists(id).then([this, id, topics = std::move(topics)](
                                   bool exists) mutable {
        if (exists) {
            std::vector<enable_response_code> id_dne(
              topics.size(), erc::script_id_already_exists);
            return ss::make_ready_future<enable_copros_reply::ack_id_pair>(
              std::make_pair(id, std::move(id_dne)));
        }
        return ss::do_with(
          std::move(topics),
          [this,
           id](std::vector<enable_copros_request::data::topic_mode>& topics) {
              return ssx::async_transform(
                       topics.begin(),
                       topics.end(),
                       [this, id](enable_copros_request::data::topic_mode tm) {
                           const auto v = enable_validator(tm.first, tm.second);
                           if (v != erc::success) {
                               return ss::make_ready_future<erc>(v);
                           }
                           return insert(
                             id,
                             model::topic_namespace(
                               cluster::kafka_namespace, std::move(tm.first)),
                             tm.second);
                       })
                .then([id](auto erc_vec) {
                    return std::make_pair(id, std::move(erc_vec));
                });
          });
    });
}

ss::future<enable_copros_reply>
service::enable_copros(enable_copros_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return ss::do_with(
            std::move(req.inputs),
            [this](std::vector<enable_copros_request::data>& inputs) {
                return ssx::async_transform(
                         inputs.begin(),
                         inputs.end(),
                         [this](enable_copros_request::data rich_topics) {
                             return evaluate_topics(
                               rich_topics.id, std::move(rich_topics.topics));
                         })
                  .then([](std::vector<enable_copros_reply::ack_id_pair> acks) {
                      return enable_copros_reply{.acks = std::move(acks)};
                  });
            });
      });
}

ss::future<disable_response_code> service::remove(script_id id) {
    vlog(
      coproclog.info,
      "Incoming request to disable coprocessor with script_id {}",
      id);
    using drc = disable_response_code;
    return _router
      .map_reduce0(
        [id](router& r) { return r.remove_source(id); },
        false,
        std::logical_or<>())
      .then([](bool r) {
          return r ? drc::success : drc::script_id_does_not_exist;
      });
}

ss::future<disable_copros_reply>
service::disable_copros(disable_copros_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return ss::do_with(
            std::move(req.ids),
            [this](const std::vector<script_id>& script_ids) {
                return ssx::async_transform(
                         script_ids.begin(),
                         script_ids.end(),
                         [this](script_id id) { return remove(id); })
                  .then([](std::vector<disable_response_code> acks) {
                      return disable_copros_reply{.acks = std::move(acks)};
                  });
            });
      });
}

ss::future<bool> service::copro_exists(const script_id id) {
    return _router.map_reduce0(
      [id](const router& r) { return r.script_id_exists(id); },
      false,
      std::logical_or<>());
}

} // namespace coproc
