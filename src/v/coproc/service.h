#pragma once
#include "cluster/namespace.h"
#include "coproc/registration.h"
#include "coproc/types.h"
#include "model/validation.h"
#include "rpc/server.h"
#include "rpc/types.h"
#include "storage/api.h"
#include "storage/log.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace coproc {

using active_mappings = absl::flat_hash_map<model::ntp, storage::log>;

/// Coprocessing rpc registration/deregistration service
///
/// When a new coprocessor comes up or down this service will
/// be its interface for working with redpanda.
class service final : public registration_service {
public:
    service(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<active_mappings>&,
      ss::sharded<storage::api>&);

    /// coproc client calls this to 'register'
    ///
    /// \param metdata_info list of topics coproc is interested in transforming
    /// \param stgreaming_context rpc context expected at every endpoint
    ///
    /// \return structure representing an ack per topic
    ss::future<enable_topics_reply>
    enable_topics(metadata_info&&, rpc::streaming_context&) final;

    /// coproc client calls this to 'deregister'
    ///
    /// \param metdata_info list of topics coproc no longer wants updates on
    /// \param streaming_context rpc context expected at every endpoint
    ///
    /// \return structure representing an ack per topic
    ss::future<disable_topics_reply>
    disable_topics(metadata_info&&, rpc::streaming_context&) final;

private:
    /// Different implementation details of update_cache
    ss::future<enable_response_code> insert(model::topic_namespace);
    ss::future<disable_response_code> remove(model::topic_namespace);

    /// Main mapping for actively tracked coproc topics
    ss::sharded<active_mappings>& _mappings;

    /// Reference to the underlying storage, for querying active logs
    /// for an incoming topic_namespace request
    ss::sharded<storage::api>& _storage;
};

} // namespace coproc
