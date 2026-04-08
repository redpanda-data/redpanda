/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "container/chunked_vector.h"
#include "kafka/client/fwd.h"
#include "kafka/data/rpc/fwd.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "security/audit/client_probe.h"
#include "security/audit/fwd.h"
#include "security/audit/probe.h"
#include "security/audit/schemas/application_activity.h"
#include "ssx/mutex.h"
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

namespace security::audit {

struct partition_batch {
    model::partition_id pid;
    model::record_batch batch;
    std::optional<ssx::semaphore_units> send_units{};
};

class audit_sink {
protected:
    static constexpr ss::shard_id client_shard_id{0};

public:
    audit_sink(
      audit_log_manager* audit_mgr,
      cluster::controller* controller,
      bool active)
      : _audit_mgr(audit_mgr)
      , _controller(controller)
      , _active(active) {}

    audit_sink(const audit_sink&) = delete;
    audit_sink& operator=(const audit_sink&) = delete;
    audit_sink(audit_sink&&) = delete;
    audit_sink& operator=(audit_sink&&) = delete;
    virtual ~audit_sink();

    /// Starts an audit_client if none is allocated, backgrounds the work
    ss::future<> start();

    /// Closes all gates, deallocates client returns when all has completed
    ss::future<> stop();

    /// Produce to the audit topic within the context of the internal locks,
    /// ensuring toggling of the audit master switch happens in lock step with
    /// calls to produce()
    ss::future<> produce(
      chunked_vector<partition_batch> records,
      std::optional<ss::timer<>::duration> timeout = std::nullopt);

    /// Allocates and connects, or deallocates and shuts down the audit client
    void toggle(bool);
    bool active() { return _active; }

    using update_auth_fn = ss::noncopyable_function<ss::future<>(bool)>;
    static std::unique_ptr<audit_sink> make_kafka_sink(
      audit_log_manager*,
      cluster::controller*,
      kafka::client::configuration&,
      update_auth_fn);

    static std::unique_ptr<audit_sink> make_rpc_sink(
      audit_log_manager*, cluster::controller*, kafka::data::rpc::client*);

protected:
    ss::future<>
      publish_app_lifecycle_event(application_lifecycle::activity_id);

    virtual void make_client() = 0;
    virtual audit_client* client() = 0;
    virtual void reset_client() = 0;
    cluster::controller* controller() { return _controller; }
    audit_log_manager* manager() { return _audit_mgr; }

    virtual ss::future<> pause() = 0;
    virtual ss::future<> resume() = 0;

private:
    ss::future<> do_toggle(bool);
    /// Primitives for ensuring background work and toggling of switch w/ async
    /// work occur in lock step
    ss::gate _gate;
    ssx::mutex _mutex{"audit_sink::mutex"};

    /// In the case the client did not finish intialization this optional may be
    /// fufilled by a fiber attempting to shutdown the client. The future will
    /// then later be waited on by the fiber that was initializing the client.
    std::optional<ss::future<>> _early_exit_future;

    /// Reference to audit manager so synchronization with its fibers may occur.
    /// Supports pausing and resuming these fibers so the client can safely be
    /// deallocated when no more work is concurrently entering the system
    audit_log_manager* _audit_mgr;

    cluster::controller* _controller;
    bool _active;
};

class audit_client {
public:
    audit_client(audit_sink* sink, cluster::controller*);
    audit_client(const audit_client&) = delete;
    audit_client& operator=(const audit_client&) = delete;
    audit_client(audit_client&&) = delete;
    audit_client& operator=(audit_client&&) = delete;
    virtual ~audit_client() = default;

    /// Initializes the client (with all necessary auth) and connects to the
    /// remote broker. If successful requests to create audit topic and all
    /// necessary ACLs will be made. produce() cannot be called until
    /// initialization completes with success.
    ss::future<> initialize();

    /// Shuts down the client, may wait for up to
    /// kafka::config::produce_shutdown_delay_ms to complete
    ss::future<> shutdown();

    /// Produces to the audit topic partition specified with each batch.
    /// Blocks if semaphore is exhausted or until the timeout expires, if one is
    /// provided
    ss::future<> produce(
      chunked_vector<partition_batch>,
      audit_probe&,
      std::optional<ss::timer<>::duration> timeout = std::nullopt);
    /// Returns true if the configuration phase has completed which includes:
    /// - Connecting to the broker(s) w/ ephemeral creds
    /// - Creating ACLs
    /// - Creating internal audit topic
    bool is_initialized() const { return _is_initialized; }

    cluster::controller* controller() { return _controller; }
    const ss::abort_source& as() const { return _as; }
    ss::abort_source& as() { return _as; }

protected:
    virtual ss::future<> do_produce(
      model::record_batch,
      model::partition_id,
      audit_probe&,
      ss::timer<>::time_point timeout = ss::timer<>::time_point::max()) = 0;
    virtual ss::future<> client_shutdown() = 0;
    virtual ss::future<> do_configure() = 0;

    audit_sink* sink() { return _sink; }

private:
    ss::future<> configure();
    ss::abort_source _as;
    ss::gate _gate;
    bool _is_initialized{false};
    size_t _max_buffer_size;
    ssx::semaphore _send_sem;
    audit_sink* _sink;
    cluster::controller* _controller;
    std::unique_ptr<client_probe> _probe;
};

} // namespace security::audit
