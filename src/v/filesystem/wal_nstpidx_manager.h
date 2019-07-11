#pragma once

#include "filesystem/wal_reader_node.h"
#include "filesystem/wal_requests.h"
#include "filesystem/wal_writer_node.h"
#include "seastarx.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/reactor.hh>

#include <smf/log.h>
#include <smf/macros.h>

/// \brief manages one active writer and an index to all the readers
/// for this partition
class wal_nstpidx_manager {
public:
    wal_nstpidx_manager(
      wal_opts otps,
      const wal_topic_create_request* create_props,
      wal_nstpidx idx,
      sstring work_directory);

    wal_nstpidx_manager(wal_nstpidx_manager&& o) noexcept;
    ~wal_nstpidx_manager();

    /// \brief appends write to a log segment
    future<std::unique_ptr<wal_write_reply>>
    append(wal_write_request r);

    /// \brief performs a read on the correct log_segment
    future<std::unique_ptr<wal_read_reply>> get(wal_read_request);

    /// \brief forces the log_segment to rotate and start a new
    /// log segment with <epoch>.<term>.log
    future<> set_tem(int64_t term);

    /// \brief topic-partition stats of offsets
    /// and log segments
    std::unique_ptr<wal_partition_statsT> stats() const;

    /// \brief opens the directory, and performs eager
    /// indexing
    future<> open();

    /// \brief closes the *ALL* readers and writer
    future<> close();

    SMF_DISALLOW_COPY_AND_ASSIGN(wal_nstpidx_manager);

public:
    const wal_opts opts;
    const wal_topic_create_request* tprops;
    const wal_nstpidx idx;
    const sstring work_dir;

private:
    wal_writer_node_opts default_writer_opts();
    future<> create_log_handle_hook(sstring);
    future<> segment_size_change_hook(sstring, int64_t);
    void cleanup_timer_cb_log_segments();

private:
    std::unique_ptr<wal_writer_node> _writer = nullptr;
    std::deque<std::unique_ptr<wal_reader_node>> _nodes;
    timer<> log_cleanup_timeout_;

private:
    struct nstpidx_mngr_stats {
        uint64_t read_bytes{0};
        uint64_t write_bytes{0};

        uint32_t write_reqs{0};
        uint32_t read_reqs{0};
        uint32_t log_segment_rolls{0};
    };

    // metrics
    nstpidx_mngr_stats prometheus_stats_;
    metrics::metric_groups _metrics{};
};
