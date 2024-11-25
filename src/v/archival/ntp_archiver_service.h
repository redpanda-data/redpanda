/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "archival/archival_policy.h"
#include "archival/probe.h"
#include "archival/scrubber.h"
#include "archival/types.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client.h"
#include "cluster/fwd.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "storage/fwd.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

namespace archival {

// Forward declaration for test class that we will befriend
class archival_fixture;

using namespace std::chrono_literals;

enum class segment_upload_kind { compacted, non_compacted };

std::ostream& operator<<(std::ostream& os, segment_upload_kind upload_kind);

class ntp_archiver_upload_result {
public:
    ntp_archiver_upload_result() = default;
    ntp_archiver_upload_result(const ntp_archiver_upload_result&) = default;
    ntp_archiver_upload_result(ntp_archiver_upload_result&&) = default;
    ntp_archiver_upload_result& operator=(const ntp_archiver_upload_result&)
      = default;
    ntp_archiver_upload_result& operator=(ntp_archiver_upload_result&&)
      = default;
    ~ntp_archiver_upload_result() = default;

    /// Result without the value (error or success)
    ntp_archiver_upload_result(cloud_storage::upload_result); // NOLINT

    /// Merge series of results into one.
    ///
    /// The 'results' list shouldn't be empty.
    /// At least one of the results should have record stats.
    static ntp_archiver_upload_result
    merge(const std::vector<ntp_archiver_upload_result>& results);

    /// Success result with value
    explicit ntp_archiver_upload_result(
      const cloud_storage::segment_record_stats& m);

    /// Check if segment meta is present
    bool has_record_stats() const;

    /// Extract segment stats
    const cloud_storage::segment_record_stats& record_stats() const;

    /// Extract operation upload_result
    cloud_storage::upload_result result() const;

    /// Convert to upload_result
    cloud_storage::upload_result operator()(cloud_storage::upload_result) const;

private:
    std::optional<cloud_storage::segment_record_stats> _stats;
    cloud_storage::upload_result _result{cloud_storage::upload_result::success};
};

/// This class performs per-ntp archival workload. Every ntp can be
/// processed independently, without the knowledge about others. All
/// 'ntp_archiver' instances that the shard possesses are supposed to be
/// aggregated on a higher level in the 'archiver_service'.
///
/// The 'ntp_archiver' is responsible for manifest manipulations and
/// generation of per-ntp candidate set. The actual file uploads are
/// handled by 'archiver_service'.
///
/// Note that archiver uses initial revision of the partition, not the
/// current one. The revision of the partition can change when the partition
/// is moved between the nodes. To make all object names stable inside
/// the S3 bucket we're using initial revision. The revision that the
/// topic was assigned when it was just created.
class ntp_archiver {
public:
    /// Iterator type used to retrieve candidates for upload
    using back_insert_iterator
      = std::back_insert_iterator<std::vector<segment_name>>;

    /// Create new instance
    ///
    /// \param ntp is an ntp that archiver is responsible for
    /// \param conf is an S3 client configuration
    /// \param remote is an object used to send/recv data
    /// \param svc_probe is a service level probe (optional)
    ntp_archiver(
      const storage::ntp_config& ntp,
      ss::lw_shared_ptr<const configuration> conf,
      cloud_storage::remote& remote,
      cloud_storage::cache& c,
      cluster::partition& parent,
      ss::shared_ptr<cloud_storage::async_manifest_view> amv);

    /// Spawn background fibers, which depending on the mode (read replica or
    /// not) will either do uploads, or periodically read back the manifest.
    ss::future<> start();

    /// Stop archiver.
    ///
    /// \return future that will become ready when all async operation will be
    /// completed
    ss::future<> stop();

    /// Get NTP
    const model::ntp& get_ntp() const;

    /// Get revision id
    model::initial_revision_id get_revision_id() const;

    const cloud_storage_clients::bucket_name& get_bucket_name() const;

    /// Get time when partition manifest was last uploaded
    const ss::lowres_clock::time_point get_last_manfiest_upload_time() const {
        return _last_manifest_upload_time;
    }

    /// Get time when a data segment was last uploaded
    const ss::lowres_clock::time_point get_last_segment_upload_time() const {
        return _last_segment_upload_time;
    }

    /// Get the timestamp of the last successful manifest sync.
    /// Returns nullopt if not in read replica mode
    std::optional<ss::lowres_clock::time_point> get_last_sync_time() const;

    /// Download manifest from pre-defined S3 locatnewion
    ///
    /// \return future that returns true if the manifest was found in S3
    ss::future<std::pair<
      cloud_storage::partition_manifest,
      cloud_storage::download_result>>
    download_manifest();

    struct upload_group_result {
        size_t num_succeeded;
        size_t num_failed;
        size_t num_cancelled;

        auto operator<=>(const upload_group_result&) const = default;
    };

    // The result of a group of parallel uploads
    struct batch_result {
        upload_group_result non_compacted_upload_result;
        upload_group_result compacted_upload_result;

        auto operator<=>(const batch_result&) const = default;
    };

    /// Compute the maximum offset that is safe to be uploaded to the cloud.
    ///
    /// It must be guaranteed that this offset is monotonically increasing/
    /// can never go backwards. Otherwise, the local and cloud logs will
    /// diverge leading to undefined behavior.
    model::offset max_uploadable_offset_exclusive() const;

    /// \brief Upload next set of segments to S3 (if any)
    /// The semaphore is used to track number of parallel uploads. The method
    /// will pick not more than '_concurrency' candidates and start
    /// uploading them.
    ///
    /// \param unsafe_max_offset_override_exclusive Overrides the maximum offset
    ///        that can be uploaded. ONLY FOR TESTING. It is not clamped to a
    ///        safe value/committed offset as some tests work directly with
    ///        segments bypassing the raft thus not advancing the committed
    ///        offset.
    /// \return future that returns number of uploaded/failed segments
    virtual ss::future<batch_result> upload_next_candidates(
      std::optional<model::offset> unsafe_max_offset_override_exclusive
      = std::nullopt);

    ss::future<cloud_storage::download_result> sync_manifest();

    uint64_t estimate_backlog_size();

    /// \brief Probe remote storage and truncate the manifest if needed
    ss::future<std::optional<cloud_storage::partition_manifest>>
    maybe_truncate_manifest();

    /// \brief Perform housekeeping operations.
    ss::future<> housekeeping();

    /// \brief Advance the start offest for the remote partition
    /// according to the retention policy specified by the partition
    /// configuration. This function does *not* delete any data.
    ss::future<> apply_retention();

    /// \brief Remove segments that are no longer queriable by:
    /// segments that are below the current start offset and segments
    /// that have been replaced with their compacted equivalent.
    ss::future<> garbage_collect();

    /// \brief Advance the archive start offset for the remote partition
    /// according to the retention policy specified by the partition
    /// configuration. This function does *not* delete any data.
    ss::future<> apply_archive_retention();

    /// \brief Remove segments and manifests below the archive_start_offset.
    ss::future<> garbage_collect_archive();

    /// \brief If the size of the manifest exceeds the limit
    /// move some segments into a separate immutable manifest and
    /// upload it to the cloud. After that the archive_start_offset
    /// has to be advanced and segments could be removed from the
    /// STM manifest.
    ss::future<> apply_spillover();

    virtual ~ntp_archiver() = default;

    /**
     * Partition 0 carries a copy of the topic configuration, updated by
     * the controller, so that its archiver can make updates to the topic
     * manifest in cloud storage.
     *
     * When that changes, we are notified, so that we may re-upload the
     * manifest if needed.
     */
    void notify_topic_config() { _topic_manifest_dirty = true; }

    /**
     * If the group has a leader (non-null argument), and it is ourselves,
     * then signal _leader_cond to prompt the upload loop to stop waiting.
     */
    void notify_leadership(std::optional<model::node_id>);

    /**
     * Get list of all housekeeping jobs for the ntp
     *
     * The list includes adjacent segment merging but may be extended in
     * the future. The references are guaranteed to have the same lifetime
     * as ntp_archiver_service object itself.
     */
    std::vector<std::reference_wrapper<housekeeping_job>>
    get_housekeeping_jobs();

    /// The user supplied function that can be used to scan the
    /// state of the archiver and return an adjacent_segment_run.
    ///
    /// \param local_start_offset is a start offset of the raft group of
    ///        the partition
    /// \param manifest is a manifest instance stored in the archival
    ///        STM
    /// \return nullopt or initialized adjacent_segment_run
    using manifest_scanner_t
      = ss::noncopyable_function<std::optional<adjacent_segment_run>(
        model::offset local_start_offset,
        const cloud_storage::partition_manifest& manifest)>;

    /// Find upload candidate
    ///
    /// Depending on the output of the 'scanner' the upload candidate
    /// might be local (it will contain a list of segments in candidate.segments
    /// and a list of locks) or remote (it will contain a list of paths in
    /// candidate.remote_segments).
    ///
    /// \param scanner is a user provided function used to find upload candidate
    /// \return {nullopt, nullopt} or the archiver lock and upload candidate
    ss::future<std::pair<
      std::optional<ssx::semaphore_units>,
      std::optional<upload_candidate_with_locks>>>
    find_reupload_candidate(manifest_scanner_t scanner);

    /**
     * Upload segment provided from the outside of the ntp_archiver.
     *
     * The method can be used to upload segments stored locally in the
     * redpanda data directory or remotely in cloud storage.
     *
     * \param archiver_units are the units for the archiver that must have
     *        already been taken
     * \param candidate is an upload candidate
     * \param source_rtc is used to pass retry_chain_node that belongs
     *        to the caller. This way the caller can use its own abort_source
     *        and also filter out the notifications generated by the 'upload'
     *        call that it triggers.
     * \return true on success and false otherwise
     */
    ss::future<bool> upload(
      ssx::semaphore_units archiver_units,
      upload_candidate_with_locks candidate,
      std::optional<std::reference_wrapper<retry_chain_node>> source_rtc);

    /// Return reference to partition manifest from archival STM
    const cloud_storage::partition_manifest& manifest() const;

    /// Get segment size for the partition
    size_t get_local_segment_size() const;

    /// Ahead of a leadership transfer, finish any pending uploads and stop
    /// the upload loop, so that we do not leave orphan objects behind if
    /// a leadership transfer happens between writing a segment and writing
    /// the manifest.
    /// \param timeout: block for this long waiting for uploads to finish
    ///                 before returning.  Return false if timeout expires.
    /// \return true if uploads have cleanly quiesced within timeout.
    ss::future<bool> prepare_transfer_leadership(ss::lowres_clock::duration);

    /// After a leadership transfer attempt (whether it proceeded or not),
    /// permit this archiver to proceed as normal: if it is still the leader
    /// it will resume uploads.
    void complete_transfer_leadership();

    const storage::ntp_config& ntp_config() const {
        return _parent.log()->config();
    }

    /// If we have a projected manifest clean offset, then flush it to
    /// the persistent stm clean offset.
    ss::future<> flush_manifest_clean_offset();

    /// Upload manifest to the pre-defined S3 location
    ss::future<cloud_storage::upload_result> upload_manifest(
      const char* upload_ctx,
      std::optional<std::reference_wrapper<retry_chain_node>> source_rtc
      = std::nullopt);

    /// Attempt to upload topic manifest.  Does not throw on error.  Clears
    /// _topic_manifest_dirty on success.
    ss::future<> upload_topic_manifest();

    /// Waits for this node to become leader, syncing archival metadata within
    /// the current term. Returns false if the sync was not successful.
    /// Must be called before updating archival metadata.
    ss::future<bool> sync_for_tests();

    ss::future<std::error_code> process_anomalies(
      model::timestamp scrub_timestamp,
      std::optional<model::offset> last_scrubbed_offset,
      cloud_storage::scrub_status status,
      cloud_storage::anomalies detected);

    ss::future<std::error_code> reset_scrubbing_metadata();

private:
    // Labels for contexts in which manifest uploads occur. Used for logging.
    static constexpr const char* housekeeping_ctx_label = "housekeeping";
    static constexpr const char* sync_local_state_ctx_label
      = "sync_local_state";
    static constexpr const char* post_add_segs_ctx_label
      = "made_dirty_post_add_segments";
    static constexpr const char* concurrent_with_segs_ctx_label
      = "was_dirty_concurrent_with_segments";
    static constexpr const char* upload_loop_epilogue_ctx_label
      = "upload_loop_epilogue";
    static constexpr const char* upload_loop_prologue_ctx_label
      = "upload_loop_prologue";
    static constexpr const char* segment_merger_ctx_label
      = "adjacent_segment_merger";

    /// Delete objects, return true on success and false otherwise
    ss::future<bool>
    batch_delete(std::vector<cloud_storage_clients::object_key> paths);

    ss::future<bool> do_upload_local(
      upload_candidate_with_locks candidate,
      std::optional<std::reference_wrapper<retry_chain_node>> source_rtc);
    ss::future<bool> do_upload_remote(
      upload_candidate_with_locks candidate,
      std::optional<std::reference_wrapper<retry_chain_node>> source_rtc);
    /// Information about started upload
    struct scheduled_upload {
        /// The future that will be ready when the segment will be fully
        /// uploaded.
        /// NOTE: scheduled future may hold segment read locks.
        std::optional<ss::future<ntp_archiver_upload_result>> result;
        /// Last offset of the uploaded segment or part
        model::offset inclusive_last_offset;
        /// Segment metadata
        std::optional<cloud_storage::partition_manifest::segment_meta> meta;
        /// Name of the uploaded segment
        std::optional<ss::sstring> name;
        /// Offset range convered by the upload
        std::optional<model::offset> delta;
        /// Contains 'no' if the method can be called another time or 'yes'
        /// if it shouldn't be called (if there is no data to upload).
        /// If the 'stop' is 'no' the 'result' might be 'nullopt'. In this
        /// case the upload is not started but the method might be called
        /// again anyway.
        ss::stop_iteration stop;
        segment_upload_kind upload_kind;
    };

    using allow_reuploads_t = ss::bool_class<struct allow_reupload_tag>;
    /// An upload context represents a range of offsets to be uploaded. It
    /// will search for segments within this range and upload them, it also
    /// carries some context information like whether re-uploads are
    /// allowed.
    struct upload_context {
        /// The kind of segment being uploaded
        segment_upload_kind upload_kind;
        /// The next scheduled upload will start from this offset
        model::offset start_offset;
        /// Uploads will stop at this offset
        model::offset end_offset_exclusive;
        /// Controls checks for reuploads, compacted segments have this
        /// check disabled
        allow_reuploads_t allow_reuploads;
        /// Archiver term of the upload
        model::term_id archiver_term;
    };

    ss::future<scheduled_upload> do_schedule_single_upload(
      upload_candidate_with_locks, model::term_id, segment_upload_kind);

    /// Start upload without waiting for it to complete
    ss::future<scheduled_upload>
    schedule_single_upload(const upload_context& upload_ctx);

    /// Start all uploads
    ss::future<std::vector<scheduled_upload>>
    schedule_uploads(model::offset max_offset_exclusive);

    ss::future<std::vector<scheduled_upload>>
    schedule_uploads(std::vector<upload_context> loop_contexts);

    /// Wait until all scheduled uploads will be completed
    ///
    /// Update the probe and manifest
    ss::future<ntp_archiver::batch_result> wait_all_scheduled_uploads(
      std::vector<ntp_archiver::scheduled_upload> scheduled);

    /// Waits for scheduled segment uploads. The uploaded segments could be
    /// compacted or non-compacted, the actions taken are similar in both
    /// cases with the major difference being the probe updates done after
    /// the upload.
    ss::future<ntp_archiver::upload_group_result> wait_uploads(
      std::vector<scheduled_upload> scheduled,
      segment_upload_kind segment_kind,
      bool inline_manifest);

    /// Upload individual segment to S3.
    ///
    /// \param archiver_term is a current term of the archiver
    /// \param candidate is an upload candidate
    /// \param segment_read_locks protects the underlying segment(s) from being
    ///        deleted while the upload is in flight.
    /// \param stream is a stream to the segment used for the initial upload. If
    /// the upload is retried, the segment will be read again.
    /// \param source_rtc
    /// is a retry_chain_node of the caller, if it's set
    ///        to nullopt own retry chain of the ntp_archiver is used
    /// \return error code
    ss::future<ntp_archiver_upload_result> upload_segment(
      model::term_id archiver_term,
      upload_candidate candidate,
      std::vector<ss::rwlock::holder> segment_read_locks,
      std::optional<std::reference_wrapper<retry_chain_node>> source_rtc
      = std::nullopt);

    /// Isolates segment upload and accepts a stream reference, so that if the
    /// upload fails the exception can be handled in the caller and the stream
    /// can be closed.
    ss::future<cloud_storage::upload_result> do_upload_segment(
      const remote_segment_path& path,
      upload_candidate candidate,
      ss::input_stream<char> stream,
      std::optional<std::reference_wrapper<retry_chain_node>> source_rtc
      = std::nullopt);

    /// Get aborted transactions for upload
    ///
    /// \return list of aborted transactions
    ss::future<fragmented_vector<model::tx_range>>
    get_aborted_transactions(upload_candidate candidate);

    /// Upload segment's transactions metadata to S3.
    ///
    /// \return error code
    ss::future<ntp_archiver_upload_result> upload_tx(
      model::term_id archiver_term,
      upload_candidate candidate,
      fragmented_vector<model::tx_range> tx,
      std::optional<std::reference_wrapper<retry_chain_node>> source_rtc
      = std::nullopt);

    struct make_segment_index_result {
        cloud_storage::offset_index index;
        cloud_storage::segment_record_stats stats;
    };

    /// Builds a segment index from the supplied input stream.
    ///
    /// \param base_rp_offset The starting offset for the index to be created
    /// \param ctxlog For logging
    /// \param index_path The path to which the index will be uploaded, used
    /// only for logging
    /// \param stream The data stream from which the index is created
    /// \return An index on success, nullopt on failure
    ss::future<std::optional<make_segment_index_result>> make_segment_index(
      model::offset base_rp_offset,
      model::timestamp base_timestamp,
      retry_chain_logger& ctxlog,
      std::string_view index_path,
      ss::input_stream<char> stream);

    /// Upload manifest if it is dirty.  Proceed without raising on issues,
    /// in the expectation that we will be called again in the main upload loop.
    /// Returns true if something was uploaded (_projected_manifest_clean_at
    /// will have been updated if so)
    ss::future<bool> maybe_upload_manifest(const char* upload_ctx);

    /// Lazy variant of flush_manifest_clean_offset, for use in situations
    /// where we didn't just upload the manifest, but want to make sure we
    /// will eventually flush projected_manifest_clean_at in case it was
    /// set by some background operation.
    ss::future<> maybe_flush_manifest_clean_offset();

    /// While leader, within a particular term, keep trying to upload data
    /// from local storage to remote storage until our term changes or
    /// our abort source fires.
    ss::future<> upload_until_term_change();

    /// Outer loop to keep invoking upload_until_term_change until our
    /// abort source fires.
    ss::future<> upload_until_abort();

    /// Periodically try to download and ingest the remote manifest until
    /// our term changes or abort source fires
    ss::future<> sync_manifest_until_term_change();

    /// Outer loop to keep invoking sync_manifest_until_term_change until our
    /// abort source fires.
    ss::future<> sync_manifest_until_abort();

    /// Delete a segment and its transaction metadata from S3.
    /// The transaction metadata is only deleted if the segment
    /// deletion was successful.
    ///
    /// Throws if an abort was requested.
    ss::future<cloud_storage::upload_result>
    delete_segment(const remote_segment_path& path);

    /// If true, we are holding up trimming of local storage
    bool local_storage_pressure() const;

    void update_probe();

    /// Return true if archival metadata can be replicated.
    /// This means that the replica is a leader, the term did not
    /// change and the archiver is not stopping.
    bool can_update_archival_metadata() const;

    /// Return true if it is permitted to start new uploads: this
    /// requires can_update_archival_metadata, plus that we are
    /// not paused.
    bool may_begin_uploads() const;

    /// Returns true if retention should remove data from STM
    /// region of the log and false if it should work on archive
    /// part.
    bool stm_retention_needed() const;

    /// Helper to generate a segment path from candidate
    remote_segment_path segment_path_for_candidate(
      model::term_id archiver_term, const upload_candidate& candidate);

    /// Method to use with lazy_abort_source
    std::optional<ss::sstring> upload_should_abort();

    // Adjacent segment merging

    std::vector<upload_candidate> get_local_adjacent_small_segments();

    model::ntp _ntp;
    model::initial_revision_id _rev;
    cloud_storage::remote& _remote;
    cloud_storage::cache& _cache;
    cluster::partition& _parent;
    model::term_id _start_term;
    archival_policy _policy;
    std::optional<cloud_storage_clients::bucket_name> _bucket_override;
    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtcnode;
    retry_chain_logger _rtclog;

    // Ensures that operations on the archival state are only performed by a
    // single driving fiber (archiver loop, housekeeping job, etc) at a time.
    //
    // NOTE: must be taken before doing anything that acquires segment read
    // locks (e.g. selecting segments for upload, replicating and waiting on
    // the underlying Raft STM).
    ssx::semaphore _mutex{1, "archive/ntp"};

    ss::lw_shared_ptr<const configuration> _conf;
    config::binding<std::chrono::milliseconds> _sync_manifest_timeout;
    config::binding<size_t> _max_segments_pending_deletion;
    simple_time_jitter<ss::lowres_clock> _backoff_jitter{100ms};
    size_t _concurrency{4};

    // When we last wrote the partition manifest to object storage: this
    // is used to limit the frequency with which we do uploads.
    ss::lowres_clock::time_point _last_manifest_upload_time;

    // When we last wrote the last_clean_at attribute of archival_metadata_stm,
    // this is used to avoid emitting a mark_clean record to the raft log
    // every time we upload a manifest, as under load we can always inline
    // these writes with add_segments records.
    ss::lowres_clock::time_point _last_marked_clean_time;

    // When we last wrote a segment
    ss::lowres_clock::time_point _last_segment_upload_time;
    ss::lowres_clock::time_point _last_upload_time;

    // When we last synced the manifest of the read replica
    std::optional<ss::lowres_clock::time_point> _last_sync_time;

    // Used during leadership transfer: instructs the archiver to
    // not proceed with uploads, even if it has leadership.
    bool _paused{false};

    // Held while the inner segment upload/manifest sync loop is running,
    // to enable code that uses _paused to wait until ongoing activity
    // has stopped.
    ssx::named_semaphore<ss::lowres_clock> _uploads_active{1, "uploads_active"};

    config::binding<std::chrono::milliseconds> _housekeeping_interval;
    simple_time_jitter<ss::lowres_clock> _housekeeping_jitter;
    ss::lowres_clock::time_point _next_housekeeping;

    // 'dirty' in the sense that we need to do another update to S3 to ensure
    // the object matches our local topic configuration.
    bool _topic_manifest_dirty{false};

    // While waiting for leadership, wait on this condition variable. It will
    // be triggered by notify_leadership.
    ss::condition_variable _leader_cond;

    std::optional<ntp_level_probe> _probe{std::nullopt};

    ss::sharded<features::feature_table>& _feature_table;

    // NTP level adjacent segment merging job
    std::unique_ptr<housekeeping_job> _local_segment_merger;

    // NTP level scrubbing job
    std::unique_ptr<scrubber> _scrubber;

    // The archival metadata stm has its own clean/dirty mechanism, but it
    // is expensive to persistently mark it clean after each segment upload,
    // if the uploads are infrequent enough to exceed manifest_upload_interval.
    // As long as leadership remains stable, we may use an in-memory clean
    // offset to track that we have uploaded manifest for a particular offset,
    // without persisting this clean offset to the stm.
    std::optional<model::offset> _projected_manifest_clean_at;

    // If this duration has elapsed since _last_manifest_upload_time,
    // then upload at the next opportunity.
    config::binding<std::optional<std::chrono::seconds>>
      _manifest_upload_interval;

    ss::shared_ptr<cloud_storage::async_manifest_view> _manifest_view;

    friend class archival_fixture;
};

} // namespace archival
