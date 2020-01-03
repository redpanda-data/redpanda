#pragma once

#include "model/fundamental.h"
#include "storage2/partition.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/iterator_range.hpp>

#include <filesystem>
#include <functional>
#include <vector>

namespace storage {

/**
 * The entry point to storage API.
 *
 * This class represents the base directory where all redpanda data is stored.
 * Its meant to be instantiated first thing when the system start and its
 * initialization logic is blocking.
 */
class repository {
private:
    using ntp_predicate = std::function<bool(const model::ntp&)>;

public:
    /**
     * The underlying ntp tuple collection type.
     */
    using ntps_type = std::vector<model::ntp>;

    /**
     * This type represents a pair of iterator to a subset of all known
     * ntp tuples in the system. Once <ranges> gets implemented in our
     * supported compilers, we should migrate to std::ranges.
     */
    using ntp_range = boost::filtered_range<ntp_predicate, ntps_type>;

public:
    /**
     * A set of optional configuration settings for the repository.
     *
     * most of the time, using default values is the recommended thing
     * to do.
     */
    struct config {
        using lazy_loading = bool_class<struct lazy_loading_tag>;
        using sanitize_files = bool_class<struct sanitize_files_tag>;

        /**
         * When a log segment reaches this size it will be closed
         * and new writes will be rolled to a new segment.
         */
        size_t max_segment_size;

        /**
         * Used in debug builds to catch concurrency problems.
         * see utils/file_sanitizer.h for more info.
         */
        sanitize_files should_sanitize;

        /**
         * When enabled log-partitions and their segments will
         * be loaded only when they're first requested. That
         * includes preloading their indices and hidrating them
         * in memory.
         *
         * Should be false if the instance of the server is deployed
         * in an environment where there is a lot of historical data
         * with very long prune times.
         */
        lazy_loading enable_lazy_loading;

        /**
         * This io priority class will be inherited by all IO operations
         * issued through this repository.
         */
        io_priority_class io_priority;

        /**
         * An instance of config with sensible defaults.
         * Either hardcoded or retrieved from config file.
         */
        static config testing_defaults();
    };

public:
    static future<repository> open(sstring basedir, config cfg);

public:
    /**
     * Access to the Namespace-Topic-Partition collection on this server.
     *
     * This collection reflects the state of the filesystem directory structure
     * that is populated in the constructor. It returns a range (pair of
     * iterator) that have all the discovered ntp tuples on disk in the default
     * namespace.
     *
     * Optionally the user may narrow down the range to a specific
     * namespace or a specific topic, up to a specific partition by
     * using non-default values for the filter arguments.
     *
     * Examples:
     *   auto all_default = ntps(); // returns all ntps in the default ns
     *   auto topic1 = ntps(model::ns("default"), model::topic("topic-one"))
     *   auto t1p2 = ntps(
     *      model::ns("default"),
     *      model::topic("topic-one"),
     *      model::partition_id(2));
     *
     * for (auto ntp: ntps()) { ... }
     * for (auto ntp: t1p2) { ... }
     */
    ntp_range ntps(
      model::ns namespace_filter = model::ns("default"),
      model::topic topic_filter = model::topic_view(),
      model::partition_id partition_filter = model::partition_id(-1));

    /**
     * Asynchronously opens a log partition.
     * Opening a log partition means enumerating available segments,
     * loading its indices into memory and getting ready for read/write
     * operations on that partition.
     */
    future<partition> open_ntp(model::ntp);

    /**
     *  Creates a namespace-topic-partition and returns an open partition.
     *
     * This method will create the filesystem structure for one partition,
     * and return an open partition object pointing at the just created ntp.
     */
    future<partition> create_ntp(model::ntp);

    /**
     * Helper function. Creates an entire range of ntps with a
     * given topic name.
     *
     * This function is equivalent to calling create_ntp from 0
     * to partitions.
     */
    future<std::vector<partition>>
    create_topic(model::ns, model::topic name, size_t partitions);

    /**
     * Deletes a partition and all its data from the current repository.
     * This operation is nonreversable. not implemented yet.
     */
    future<> remove_ntp(model::ntp ntp);

    /**
     * Getter for the path used as the root working directory.
     *
     * This is where the top-level children are namespaces,
     * the second level children topic, third are partitions
     * and leafs are segment files and indices.
     */
    const std::filesystem::path& working_directory() const;

    /**
     * Closes and releases all partitions that were open during the lifetime
     * of this instance.
     *
     * This method most likely will never be called unless we expect the system
     * to support graceful shutdown.
     */
    future<> close();

    /**
     * Gets a read-only view of the config values used to initialize
     * this repository.
     */
    const config& configuration() const;

private:
    class impl;
    shared_ptr<impl> _impl;

private:
    /**
     * Initializes a data repository with a given working directory and configs.
     *
     * This is a not a blocking call but an expensive one and it should only
     * be called once, very early in the process start.
     *
     * It will enumerate all directories inside the working directory and build
     * the ntps() data structure. Also if lazy loading is turned off, then this
     * constructor will trigger the hidration of all log segment caches.
     */
    repository(shared_ptr<impl> impl);
};

} // namespace storage