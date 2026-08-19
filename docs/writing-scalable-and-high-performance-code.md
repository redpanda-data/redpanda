# Writing scalable and high performance code

## Scale

Redpanda runs at scale and it is good to keep this in mind when writing code.
Most tests do not run at high scale, and the scale tests that do run have
limited coverage. So we cannot expect all scale issues to be caught by testing
and proactive scale-sensitive choices can go a long way towards mitigating that.

Some specific things to keep in mind when writing for scale, such as what data
structure to use, are very important, and these are addressed in subsections
below.

### Memory allocation limits (128K rule)

**As a guideline, do not allocate more than 128 KiB of contiguous memory in
redpanda after startup.**

Allocations larger than 128 KiB have a relatively high likelihood of failing
allocation even when substantial free memory is available due to
seastar-specific fragmentation. These failures won’t usually occur in our tests
(except perhaps very long running ones), but will fail at some inopportune
moment at an important customer.

128 KiB is not a magic number that it is guaranteed to work, but it is small
enough that we *believe* it won’t be a significant problem even in fragmented
heaps and it aligns with the existing sizes of some very common allocations such
as `iobuf` fragments.

The most common causes of large contiguous allocations are large arrays or array
like containers (such as `std::vector`, or the use of containers that are backed
by those types of containers, such as `std::unordered_map` or
`absl::flat_hash_map`).

#### Problematic containers

Containers below make large contiguous allocations and should be avoided if the
container size could reasonably exceed the 128K rule.

| **Container**                                       | **Approximate contiguous allocation size** | **Notes**                                                                                                                                                                                                                                                                                                                      |
| --------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `std::vector<T>`                                    | `2 * size() * sizeof(T)`                   | Factor of two comes from the resize step: the size doubles so will contain `2 * size()` at that point. In some cases you remove this factor of two when you know the final size and use `reserve()` or `resize()`.                                                                                                             |
| `std::unordered_map<K, V>`, `std::unordered_set<K>` | `16 * size()`                              | Factor of 16 comes from `2 * sizeof(void *)` where 2 is due to the doubling during growth (so half the buckets will be empty) and `sizeof(void *)` reflects that these are closed-addressed hash maps which have a single pointer per bucket. The size of the contiguous allocation does not depend on the size of `K` or `V`. |
| `absl::flat_hash_map<K, V>`                         | `2.3 * (sizeof(K) + sizeof(V)) * size()`   | The contiguous allocation comes from the key/value array, as the separate control array is generally much smaller. The factor of 2.3 comes from ~1.15 size → capacity ratio (due to load factor) times 2 for doubling.                                                                                                         |
| `absl::node_hash_map<K, V>`                         | `2.3 * (8 + 1) * size()` (? untested)      | Similar to `std::unordered_map`, except an additional 1 byte per slot (the `+ 1` part) for control byte, and a `1.15x` factor due to load factor.                                                                                                                                                                              |

If all else fails (e.g., a third party lib really wants big contiguous
allocations), make the allocations at startup, when large contiguous blocks are
easy to obtain.

### Preferred datastructures:

Prefer these data structures as they are resistant to oversized allocations and
memory fragmentation.

| **Datastructure** | **Preferred**      | **Caveats**                                                                                              | **Alternatives**                                                                                                                                                                                                                                                                                                                                                   |
| ----------------- | ------------------ | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Array, vector     | `chunked_vector`   |                                                                                                          | `std::deque`: Fine from oversized allocation perspective. The chunk size cannot be controlled. As soon as 1 element is added, the full chunk is allocated, making it a memory hog in scenarios where you have one per X (for example partition, topic etc.) as the overhead will be large.`boost::deque`: as above, but at least the chunk size can be controlled. |
| Unordered Map     | `chunked_hash_map` | Insert and erase invalidates all iterators: Don’t do `map.erase(it++)`, instead do `it = map.erase(it)`. | `absl::node_hash_map`: Useful if pointer stability is needed (try working around first). Don't use if scaling with partitions etc as this will suffer from oversized allocs                                                                                                                                                                                        |
| Ordered Map       | `absl::btree_map`  |                                                                                                          | `std::map`: Use if pointer stability is needed (probably can work around that by using std::unique_ptr values in the map)                                                                                                                                                                                                                                          |

### Managing Concurrency in the system

Seastar makes it very easy to write async and concurrent code and thus hide IO
latency. This allows for writing high performing code.

At the same time care needs to be taken to not introduce too much concurrency
into the system. Each concurrent task comes with memory usage that depending on
the task can be relatively large and if many of those tasks are in the system
waiting it can lead to OOM.

Think about how much concurrency is needed to adequately hide latency. For
sending large chunks of data to a local service with low latency not much
concurrency is needed, e.g.: uploading chunks of 128KiB to local S3 you can get
very good throughput on a single connection so not much concurrency is needed.
Writing 4KiB blocks to EBS will need higher concurrency to get good throughput.

Keep in mind that effectively most things are shard local so often higher
concurrency is already induced by having multiple shards.

There is also various patterns to manage concurrency and memory usage.

#### Limit the concurrency for a certain operation

Use `ss::max_concurrent_for_each` to limit max concurrency. Concurrency larger
than 100 is probably never a good idea, most of the time 10 is probably
approximately right but there is no hard rule. Keep above advice in mind.

Avoid using `ss::parallel_for_each` for ranges that can grow large such as
segments, topics or partitions.

Be aware of implicitly nested `ss::max_concurrent_for_each` usages. If it’s a
concern use a `ssx::semaphore` to better limit active concurrency.

#### Limit memory usage for a certain class of operations

An alternative approach if tasks themselves are not an issue is to limit memory
usage directly. We generally do this by using a `ssx::semaphore` which counts
the total available memory for this operation. Tasks try to draw from this
before they are allowed to execute and otherwise have to wait.

This is generally the better way to limit concurrency but can be hard to limit
if it’s hard to estimate how much memory a task will need or the memory need
isn’t known a-priori at all. Be careful in those cases as task throughput can be
starved if memory estimation is wrong.

## Performance

This section is a stub.

Unlike scale, you don’t need to design most of the code you write for the
highest performance (i.e., doing the required job as fast as possible). Most
code is cold or very cold. Even in hot paths, simple, straightforward code
should be the default.

Only in specific cases where profiling information shows a hotspot, or you have
solid reasoning to believe you are writing hot code, then some deviation from
“simple, straightforward” might be appropriate.

That disclaimer aside, here is a short list of (unsorted) performance notes:

### Miscellaneous Performance Notes

#### iobuf is not super cheap to copy or share

`iobuf` can be copied in two different ways: `iobuf::copy()` which creates a
byte-wise copy of all underlying buffers, or `iobuf::share()` which creates a
“logical” copy that shares the underlying buffers and so avoids a byte-wise
copy. Both of these can be relatively expensive: `copy()` in the obvious way
that it effective needs to do a `memcpy` of the entire buffer, and `share()`
because it needs to do an atomic operation and an allocation (though this is
O(1) work, so less relevant for larger buffers).

This means that *if* you have a sync function which takes an `iobuf` and does
not mutate it, just take it as a `const iobuf&` instead of by-value (followed by
moves) which is a pattern which occurs often in our codebase.
