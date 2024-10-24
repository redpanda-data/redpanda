- Feature Name: Fragmentation Analysis
- Status: draft
- Start Date: 2020-12-08
- Authors: Travis Downs
- Issue: https://github.com/vectorizedio/Redpanda/issues/211

# Executive Summary

Fragmentation of the Seastar allocator, caused by a combination of factors such as inherent susceptibility of non-moving allocators to fragmentation as well as the use of opportunistic caches that can fill the available memory, can result in even modestly large allocations failing (as low as a "few MB"), and generally in a failure of the shard and process.

This RFC does not propose one specific new feature that will _solve_ fragmentation, per se. A non-moving allocator will always be subject to fragmentation. Rather, the problem is outlined and a variety of mitigations and partial solutions are presented for analysis.

The scope of this RFC is the main Redpanda binary written in C++ and using the Seastar thread-per-core event-driven framework and allocators.

## What is being proposed

The fragmentation problem is described, analyzed and finally possible mitigation or solutions are discussed.

## Why (short reason)

Fragmentation resulting in failed allocations is a serious functional (crash) and performance risk to the Redpanda cluster.

## How (short plan)

We don't yet know how, it needs to be discussed.

## Impact

Solution dependent. Specific impacts are discussed for each approach below.

# Motivation

## Why are we doing this?

Fragmentation must be understood and where possible addressed, as the symptom may be a failure of a shard, node, or the entire cluster, depending on the nature of the load impacted by fragmentation

The nature of fragmentation makes its occurrence is difficult to predict and when it occurs there may be few workarounds short of a code change. The impact can range from cascading node failures, to serious performance impacts (due to the interaction with reclaim). Furthermore, this may present a DOS (denial of service) attack vector for any party who can access a Redpanda cluster, since it is entirely plausible that the pattern of allocations can be controlled by the attacked (such control would very hard to avoid).

Fragmentation is a pervasive concern. There are many thousands of existing allocation points in the code, and these will scale proportionally as more code is added. A developer cannot be expected to think of of the consequences of fragmentation each time _potentially allocating_ code is added: there are just too many and fragmentation is too complicated. Contrast this with e.g., adding locking via a mutex: these are few enough and the rules are clear enough that we could expect good "mutex discipline". Allocation is different. Finally, many allocation sites are invisible to the developer, hidden by standard library or third party components.

Redpanda is position as a high-performance, high-availability service and fragmentation puts this at risk.

## What is the expected outcome?

A comprehensive (?) plan to manage and mitigate fragmentation. At the very least, a better understanding of fragmentation.

# Reference-level explanation

## Named Concepts

We define some terms that may not be familiar to all readers, or which I have possibly just made up on the spot, here.

**Span:** In the Seastar allocator, and allocators more generally, a _span_ is a contiguous range of 1 or more pages, treated as a unit for some operations. In the specific case of Seastar the buddy allocator uses spans of size 2^n. The `Seastar::page` struct serves as the type for both pages and spans: the first page in a span (and to a lesser extent the last) holds information for the whole span.

**Non-moving allocator:** A non-moving allocator is one which cannot move (in the virtual address space) the pointers it returns. Typical allocators in native languages, like `malloc` in C/C++ and `new` in C++ are
non-moving: they return a pointer value and the memory must remain allocated at that location for the lifetime of the pointer. This is opposed to moving (or _compacting_) allocators, which may move memory they have returned. Garbage collected languages often feature moving allocators, and such allocators are possible in native languages, e.g., using _handles_ (double indirection) rather than pointers.

**Availability %:** This is a measure of fragmentation and applies to a specific potential allocation (span) size. It is calculated as: the fraction of _free memory_ which can be used to satisfy allocations of the given size (not considering reclaim). For example, if there is 100 MiB of free memory, but only three 1 MiB blocks can be allocated before failure (or reclaim), the availability is 3% for 1 MiB spans. An availability of 0% indicates complete fragmentation at a given size.

**High Water Mark:** An indication of the highest part of the heap that has been _touched_ by allocation (assuming spans are allocated from lowest addresses first, which they are in the Seastar allocator). The high water mark is somewhat larger than simply the largest live set of allocated memory at any point in the past because of fragmentation (not all pages below the high water mark are actually filled).

**Reclaim:** The process by which the Seastar allocator requests the application to free memory, which may be asynchronous or synchronous.

## Interaction With Other Features

Fragmentation as discussed is a cross-cutting concern which interacts with almost any feature that allocates memory, particularly anything which might request large contiguous allocations, such as a large `std::vector`. The concern is pervasive because it is difficult inspect the implementation of every class to understand its allocation behavior, especially the significant amount of third party code we use, and it is also difficult to determine what the maximum expected size of any particular contiguous allocation will be, even if the allocation points are identified.

## Telemetry & Observability

Some of the solutions relate specifically to observability, and they are discussed in that section.

## Seastar Allocator Background

This section has a comprehensive(ish) description of the Seastar allocator. This is necessary because the design of the Seastar allocator is tied to behavior of fragmentation.

### Overall Approach

In its recommended configuration, Seastar uses its own custom allocator implementation, completely overriding the standard allocation routines, `malloc`, `free`, `operator new`, `operator delete` and so on. Functionally, this is generally transparent to applications built on Seastar (although good performance requires following some rules), and in fact the Seastar allocator could be disabled by defining the `Seastar_DEFAULT_ALLOCATOR` macro at compile time to use the usual allocators (we do this in debug configuration, to enable ASAN).

Al memory handling is implemented in in `memory.cc` and associated header `memory.h`. All memory for each reactor thread is tracked in a `thread_local` `cpu_pages` object.

All memory allocations are carved out of a large virtual address space allocation of size 2^44 (~17.6 terabytes), reserved at startup: each core gets an equally sized partition of 2^36 bytes (~68 GB) within that space (see `cpu_pages::initialize()` and `mem_base()`). Yes, these values are currently hard-coded, so no reactor thread can easily use more than 68 GB of memory. 2 MB hugepages are requested for the entire region with `madvise`, and the allocator also supports hugetlbfs. When the allocator is configured (see `ss::memory::configure()`), a specific amount of memory is `mmap`ed (but not `MAP_POPULATE`d) within the virtual allocation. The per-page state for each page in this allocation is tracked in the cpu_pages::pages array, which is embedded directly at the start of the memory for the core (so some number of initial pages are reserved for this purpose and never available for allocation).

Every allocator is core-local, implemented by making the core allocator structures described above (and below) `thread_local`. This means that allocations must be (internally) freed on the core on which they were allocated, and the large majority of allocations are expected to live only on the owning core. User code calling `free` on the "wrong" thread _is_ supported but is inefficient: requiring that the pointer be sent to the correct core using message passing (see `cpu_pages::free_cross_cpu` and associated methods).

The base unit of allocation is the _page_, 4096 bytes by default (can be overridden at compile time). This happens to be the same size as the smallest page size on x86, but they are not intrinsically related. For example, it would be [possible](https://github.com/scylladb/Seastar/issues/836#issuecomment-748531815) to increase the Seastar page size to 16K regardless of the OS page size.

The main entry point for all allocations is `ss::memory::allocate(size_t)`. All allocations are either _small_ or _large_, with the dividing point being hard-coded at 4 pages (16 KiB). Small allocations go to a dedicated small allocator which is _pool-based_ and is backed by the large allocator, while large allocations are serviced by the buddy allocator directly (hereafter we might use the terms _large allocator_ and _buddy allocator_ interchangeably).

We cover the large pool first, then the small pool (since some large pool concepts are useful to understand the small pool).

### Large Allocations

Large allocations use a buddy allocator system: the entire committed region of memory starts out as a single large span, and each 2^n-sized region of memory can be split into two 2^n-1 sized regions during allocation to satisfy a smaller allocation if no smaller spans are available, or two adjacent regions ("aligned", i.e., buddies) can merged into one 2^n+1 region. Those basic operations, which can be applied recursively, pretty much define the entire behavior of the buddy allocator.

Large allocations are rounded up to the next multiple of the page size (4K), and eventually end up in `cpu_pages::allocate_large_and_trim`. This function uses the free_spans list, starting with the smallest suitable span, to find the smallest free span that can fit the requested allocation. If the span chosen wasn't the smallest possible (i.e., wasn't the requested size rounded up to the next power of 2), the remaining parts of the span are split off and returned the appropriate free list. For example, if a request for a 7K allocation is satisfied by a 32K span, an 8K span is actually returned to the caller and one 8K and one 16K span comprising the salvageable unused memory are returned to the buddy allocator.

Freeing eagerly merges the returned allocation with its buddy if both are free, recursively. This means that the large heap is always in a most-merged state: the number of spans is as few as possible and they are as large as possible.

Available (free) spans are kept in linked lists (`cpu_pages::free_spans`): one for each size. When a request for a span of a given size occurs, the first free span in the list is returned, if any. Otherwise, larger lists are searched, starting with the next larger size and any found larger span will be split. Freeing adds the span to the front of the free span list (or at least it adds _some_ span, after merging). This whole paragraph is just trying to convey the idea that spans are added and removed from the free lists in _FIFO_ order: there is no particular effort expended to defragment the freelist. This means that in some cases where fragmentation could be avoided by a best-case allocator it will be not be avoided in Seastar because the freelist behavior is not fragmentation aware.

Like most userspace allocators in native language, the buddy allocator is a _non-moving_ allocator.

#### Fragmentation in the Large Allocator

Fragmentation is a problem in the large allocator. As shown in the experiments below, random allocation of smaller blocks can prevent larger allocations completely. Specifically, after the memory is filled with objects of a given size, even after ~97% of the memory is freed, allocations 7 to 8 orders larger will often not be available.

We give more details on _how_ fragmentation occurs below.

### Small Allocations

Small allocations (see `cpu_pages::allocate_small()`) are mapped onto a size class using a log-linear mapping: there are size classes for every power-of-two size, as well as three intermediate sizes between each power of two. E.g., there are 256-byte and 512-byte size classes, as well as for three intermediate sizes 320, 384 and 448 linearly spaced between those two points. Each size class maps to a pool allocator that only allocates objects of that size.

Each small pool has a _preferred_ and _fallback_ span size. The fallback size is simply the smallest span size that is equal or larger to a single object in the pool. Using this size might have a lot of waste: imagine a pool for 5120 byte objects, which would have a minimum (fallback) size of 8 KiB but this can fit only a single object and would waste 37.5% of the span. Instead a 32 KiB span is chosen as the preferred size, with a waste of only 6.25%.

Each small pool is organized as a free list of single objects as a linked list, with a target count _min_size and _max_size. Allocation requests are allocated from and freed to the free list, in an LRU fashion (i.e., the most recently freed object will be used to satisfy the next allocation). Memory for individual small pools is obtained from the large allocation path, in chunks of `_span_sizes.preferred` pages (or `_span_sizes.fallback` if that fails). This means that the smallest spans allocated by the large path are actually those needed to satisfy small pool refills, since these are as small as 1 page, whereas large-path allocations to satisfy requests directly will be at least 5 pages (since <= 4 pages are handled by the small path).

This behavior produces a significant link between small memory allocations and requested spans from the large allocator. For example, objects of 1024 (or slightly less) bytes use a preferred span of 4K, so will allocate spans of 4K. Objects of 1025 bytes (up to 1280 bytes) have a preferred span of 20K, so will allocate in a different pattern and affect the large allocator differently. Generally speaking, it seems that the small pools could reduce fragmentation by preferring larger spans of a more consistent power-of-two size.

Each small pool tries to keep the size of the free list between the `_min_free` and `_max_free` sizes: when it exceeds the max size, `trim_free_list` is called. This pops objects off the primary pool free list and adds them to a per-span freelist (`page::freelist`). If ever a span is completely freed (i.e., all the objects associated with the span end up on the per-span freelist), the entire span is returned to the buddy allocator. This is the only way memory ever escapes a given small pool.

#### Fragmentation in the Small Pool

The small pool drives allocations in the large pool, so it causes fragmentation in the same way as allocations directly to the large pool, with varying sizes as set by the preferred size for each pool.

In addition, the small pool is subject to its own kind of fragmentation, where spans used by a given small pool can never be used by allocations for objects of other sizes, if even a single object remains allocated in the span. This is different from the buddy allocator behavior, which can suffer fragmentation but where all unallocated memory is still available to other allocations of any size (as long as the span is large enough).

This could be an issue if many allocations of a specific size happen in some phase of operation, where after most but not all are freed, and then this size becomes unpopular. A large amount of memory could be used by the unpopular object size (and, of course, this process can repeat with other pool sizes). I don't consider this as serious of a problem as general buddy allocator fragmentation since (a) this same problem exists in general purpose allocators and usually doesn't cause exhaustion, probably because the allocation pattern that would cause a serious amount of waste is unusual and (b) the allocations made by the things that tend to fill up memory are large enough that they are allocated directly in the large pool.

### Reclaim

A specific feature of the Seastar allocator is that when low on memory it can make an _upcall_ to the application code to request that the application free up memory -- a so-called _reclaim_ request (see `ss::memory::reclaimer`).

Originally, this only supported an asynchronous mode, where the upcall would inform the application of the low memory condition, which would respond with a callback to be scheduled later which would try to free memory. This didn't always work well (memory might run out before the reclaim got scheduled, or the reclaim wouldn't free enough, perhaps because it didn't know how much was needed).

To help solve some of those problems a _synchronous_ mode was added: the upcall is made to get a reclaimer which is immediately invoked to attempt to free memory before the allocation attempt proceeds. Redpanda only uses synchronous reclamation, and so we only discuss this type below.

Seastar runs reclaim at two different times, described below.

First in `cpu_pages::maybe_reclaim` (called every time an allocation occurs) if the current free memory is less than `min_free_pages` (defaults to 20 MiB and Redpanda doesn't change this default) then synchronous reclaim is run once (and the result is ignored). It is important to note that the 20 MiB threshold here is just total free pages: it doesn't mean that the _high water mark_ will stay 20 MiB below the full region, because fragmentation means the entire region may be filled before the threshold is breached. In fact, memory allocation may fail (triggering the second reclaim path) before this reclaim path is ever used.

Second, when an allocation actually fails (i.e., there are no spans of sufficient size), reclaim is attempted. Unlike the first case, reclaim will occur repeatedly (see `cpu_pages::find_and_unlink_span_reclaiming`) while progress being made (as determined by the reclaim result of `reclaimed_something`) until the allocation succeeds. The allocator tells the reclaimer how much memory it needs, but even if the reclaimer satisfies the request, allocation may keep failing due to fragmentation. Specifically, the allocator needs N contiguous pages, and asks the reclaimer for N pages, but these N reclaimed pages may not be contiguous (and in general there is no reason to expect them to be). This means that before an allocation ultimately fails, it will first flush all memory subject to reclaim.

In Redpanda, there is one reclaimer: the batch cache (see `batch_cache::reclaim`). This iterates through the records in least recently used order, applying some rules about what can be reclaimed until it reclaims enough bytes. It also has rules to reclaim _more_ than the requested number of bytes, if the requested number is below some threshold, or there have been frequent reclaim requests recently.

## Experiments

### Synthetic Fragmentation Tests

I did some synthetic fragmentation tests to observe fragmentation behavior. Most of the results were mostly as expected from theory, although some interesting things did crop up due to the specific behavior of the Seastar allocator.

#### Worst Case Fragmentation

The worst case fragmentation is easy to calculate and is not specific to the buddy allocator, but applies to any _non-moving_ allocator. Fragmentation isn't a scalar value, but a spectrum that depends on the desired allocation size `S`. The worst case occurs when all free memory occurs in spans of slightly less than `S`. It is always possible to set up such a scenario: allocate all available memory with the smallest possible blocks of size `M`, then in every contiguous region of size `S` free all but one block.

This results in free to total memory ratio of `(S - M) / S` but no way to allocate any `S`-sized blocks. As an example, a block of 1 MiB may fail to allocate even when memory is 96.8% free if the used memory is in carefully spaced 32 KiB blocks.

Tests `pattern_alloc_small` and `pattern_alloc_big` illustrate this in practice, by allocating blocks in the pattern described above. The small tests allocates blocks that use the small pools and demonstrates that this problem is somehow _worse_ in the small pools: memory can be fragmented within the spans allocated to a given small pool, such that the small pool consumes a large number of pages for a small amount of actually allocated memory, and this space is only available to allocations of exactly that size. This is unlike the buddy allocator which suffers from fragmentation but at least allows any allocation that can fit in any free span to succeed.

We aren't likely to encounter _worst_ case fragmentation outside of malicious input (which is a possibility not be dismissed), but it serves as a useful upper bound on fragmentation, and is easy to demonstrate.

#### Randomized Fragmentation

As a somewhat more realistic synthetic test we can allocate most of the shard's available memory (as the batch cache tends to do) and then free a specified fraction of it at random. This is probably not actually a _terrible_ approximation to what the buddy heap will look like after a lot of uptime, as the mechanics of the buddy allocator mean that the general trend is towards more entropy in the heap.

If we fill the available memory with 32 KiB allocations, then free 96.8% of them randomly (this fraction chosen to line up with the worse case example above) we find the following availability of various chunk sizes:

~~~
Used memory:    151 M
Free memory:   3432 M
Page spans:
index   size    total   used    avail%  spans
0       4K      404K    400K    100.0   101
1       8K      280K    280K    100.0   35
2       16K     944K    928K    100.0   59
3       32K     225M    115M    100.0   7k
4       64K     212M    832K     96.8   3k
5       128K    379M    384K     90.6   3k
6       256K    619M    0B       79.6   2k
7       512K    859M    0B       61.5   2k
8       1M      770M    0B       36.5   770
9       2M      414M    2M       14.1   207
10      4M      72M     0B        2.1   18
11      8M      0B      0B        0.0   0
12      16M     0B      0B        0.0   0
13      32M     32M     32M       0.0   1
14      64M     0B      0B        0.0   0
15      128M    0B      0B        0.0   0
16      256M    0B      0B        0.0   0
[... all remaining spans empty]
~~~

In this case, unlike the worst case, we _are_ able to allocate some 1M spans: fully 36% of the free memory is available for 1M allocations. From there, however, it gets worse quickly: only 14% and 2.1% of the free memory is available for 2M and 4M spans. For 8M spans, we are out of luck: nothing is available. Yes, we have 3.4G of free memory, but can't allocate a measly 8M.

So the random case isn't too far off the worst case: it only bought us 3 additional orders of allocation. This pattern repeats itself with other parameters.

The results above follow directly from the math of random allocations. The chance that a span of length `S` is fully free when considered as made of blocks of size `M`, given that the fraction of free memory is `F`, is `F^(S/M)`. This closely matches the availability results above (the divergence is probably related to the fact that some blocks other than those allocated by the simulation are also present). In order to calculate the _expected_ number of blocks of a given size (rather than simply the availability %) we need to multiply by the total number of possible blocks (where `T` is the total memory): `F^(S/M)*T/S`. This calculation shows that the expected number of 8M blocks is about 0.1 in this scenario: so we weren't that _close_ to having an 8M block available.

## Why (Not) Fragmentation

The examples above illustrate that fragmentation can be severe and occur with only a single allocation pass followed by a de-allocation pass.

Actual fragmentation will not be so severe, at least initially. For example, allocating N blocks of N size, then freeing those N blocks will return to the original state: fragmentation did not increase. Furthermore, interleaved allocations and freeing of blocks of various sizes, e.g., in a loop, will tend to only use a small working set of spans (equal to the highest number of simultaneously live allocations for each size class) as the allocators are FIFO.

For serious (read: allocation failures) fragmentation to occur, at least the following elements need to be present:

 1. Substantially all of involved memory region needs to be filled. Not necessarily "densely" - but the high water mark of allocation needs to reach close to the highest address. That is, if you imagine spans being used starting from the lowest addresses, the high water mark is the highest address allocated. If a shard has a small working set, the high water mark will stay small, because of the FIFO nature of the buddy allocator. If the entire per-shard region hasn't been filled, allocations can't fail since the untouched spans at the top of the region will be available.
 2. There needs to be a significant difference in size of the most commonly allocated objects and the allocation that fails. As a degenerate case we can see that if all allocations of the same size, fragmentation isn't possible: every available span will be of the required size, or larger. Similarly, for spans twice as large as the common size, you only need to find _two_ adjacent spans of the smaller size that are free for allocation to succeed, which is highly probable. In general, allocations that are are within "a few" orders of the dominant allocation sizes will succeed. In real applications there won't necessarily be a specific dominant size but a spectrum of sizes, but the same principle applies.
 3. Allocations of different lifetimes need to be interleaved. Without interleaving, groups of smaller allocations will tend to act like one large block, preventing fragmentation. A large number of small allocations may occur, but if they are freed around the same time the holes that cause a problem won't exist. It only takes a small amount of interleaving, however, to eventually fragment memory, because once some interleaving occurs, it tends to persist as a block with a different lifetime from the surrounding blocks will be freed at a different time, end up on the free list and reallocated a different time (potentially as part of a stream of allocations that would otherwise be contiguous).
 4. The total size of the available region must be fixed. If not, fragmentation can still happen in a sense (some free regions are unusable for some allocations), but it doesn't necessarily cause allocation failure as we can simply grow the region and allocate in fresh space. This is why most 64-bit userspace processes don't suffer from fragmentation: they can simply increase their virtual memory use, without increasing their physical memory use, by remapping pages from unusable holes to new contiguous VA space as described below.
 5. The allocator is not able to compact allocated memory. For example allocators in garbage collected languages can often move memory, which allows them to avoid fragmentation simply by copying live allocations to a contiguous region, removing any holes.

This list handily explains why fragmentation isn't something that affects every application. First, a large class of non-native applications written in garbage collected languages sidestep the whole problem by moving allocated memory during collection.

Then, for native applications, fragmentation _can_ occur, but new virtual address space can simply be allocated by `mmap` (or `sbrk`). This is actually a combination of items (4) and (5) above: the total size of virtual address space is not fixed, but you'd still eventually run out of physical memory if you left the fragmented pages allocated: but the magic of physical to virtual means that physical pages can be remapped to the new virtual pages. So even though pointers can't move in the virtual address space, the underlying physical pages can move (although this is obviously restricted to page granularity).

Many native applications also don't satisfy (1): they don't use all memory available to them (which, by default, is "all" memory). The memory used might be proportional to the input, or concurrent requests, or whatever other factors. If all of memory is every used, the application might be more likely to crash due to memory exhaustion (which _is_ common in native applications) than fragmentation, since if you've used 99% of memory, the next step is probably using 101%.

### Redpanda

In the case of Redpanda, however, all of these criteria are met. Most of them flow from the design of the Seastar memory allocator: it uses a relatively small (few GB) _fixed in the virtual address space_ per-shard region (4) which is not able to move memory (5). Allocation of different sizes (3) occurs frequently for many reasons, including simply the natural behavior of the code and also because of the rapid scheduling of coroutines performing different tasks on the same shard. Filling all of memory (1) occurs naturally in Redpanda because we let the batch cache grow to fill member (why not, it would be wasted otherwise) and rely on the reclaim mechanism to free up memory as needed.

## Mitigation Design

Here we examine several possible solutions to the fragmentation problem.

We broadly group them into avoidance, allocator modifications and application (redpanda) modifications.

### Avoid

Broadly speaking, the solution here is: *don't try to avoid fragmentation, but rather try to avoid failure even when fragmentation occurs*. The main approach is to simply to _avoid large allocations_.

This is the same approach taken by the Linux kernel (which uses a buddy allocator) for `kmalloc` (the guideline is expect allocations greater than order 4 to fail and for a long time the maximum allocation size was 128 KiB) as well as ScyllaDB (see for example issues [#7364](https://github.com/scylladb/scylla/issues/7364), [#6376](https://github.com/scylladb/scylla/issues/6376), [#108](https://github.com/scylladb/scylla/issues/108), [#1546](https://github.com/scylladb/scylla/issues/1546) and many more).

The main tools to _detect_ such cases are using the large allocation warning emitted by Seastar (not currently configured in Redpanda), and the diagnostics when an allocation fails.

Once a large allocation is detected it, avoiding it depends on the context (browsing the related ScyllaDB issues shows the large variety of approaches taken). Sometimes it is as simple as using a fragmented data structure (linked list, iobuf, deque) rather than a contiguous one. In other cases it was not expected that such a structure became so large in the first place, so some other adjustment is needed.

#### Drawbacks

A significant downside of the this approach is that is mostly reactive: problems are identified only when they cause a crash or (hopefully) when the warning is detected and acted on. Although the issue may be resolved, significant downtime may already have been incurred.

Another issue is that many allocations occur in third party libraries rather than the core code: although we have the source to all third parties and can patch them, this creates a divergence with upstream, creates a maintenance burden, and so on. Furthermore, it may be harder to avoid large allocations in third party libraries since we won't the same tools available to use as

#### Recommendations

At least, we should enable the large allocation warning, perhaps at 256 KiB. We can have it enabled in testing with a post-processing phase which determines if the discovered large allocations are from new locations which need to vetted to check how large they can get.

We should expose metrics related to fragmentation such as availability % for certain sizes in our metrics and possibly in telemetry, in order to understand what "typical" fragmentation looks like.

### Make Reclaim Span-Aware

As described above, the current reclaim mechanism means that reclaimers aren't aware of the required *size* for the pending allocation. So they may repeatedly free scattered chunks of memory which can't satisfy the current allocation. In the worst case, we can free almost the entire batch cache until we finally get a large enough contiguous chunk of memory. This means that fragmentation can introduce a tricky "semi-failure" mode rather than total failure (failed allocations). In this mode, performance is badly degraded because the batch cache is continually forced to a small size in order to satisfy large allocations inefficiently.

To mitigate this, we could implement a span-aware reclaimer: the reclaim code already tells the reclaimer the size of the allocation, but now the reclaimer should try to free spans that will result in a _contiguous_ region of at least the given size (rather than just a free of scattered regions reaching this total). For example, rather than LRU eviction, use an eviction algorithm which evicts entries which happen to be contiguous, or perhaps a hybrid algorithm which considers both the contiguity and age of the entries. This could significantly reduce the impact of fragmentation since the batch cache would suffer fewer evictions while reclaiming.

One can imagine two variants of this approach: in the simplest version, the reclaimer isn't aware of which spans are free from the PoV of the allocator and tries to return a contiguous region solely by freeing memory it owns. The potential problem is that with limited visibility opportunities to combine a newly freed region and an already free region will be missed, because the reclaimer doesn't know which regions are free. A more complete solution would combine free-region information exported by the allocator with reclaimer information to find contiguous regions.

### Allow the Batch Cache to Compact Memory

In a similar vein to the mitigation discussed above, we could make the batch cache capable of *compacting* the memory it uses. By tracking the pointers into the batch cache, we could move batches which are not currently being referenced to make them contiguous. This would greatly reduce fragmentation in the batch cache, the largest user of long-lived memory. This would require having a mechanism to understand when a batch is being referenced by a in-progress request, since we cannot safely move those batches. Perhaps this is similar to shared pointer tracking.

### Modify the Allocator

Another strategy would be to modify the Seastar allocator to improve fragmentation.

Per [this discussion](https://github.com/scylladb/seastar/issues/836#issuecomment-742924680) one small thing we can do reduce worst-case fragmentation is to increase the effective minimum span size to 16K. Currently, the large allocator only handles requests of at least that size already, but for some pool sizes the small allocator can request spans as small as 4K (but for many sizes it requests larger spans because of the _preferred size_ mechanism described above). Upping the minimum size of small page span requests would waste some memory in the small pool (due to partially filled spans), but overall this should _save_ memory since it would reduce the size of the pages array by ~75% per [Avi's comment](https://github.com/scylladb/seastar/issues/836#issuecomment-748531815): this would save perhaps 7M, much larger than the worst-case small pool waste of less than 1M.

This won't "solve" fragmentation since there are still pages as small as 16K, but it is easy and seems to have no downside.

More radical changes are possible. For example the allocator could reserve some fraction of the memory for each shard as a "huge pool". This wouldn't be used by the buddy allocator, but rather to satisfy allocations that might otherwise fail due to fragmentation. This allocator would be a more "traditional" `mmap`-based allocator which isn't restricted to a 1:1 relationship between VA and PA, and so doesn't suffer as much from physical memory fragmentation (since all allocations are multiples of 4K and 4K pages can be remapped freely by the OS). There are many variations here:

 - Is the huge allocator used unconditionally for allocations above some size, or only when the buddy allocator fails?
 - Is the huge pool per-shard or shared?
 - Do we just use the native built-in libc allocator (via the saved malloc pointer) as the huge poor, or layer another allocator on top of the reserved storage?
 - How exactly does the allocator reserve physical memory, since they mostly work in the virtual address domain? We need an allocator which carefully limits its physical memory use, but tries to use all of it e.g., by punching holes (e.g., with `MADV_DONTNEED`) in existing mappings.


### Abandon the Allocator

We know seastar works with the default system allocator: that's how we run it in debug mode (so we can use ASAN and friends). So in principle, we could use the system allocator, or some enhanced drop-in replacement instead. However, we would lose the significant benefits of the seastar allocator:

 - The performance benefits of per-shard allocation such as no locks or thread-local caches, exact NUMA affinity, etc.
 - Allocation may block the reactor threads: the seastar allocator minimizes the chance of blocking my allocating everything up front, but default allocators calling mmap may block in some cases.
 - The reclaim mechanism.
 - Heap profiling (though the replacement may offer similar functionality).
 - The exact division of memory among shards and the exact up-front nature of memory allocation. General purpose allocators tend to keep requesting more memory from the OS until something breaks, while the seastar allocator fails immediately if the assigned space is exhausted (after reclaim).

This would be a significant and potentially risky change.

### Quality of Life

Even though we might be living with fragmentation, we can make some quality of life improvements to make our life easier when root-causing failures.

 - OOM error messages should clearly log relevant details such as the "availability" metric discussed above to pinpoint if the failure was caused by fragmentation.
 - OOM error messages should include details of reclaim activity (if any) done during the failing allocation.
 - We should surface fragmentation related metrics (perhaps availability at a few key sizes) in our metrics.
 - We should surface metrics related to reclaim and batch cache activity to detect the "partial failure" case where the batch cache is thrashing described above (needs some thought about heuristics to detect this case).
 - We should track the high water mark in the seastar allocator and expose it, so we can see how it evolves over time.

## Unresolved questions

 - Is the underlying cause of fragmentation scattered across many allocations on many different paths, or are there a few "hotspots" which behave differently and tend to cause most of the fragmentation.
 - How does fragmentation evolve over time with typical loads? Do we quickly reach a plateau or does it continue to worsen for a long period of time?
 - In normal, high-load operation, are we continually "dodging a bullet", i.e., there is some small but non-zero chance of total failure due to fragmentation, or is it really only the specific workloads where we've recorded failures that can fail in practice? Said another way, should we expect a small stream of fragmentation related failures on high-load systems even if they don't do the identified problematic workloads?
