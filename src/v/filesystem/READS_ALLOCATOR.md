# Read size allocator overview

The read side for the wall uses an arena allocator.
Each arena is a bump allocator of fixed size. Since
pages on disk are either `512` or `4096` bytes on xfs for `reads` 
we allocate 2 vectors of these pools. 

Each bump allocator is of `256KB`. Internally it can either allocate
pages of 512 or 4096 but not mixed. 

The arena allocator has a `min(~30% of core mem)` and a `max(~50% of core mem)` 

* It will try to compact as soon as the number of bytes used is above min.
* It will block if mem usage is >= 50%.
* Arena's can only free a bump-allocator worth at a time 256KB

TL;DR

```

B = bump_allocator(256KB)


arena_allocator (30%-50%):
  512_pages =  [B_0, ... , B_n]
  4096_pages = [B_0, ... , B_n]

```

# Design Philosophy

The idea behind this design is that all reads for a queue-like system
exhibit perfect-prefetching-capabilities.

That is, unless something goes wrong - it does only on client error states - 
we know the current offset the client is asking for, the length of bytes
and the next set of bytes available.

The typical use for a queue is for a client to fetch some data and move on,
with monotonically increasing offsets. It is rare that a client re-reads
the same page multiple times.

## The ideal scenario:

What we need is to be able to fetch N+1 such that there is no wait
next time the user comes asking for the page.

In addition to prefetching this needs to involve a caching strategy
which is not yet implemented.

## What we do today:

No caching, only prefetching.

We pay the cost of 1 page cache miss always. Reads will simply *not* be 
faster than this in any case. 

... add content here


### The read pipeline

| Stage 1   | Stage 2 | Stage 3           |
|-----------|---------|-------------------|
| Read      | Copy    | Checksum/validate |
| Very slow | Fast    | Medium slow       |

* Stage 1 - `very slow` 
  we get the offsets and return after the first page was read. 
  Immediately, we fire the next page to be read.
    1. Open question: is reading 1 page usually `512` bytes more or less
    expensive than reading 4 pages `4096`
    2. Open question: We really should only return *after* we have enough
    bytes for one average `stride` which can be > 1 page. And we should 
    read one `stride` at a time
    3. Open question: each `stride` should have a maximum of 2 pages. How
    to reason about cpu cache misses here. Each cacheline access is `64 bytes`
    and L1 is 256KB bytes.

* Stage 2 - `fast` 
  We copy the bytes, one record at a time. That means that
  a single page on a 100byte record would be able to fit a ~4 records
  (24 bytes header + 100 bytes)
    1. We will call this 100byte example the `stride`
    2. Open question: when to use intrinsic `__prefetch()`. 
    on linux we can do `sudo dmidecode -t cache` to get the cache lines 
    for the current processor and the sizes

* Stage 3 - `medium slow`
  Currently we use `xxhash64` which can operate at `~5GB/s` on a contigous
  array of bytes. Our paylodas are often 512 bytes or less. Refer to `stride`.
  
  
Over all the pipeline is dominated by `stage 1`. Disk fetching by and large.
Which means we should optimize to reduce that one latency. This is a tradeoff
between doing useful work and fetching a bunch of memory. It is an anti pattern
to simply fetch all the pages, because you are telling the processor that you
need the page *right now* which is often wrong since you are prefetching in the 
background, but you can't really tell the processor to put it on `l2` instead of
bringing the pointer all the way to l1 and invalidating a bunch of instruction
and data caches.

This is worsen by concurrency, multiplied by parallelism. The more concurrent fetches
we perform in the background the more l1 cache pollution we experience, and on 
copying data we want the fastest possible copy.

# Design Requirements of `cache_manager.h`

1. Should serve tailing reads
2. Should attempt to predict the next pages clients are currently
   asking the service
   2.1.    Should minimize cache miss
   2.2.    Should minimize memory waste
   2.3.    Should have a net positive effect on latency
3. Should have a deterministic memory footprint.
   memory can grow and shrink but it has to be deterministic
