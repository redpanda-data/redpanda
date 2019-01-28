# Notes on page cache

| Term | definition |
|------|------------|
| `wal_disk_pager` | interacts w/ the user request. Interface to copy pages. In the background it does eager fetching and interacts w/ the page_cache_arena & page_cache_region |
| `page_cache_region` | All files get consistently hashed via the `fileid` into one particular page_cache_region. This segment does the normal page fetching logic and it *may* (on most cases it does) do *extra* page prefetching when there is extra memory. Users **do not** interact w/ this class directly. Has no reference stability guarantees whatsoever. |
| `page_cache` | Handles the eviction on memory-events (low memory pressure) and managers all the `page_cache_region`|


# Flow

## Pre page cache

1. We get a request from the network
2. We map it to a particular core
3. The core looks up the tpidx=(namespace, topic, partition)
4. the tpidx responsible for that partition does a range-map scan on metadat we have in memory
5. we pick the physical file that contains the start of the request
6. we create a wal_disk_pager for that `wal_reader_node.h (physical file name)`
7. we hydrate the request w/ that physical file
8. if the request spans multiple files, we go to the next file and repeat from (4.)

## Page cache flow

1. A wal_disk_pager asks for a particular page
2. We do a consistent hash for the `uint64_t:file_global_id=xxhash(device & inode number)`
3. We pick a region based on this `jump_consistent_hash` of the `file_global_id`.
4. We ask the particular `page_cache_region` to give us the page, else null
5. if null, we take a semaphore from the file_locks and a memory aligned buffer to read
6. We fill up the buffer, and copy the tmp buffer into the `page_cache_region` for keeping
7. we return a copy of that page to the user
8. `wal_disk_pager` stores that copy and uses it to return data to the `wal_node_reader`
9. This page is often hit hundreds of times which is why we store this copy
10. We fill up the returned user data based on this page until we need the next page
11. On next page we repeat the same process.
12. wal_disk_pagers are not allowed to go back in page numbers. Only allowed to go forward.

# Implementation Notes

1. We copy the data 3 times in total.
    1.1.  From the filesystem into the `page_cache_region`
    1.2.  From the page cache segment into the `wal_disk_pager`: this is actually an
    optimization. We keep only the current page being accessed cached in `wal_disk_pager`.
    Which saves us a lot of work and potential re-fetch in case the page gets evicted in the
    page cache
    1.3.  To the actual returned data to the user
   Obviously This is not ideal. We will **can** remove one copy
   At the expense of making the allocator/memory-reclamation more complex.


2. The page_cache.h keeps a 16KB allocator that **is** file aligned for reads
   and a corresponding vector of semaphores so that we can fetch up to 16KB of
   concurrent pages from the file.
    2.1. This might not be ideal in a situation where we read only from *some*
    small number of files and therefore waste 16KB instead of pooling them together

3. A `page_cache_region` is effectively an index over a
   bump allocator.
   Keeps track of what pages per file are allocted.
    3.1. We randomly pick a file in case of backpressure and evict its lowest
    page to make room for a new page.


4. Memory reclamation and backpressure:
   On backpressure, we evict a random 256KB page_cache_region.h (they are non-aligned)
   only serve as a buffer. This has a few pitfals that we should fix
    4.1. Evicting a random region evicts good pages to evict as well as bad pages to evict
    For example, we might evict the page we just fetched and have to refetch in case
    there is a second reader asking for the same page - this is somewhat aliviated by
    the copy he wal_disk_pager keeps around.
    4.2. We weight all readers equally. Recnetly there is some research that shows
    that robinhood caching (giving more space to higher latency files) can have a 20%
    improvement on tail latencies for all at the expense of much higher average latency.
    We should investigate.
    We already keep the exponential moving average with a period of 10 per file.
    4.3. In the case of extreme reclamation, we reserve 30% of the core's memory for page cache

5. Growth of idle memory:
   We have a timer set at every 100ms to grow the memory of the page cache by 1 region (256KB)
   if we have not gotten reclaimed in the last 100ms. if so, wait 200ms.


0. Brainstorming / TODO:
    0.1. Work in a smarter eviction strategy only evicting the lowest pages
    compacting the segments and then freeing up one segment instead of a random one.
    if, there is still too diverse a workload fall back to random.
    0.2. Store metadata about the readers (robinhood cache is one example). so that
    you know what is a better workload to evict.
    0.3. look at the distance of the current reader and where they want to be, maybe
    evicting a higher number page is not too bad if the readers are all far behind
    0.4. grow more intelligently - goes hand in hand w/ reclaim more intelligently.


# Notes on reclamation

*  Reclamation has to happen in multiple stages
    *   We pick the lowest loaded page_cache_region.h
    *   Then we need to evit all the files.
    *   Each file that was in the allocator reclaim pages not needed.
       *   It returns a lits of pages that need relocation.
       *   We select the pages with the lowest latency for fetches (we need stats ptr)
       *   In case of no space, then each page has to deal w/ its own eviction
           and either give up the pages that were on the prevoius allocator or
           internally relocate them ?


    we can also have each file do an `evict_one()` which returns the allocator_id, bitno
