- Feature Name: add `v_type` to `model::record_batch_header`
- Status: in-progress
- Start Date: 2019-09-28
- Authors: alex
- RFC PR: (PR # after acceptance of initial draft)
- Issue: (one or more # from the issue tracker)

# Summary

Add an internal type to differentiate between different subsystem batches.

```patch

@@ -141,10 +141,24 @@ private:
     std::bitset<16> _attributes;
 };
 
+using record_batch_type = named_type<uint8_t, struct model_record_batch_type>;
+

struct record_batch_header {
     // Size of the batch minus this field.
     uint32_t size_bytes;
     offset base_offset;
+    record_batch_type type;
     int32_t crc;
     record_batch_attributes attrs;
     int32_t last_offset_delta;
modified   src/v/raft/types.h
@@ -37,40 +37,32 @@ struct group_configuration {
     std::vector<model::broker> learners;
 };
 
-class entry {
+/// \brief a *collection* of record_batch. In other words
+/// and array of array. This is done because the majority of
+/// batches will come from the Kafka API which is already batched
+/// Main constraint is that _all_ records and batches must be of the same type
+class entry final {
 public:
-    using type = int32_t;
-
-    // well known types
-    static constexpr type type_unknown = 0;
-    static constexpr type type_data = 1;
-    static constexpr type type_configuration = 2;
-
-    explicit entry(type t, model::record_batch_reader r)
+    explicit entry(model::record_batch_type t, model::record_batch_reader r)
       : _t(t)
       , _rdr(std::move(r)) {
     }
-    virtual ~entry() = default;
-
-    virtual type entry_type() const {
+    model::record_batch_type entry_type() const {
         return _t;
     }
-    virtual model::record_batch_reader& reader() {
+    model::record_batch_reader& reader() {
         return _rdr;
     }
-    virtual future<> on_replicated() {
-        return make_ready_future<>();
-    }
 
 private:
-    type _t;
+    model::record_batch_type _t;
     model::record_batch_reader _rdr;
 };
 
 struct append_entries_request {
     model::node_id node_id;
     protocol_metadata meta;
-    std::vector<std::unique_ptr<entry>> entries;
+    std::vector<entry> entries;
 };
 

```

## What is being proposed

First observation

The detection whether or not the record is internal should actually
happen at the batch level. The reason is `record_batch` is our atomic unit for
reads and writes. 

Thus, the next modifications follow:

1. add a `model::record_batch_type` _above_ the CRC line
2. de-virtualize `raft::entry` type

## Why (short reason)

We want other subsystems `raft` and `cluster` in particular to take advantange
of our `storage::log` and related infrastructure to persist changes to disk.
It allows us to have one code base for persisting records and reading them from
disk.

## How (short plan)

We will be extending the `model::record_batch_header` with a
`model::record_batch_type` which is just a wrapper around a `uint8_t`

By default `0` will be used to detect errors while either parsing data from the
Kafka layer or from the RPC layer. 

## Impact

* The wire format will increase by 1 byte on disk
* The Kafka produce API will have to set this byte explicitly to `1`
* Raft configurations will have to set this byte to `2`
* Kafka fetch API will have to skip all records that are not equal to `1`

# Motivation

We want to leverage the existing infrastructure for persisting and reading data
from disk.

# Guide-level explanation

Largely, each new component that wishes to extend the type, should reserve their
integer in the `model` submodule to ensure no collisions at runtime. 

Note that we `explicitly` chose an integer to have no inter-module coupling.

Reading and writing will have no modification other than reading+writing 
the extra byte which should be a very small change. 

The main dependency is that the `kafka::produce` API will have to set the 
byte. 

# Reference-level explanation

Changing the `model::record_batch_header` is a very low level feature. It is 
modifying the disk layout and therefore the readers. We expect that most 
features will never actually extend this.

1. Storage
2. Raft
3. Cluster
4. Kafka

We expect most features will actually interact with modules at the interface 
level and not at the disk format level. That is, you will wrap your _puts_ 
as a Kafka produce API format, or you will interact w/ the global cluster
via the `cluster::controller` module and so on.

The main note worth highlighting is that by setting the default to 0, 
it allows us to detect if the proper subsystems actually set the field 
or simply left it alone. Note that leaving it alone is a bug. It _must_ be
set by each subsystem. 

We expect to have tooling integrated with this feature so system administrators
can actually inspect and see if we have a bug by not having set the 
`model::record_batch_type` in the actual on-disk format.

## Drawbacks

Why should we *not* do this?

* Adds complexity to the parsing protocol
* Adds complexity to every module by allowing every module to set the 
  `model::record_batch_type`
  
## Rationale and Alternatives

- Why is this design the best in the space of possible designs?

  This proposal allows us to leverage the same on-disk format as the 
  `storage::log` _without_ the need for keeping secondary metadata.
  In other words, if we were to _not_ use this design, we would have to 
  have _some_ way of knowing whether the batch on disk is publicly visible
  to the Kafka layer or not. 
  
  The complexities of maintaining alternative metadata is a possible source
  of inconsistencies and a _major_ source of complexity. 

- What other designs have been considered and what is the rationale 
  for not choosing them?

  Setting  a single bit in the `model::record_batch_attributes` was 
  considered and discarded:
  
  -  It is actually taken into account with the Kafka CRC
     Which meant that we would have to re-checksum all payloads
  -  We would have no way of detecting `unset` bits. This is a powerful 
     property of this design. By requiring _each_ module to set this byte
     we can detect if a new extension to the code base simply forgot to 
     set the `model::record_batch_type` and inform early during testing.
     This property could not have been achieved with a single bit.

## Unresolved questions

-  Missing integration with tooling. What and which tool should
   work w/ the log replay functions to help us debug bad log segments

