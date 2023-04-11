from dataclasses import dataclass, field
import json
import os
import re
from os.path import join
import argparse
"""
Brief description of all possible files in the Redpanda data directory.

If you're here because of the failing ci then most likely you've changed Redpanda to write new
type of file e.g. introduced new persisted_stm state machine. Please update the script to expect
the file and make the detection strict. If it's too permissive then it may match more files than
it was planned and when another file is introduced we may miss it because it may fall unther the
permissive filter. Add a brief description below if of the file below.

pid.lock                                               # created by rpk  - does not have to exist
startup_log                                            # state preventing unbounded crash loops - does not have to exist
config_cache.yaml                                      # cache of the controller stored configuration
                                                       # does not have to exist
.redpanda_data_dir                                     # empty file, needed if storage_strict_data_init is set
syschecks/                                             # empty dir, created if disk_benchmark is used
kafka/                                                 # stored user's topics and consumer groups
  __consumer_offsets/                                  # int. consumer group topic, users can't write to it directly
    12_5/                                              # each partition is represented as a folder
                                                       #   first number (e.g. 12) is the partition id
                                                       #   second (e.g 5) is a revision
      1337-2-v1.log                                    # segment of a log
                                                       #   first number (e.g. 1337) is the start offset
                                                       #   second is the segment's term
      1337-2-v1.base_index                             # index of a corresponding segment
                                                       #   logical offset -> byte offset
                                                       #   timestamp -> byte offset
      1337-2-v1.log.staging                            # a segment during self compaction, see
                                                       # do_self_compact_segment of segment_utils
      1337-2-v1.log.compaction.staging                 # adjusted segments being merged
      1337-2-v1.log.compaction.base_index              # index corresponding to .log.compaction.staging
      1337-2-v1.log.compaction.staging.staging         # merged adjusted segments being compacted
      1337-2-v1.compaction_index                       # key -> offset (offsets?) index
      1337-2-v1.compaction_index.staging               # compaction_index in the process of being written
                                                       # see do_write_clean_compacted_index of segment_utils
      1337-2-v1log.compaction.compaction_index         # compaction index for a segment merged by adjacent segment compaction
      1337-2-v1log.compaction.compaction_index.staging # 1337-2-v1log.compaction.compaction_index in the process of being written
      archival_metadata.snapshot                       # snapshot of archival_metadata_stm
      archival_metadata.snapshot.partial.1621.a8D9     # snapshot in the process of being written, suffix is random
                                                       # see snapshot_manager::start_snapshot
      snapshot                                         # raft snapshot created during prefix truncation
                                                       # see consensus::_snapshot_mgr

  topic1/                                              # user's topic
    12_5/                                              # user's topic partition
      ...                                              # has the same structure as __consumer_offsets's partition, except
      tx.snapshot                                      # it has additional rm_stm's snapshot used for idempotency and transactions
      tx.snapshot.partial.1621.a8D9
    12_5_part/                                         # partition in the process of recovery, see partition_downloader::download_log
      1337-2-v1.log                                    # unlike "regular" partition it contains only segments
  
  ...                                                  # other user's topics

kafka_internal/                                        # stored internal redpanda topics: tx, id_allocator & group
  tx/                                                  # tx coordinator's topic
    0_1/                                               # tx coordinator partition is similar to "regular" partitions but
                                                       # lacks compaction related files
      1337-2-v1.log
      1337-2-v1.base_index
      snapshot
      tx.coordinator.snapshot                          # tm_stm's snapshot
      tx.coordinator.snapshot.1621.a8D9

  id_allocator/                                        # id allocator's topic used for idempotency and transacitons
    0_1/                                               # it's similar to tx coordinator's topic but has only one partition
      1337-2-v1.log
      1337-2-v1.base_index
      snapshot
      id.snapshot                                      # id_allocator_stm's snapshot
      id.snapshot.1621.a8D9
    
  group/                                               # consumer offsets used to be stored in this topic
    0_1/
      ...                                              # has similar structure as __consumer_offsets's partition
                                                       # but lacks archival_metadata.snapshot
      tx.snapshot                                      # has rm_stm's snapshot which is a bug
      tx.snapshot.partial.1621.a8D9

redpanda/                                              # another place to store inernal topics: controller & kvstore
  controller/
    0_1/                                               # has only one partition, no persisted stm's
      1337-2-v1.log
      1337-2-v1.base_index
      snapshot
  kvstore/                                             # kvstore is local non-replicated topic, different nodes have
                                                       # different kvstore logs; it's used to keep aux info such as
                                                       # raft's votes of the current node
    0_1/
      1337-2-v1.log
      1337-2-v1.base_index
      snapshot                                         # name collision, this isn't the raft snapshot

cloud_storage_cache/                                   # SI cache
  accesstime
  f6ek42xx/                                            # 8 symbol hash
    kafka/
      topic1/                                          # name of the cached topic
        12_5/                                          # partitions {id}_{revision}
          1337-1452-666-4-v1.log.6                     # cached segment, legend:
                                                       #    base_offset (e.g. 1337)
                                                       #    committed_offset (e.g. 1452)
                                                       #    size_bytes (e.g. 666)
                                                       #    segment_term (e.g. 4)
                                                       #    uploader_term (e.g. 6)
          1337-1452-666-4-v1.log.6.tx                  # aborted transactions
          1337-1452-666-4-v1.log.6.index               # index
"""


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        return getattr(obj.__class__, "jsonable", lambda x: x)(obj)


@dataclass
class File:
    path: str

    def jsonable(self):
        return {"type": "file", "path": self.path}


@dataclass
class Dir:
    path: str

    def jsonable(self):
        return {"type": "dir", "path": self.path}


@dataclass
class DataDirMarker:
    """ see storage_strict_data_init """
    path: str

    def jsonable(self):
        return self.__dict__


@dataclass
class ConfigCache:
    path: str

    def jsonable(self):
        return self.__dict__


@dataclass
class Accesstime:
    path: str

    def jsonable(self):
        return self.__dict__


@dataclass
class CrashLoopFile:
    path: str
    size: int = -1

    def jsonable(self):
        return self.__dict__


@dataclass
class CloudSegmentName:
    base_offset: int
    committed_offset: int
    size_bytes: int
    segment_term: int
    version: str
    uploader_term: int

    def jsonable(self):
        return self.__dict__


@dataclass
class CloudSegment(CloudSegmentName):
    pass


@dataclass
class CloudTx(CloudSegmentName):
    pass


@dataclass
class CloudIndex(CloudSegmentName):
    pass


@dataclass
class CloudPartition:
    id: int
    revision: str
    segments: list[CloudSegment] = field(default_factory=list)
    txs: list[CloudTx] = field(default_factory=list)
    indices: list[CloudIndex] = field(default_factory=list)

    def jsonable(self):
        return self.__dict__


@dataclass
class CloudTopic:
    namespace: str
    name: str
    partitions: list[CloudPartition] = field(default_factory=list)

    def jsonable(self):
        return self.__dict__


@dataclass
class CloudStorageCacheEntry:
    hash: str
    topics: dict[str, CloudTopic] = field(default_factory=dict)

    def jsonable(self):
        return self.__dict__


@dataclass
class CloudStorageCache:
    path: str
    accesstime: Accesstime = None
    entries: list[CloudStorageCacheEntry] = field(default_factory=list)

    def jsonable(self):
        return self.__dict__


@dataclass
class SegmentName:
    path: str
    start_offset: int = -1
    term: int = -1
    version: str = ""
    size: int = -1

    def jsonable(self):
        return self.__dict__


@dataclass
class Segment(SegmentName):
    pass


@dataclass
class Index(SegmentName):
    pass


@dataclass
class CompactionIndex(SegmentName):
    pass


@dataclass
class Snapshot:
    type: str
    path: str
    size: int = -1

    def jsonable(self):
        return self.__dict__


@dataclass
class BarePartition:
    id: int
    revision: str
    segments: list[Segment] = field(default_factory=list)

    def jsonable(self):
        return self.__dict__


@dataclass
class KVPartition(BarePartition):
    indices: list[Index] = field(default_factory=list)
    kv_snapshot: Snapshot = None


@dataclass
class Controller(BarePartition):
    indices: list[Index] = field(default_factory=list)
    raft_snapshot: Snapshot = None


@dataclass
class Partition(BarePartition):
    indices: list[Index] = field(default_factory=list)
    snapshots: list[Snapshot] = field(default_factory=list)
    partial_snapshots: list[Snapshot] = field(default_factory=list)
    raft_snapshot: Snapshot = None


@dataclass
class CompactedPartition(Partition):
    staged_segments: list[Segment] = field(default_factory=list)
    staged_compaction_segments: list[Segment] = field(default_factory=list)
    compaction_base_index: list[Index] = field(default_factory=list)
    staged_staged_compaction_segments: list[Segment] = field(
        default_factory=list)
    compaction_indices: list[CompactionIndex] = field(default_factory=list)
    staged_compaction_indices: list[CompactionIndex] = field(
        default_factory=list)
    compaction_compaction_indices: list[CompactionIndex] = field(
        default_factory=list)
    staged_compaction_compaction_indices: list[CompactionIndex] = field(
        default_factory=list)


Log = KVPartition | BarePartition | Partition | CompactedPartition | Controller


@dataclass
class Topic:
    namespace: str
    name: str
    partitions: list[Log] = field(default_factory=list)
    # see partition_recovery_manager::download_log_with_capped_size
    downloads: list[BarePartition] = field(default_factory=list)

    def jsonable(self):
        return self.__dict__


UnknownFSItem = Dir | File


@dataclass
class Description:
    unknown: list[UnknownFSItem] = field(default_factory=list)
    config_cache: ConfigCache = None
    data_dir_marker: DataDirMarker = None
    cloud_storage_cache: CloudStorageCache = None
    controller: Topic = None
    kvstore: Topic = None
    tx: Topic = None
    id_allocator: Topic = None
    consumer_offsets: Topic = None
    consumer_groups: Topic = None
    topics: dict[str, Topic] = field(default_factory=dict)
    crash_loop_tracker_file: CrashLoopFile = None

    def jsonable(self):
        return self.__dict__


def listdir(root):
    try:
        return os.listdir(root)
    except FileNotFoundError:
        return []


def isfile(root):
    try:
        return os.path.isfile(root)
    except FileNotFoundError:
        return False


def isdir(root):
    try:
        return os.path.isdir(root)
    except FileNotFoundError:
        return False


def expect_dir(path, description):
    if isfile(path):
        description.unknown.append(File(path))
        return False
    if not isdir(path):
        # path was removed while inspecting
        return False
    return True


def expect_file(path, description):
    if isdir(path):
        unknown_dir(path, description)
        return False
    if not isfile(path):
        # path was removed while inspecting
        return False
    return True


def unknown_dir(root: str, description: Description) -> None:
    description.unknown.append(Dir(root))
    for item in listdir(root):
        path = join(root, item)
        if isfile(path):
            description.unknown.append(File(path))
        elif isdir(path):
            unknown_dir(path, description)
        else:
            # path was removed while inspecting
            pass


def inspect_kafka_internal(root: str, description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if item == "tx":
            if not expect_dir(path, description):
                continue
            description.tx = get_topic(path, "kafka_internal", item,
                                       description)
        elif item == "id_allocator":
            if not expect_dir(path, description):
                continue
            description.id_allocator = get_topic(path, "kafka_internal", item,
                                                 description)
        elif item == "group":
            if not expect_dir(path, description):
                continue
            description.consumer_groups = get_topic(path, "kafka_internal",
                                                    item, description)
        elif isfile(path):
            description.unknown.append(File(path))
        elif isdir(path):
            unknown_dir(path, description)
        else:
            # path was removed while inspecting
            pass


def inspect_syschecks(root: str, description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if isfile(path):
            description.unknown.append(File(path))
        elif isdir(path):
            unknown_dir(path, description)
        else:
            # path was removed while inspecting
            pass


def get_topic(root: str, namespace: str, name: str,
              description: Description) -> Topic:
    topic = Topic(namespace, name)
    for item in listdir(root):
        path = join(root, item)
        if not expect_dir(path, description):
            continue

        m = re.match("^(\d+)_(\d+)$", item)
        if m:
            id = int(m.group(1))
            revision = int(m.group(2))
            if namespace == "redpanda" and name == "controller":
                if id > 0:
                    unknown_dir(path, description)
                    continue
            if namespace == "kafka_internal" and name == "id_allocator":
                if id > 0:
                    unknown_dir(path, description)
                    continue
            topic.partitions.append(
                get_partition(path, topic, id, revision, description))
            continue

        if namespace == "kafka" and name != "__consumer_offsets":
            m = re.match("^(\d+)_(\d+)_part$", item)
            if m:
                id = int(m.group(1))
                revision = int(m.group(2))
                topic.downloads.append(
                    get_partition(path,
                                  topic,
                                  id,
                                  revision,
                                  description,
                                  is_recovering=True))
                continue

        unknown_dir(path, description)
    return topic


def inspect_kafka(root: str, description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if not expect_dir(path, description):
            continue
        if item == "__consumer_offsets":
            description.consumer_offsets = get_topic(path, "kafka", item,
                                                     description)
        else:
            description.topics[item] = get_topic(path, "kafka", item,
                                                 description)


def get_partition(root: str,
                  topic: Topic,
                  id: int,
                  revision: int,
                  description: Description,
                  is_recovering: bool = False) -> Log:
    snapshots = []
    compacted = False
    is_kv = False
    is_controller = False

    if topic.namespace == "kafka_internal" and topic.name == "tx":
        snapshots = ["tx.coordinator.snapshot"]
    if topic.namespace == "kafka_internal" and topic.name == "id_allocator":
        snapshots = ["id.snapshot"]
    if topic.namespace == "kafka_internal" and topic.name == "group":
        snapshots = ["tx.snapshot"]
        compacted = True
    if topic.namespace == "kafka":
        compacted = True
        if topic.name == "__consumer_offsets":
            snapshots = ["archival_metadata.snapshot"]
        else:
            snapshots = ["tx.snapshot", "archival_metadata.snapshot"]
    if topic.namespace == "redpanda" and topic.name == "controller":
        is_controller = True
    if topic.namespace == "redpanda" and topic.name == "kvstore":
        is_kv = True
        snapshots = []

    if is_recovering:
        snapshots = []
        compacted = False

    partition = None

    if is_controller:
        partition = Controller(id, revision)
    elif is_kv:
        partition = KVPartition(id, revision)
    elif is_recovering:
        partition = BarePartition(id, revision)
    elif compacted:
        partition = CompactedPartition(id, revision)
    else:
        partition = Partition(id, revision)

    for item in listdir(root):
        path = join(root, item)

        m = re.match("^(\d+)-(\d+)-(v1)(.+)$", item)
        if m:
            if not expect_file(path, description):
                continue
            suffix = m.group(4)
            entity = None
            collection = None
            if suffix == ".log":
                entity = Segment(path)
                collection = partition.segments
            elif not is_recovering and suffix == ".base_index":
                entity = Index(path)
                collection = partition.indices
            elif compacted and suffix == ".log.staging":
                entity = Segment(path)
                collection = partition.staged_segments
            elif compacted and suffix == ".log.compaction.base_index":
                entity = Index(path)
                collection = partition.compaction_base_index
            elif compacted and suffix == ".log.compaction.staging":
                entity = Segment(path)
                collection = partition.staged_compaction_segments
            elif compacted and suffix == ".log.compaction.staging.staging":
                entity = Segment(path)
                collection = partition.staged_staged_compaction_segments
            elif compacted and suffix == ".compaction_index":
                entity = CompactionIndex(path)
                collection = partition.compaction_indices
            elif compacted and suffix == ".compaction_index.staging":
                entity = CompactionIndex(path)
                collection = partition.staged_compaction_indices
            elif compacted and suffix == "log.compaction.compaction_index.staging":
                entity = CompactionIndex(path)
                collection = partition.staged_compaction_compaction_indices
            elif compacted and suffix == "log.compaction.compaction_index":
                entity = CompactionIndex(path)
                collection = partition.compaction_compaction_indices
            else:
                description.unknown.append(File(path))
                continue
            try:
                entity.start_offset = int(m.group(1))
                entity.term = int(m.group(2))
                entity.version = m.group(3)
                entity.size = os.path.getsize(path)
                collection.append(entity)
            except FileNotFoundError:
                pass
            continue

        if item == "snapshot":
            if is_kv:
                if not expect_file(path, description):
                    continue
                try:
                    snapshot = Snapshot(item, path)
                    snapshot.size = os.path.getsize(path)
                    partition.kv_snapshot = snapshot
                except FileNotFoundError:
                    pass
                continue
            elif not is_recovering:
                if not expect_file(path, description):
                    continue
                try:
                    snapshot = Snapshot(item, path)
                    snapshot.size = os.path.getsize(path)
                    partition.raft_snapshot = snapshot
                except FileNotFoundError:
                    pass
                continue

        if item in snapshots:
            if not expect_file(path, description):
                continue
            try:
                snapshot = Snapshot(item, path)
                snapshot.size = os.path.getsize(path)
                partition.snapshots.append(snapshot)
            except FileNotFoundError:
                pass
            continue

        m = re.match("^(.+)\.partial\.\d+\.[a-zA-Z0-9]{4}$", item)
        if m:
            if not expect_file(path, description):
                continue
            if m.group(1) not in snapshots:
                description.unknown.append(File(path))
                continue
            try:
                snapshot = Snapshot(item, path)
                snapshot.size = os.path.getsize(path)
                partition.partial_snapshots.append(snapshot)
                continue
            except FileNotFoundError:
                pass

        if isfile(path):
            description.unknown.append(File(path))
        elif isdir(path):
            unknown_dir(path, description)
        else:
            # path was removed while inspecting
            pass
    return partition


def inspect_redpanda(root: str, description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if item == "controller":
            if not expect_dir(path, description):
                continue
            description.controller = get_topic(path, "redpanda", item,
                                               description)
        elif item == "kvstore":
            if not expect_dir(path, description):
                continue
            description.kvstore = get_topic(path, "redpanda", item,
                                            description)
        else:
            if isfile(path):
                description.unknown.append(File(path))
            elif isdir(path):
                unknown_dir(path, description)
            else:
                # path was removed while inspecting
                pass


def inspect_cloud_partition(root: str, partition: CloudPartition,
                            description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if not expect_file(path, description):
            continue

        m = re.match("^(\d+)-(\d+)-(\d+)-(\d+)-v1.log.(\d+)$", item)
        if m:
            segment = CloudSegment(base_offset=int(m.group(1)),
                                   committed_offset=int(m.group(2)),
                                   size_bytes=int(m.group(3)),
                                   segment_term=int(m.group(4)),
                                   uploader_term=int(m.group(5)),
                                   version="v1")
            partition.segments.append(segment)
            continue

        m = re.match("^(\d+)-(\d+)-(\d+)-(\d+)-v1.log.(\d+).tx$", item)
        if m:
            try:
                tx = CloudTx(base_offset=int(m.group(1)),
                             committed_offset=int(m.group(2)),
                             size_bytes=int(m.group(3)),
                             segment_term=int(m.group(4)),
                             uploader_term=int(m.group(5)),
                             version="v1")
                partition.txs.append(tx)
            except FileNotFoundError:
                pass
            continue

        m = re.match("^(\d+)-(\d+)-(\d+)-(\d+)-v1.log.(\d+).index$", item)
        if m:
            index = CloudIndex(base_offset=int(m.group(1)),
                               committed_offset=int(m.group(2)),
                               size_bytes=int(m.group(3)),
                               segment_term=int(m.group(4)),
                               uploader_term=int(m.group(5)),
                               version="v1")
            partition.indices.append(index)
            continue

        description.unknown.append(File(path))


def get_cloud_topic(root: str, namespace: str, name: str,
                    description: Description) -> CloudTopic:
    topic = CloudTopic(namespace=namespace, name=name)
    for item in listdir(root):
        path = join(root, item)
        if not expect_dir(path, description):
            continue

        m = re.match("^(\d+)_(\d+)$", item)
        if m:
            partition = CloudPartition(id=int(m.group(1)),
                                       revision=int(m.group(2)))
            inspect_cloud_partition(path, partition, description)
            topic.partitions.append(partition)
            continue

        unknown_dir(path, description)
    return topic


def inspect_cloud_kafka(root: str, entry: CloudStorageCacheEntry,
                        description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if not expect_dir(path, description):
            continue
        entry.topics[item] = get_cloud_topic(path, "kafka", item, description)


def inspect_cloud_cache_entry(root: str, entry: CloudStorageCacheEntry,
                              description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if item == "kafka":
            if not expect_dir(path, description):
                continue
            inspect_cloud_kafka(path, entry, description)
        elif isfile(path):
            description.unknown.append(File(path))
        elif isdir(path):
            unknown_dir(path, description)
        else:
            # path was removed while inspecting
            pass


def inspect_cloud_storage_cache(root: str,
                                cloud_storage_cache: CloudStorageCache,
                                description: Description) -> None:
    for item in listdir(root):
        path = join(root, item)
        if "accesstime" == item:
            if not expect_file(path, description):
                continue
            cloud_storage_cache.accesstime = Accesstime(path)
            continue

        m = re.match("^[a-z0-9]{8}$", item)
        if m:
            if not expect_dir(path, description):
                continue
            entry = CloudStorageCacheEntry(hash=item)
            cloud_storage_cache.entries.append(entry)
            inspect_cloud_cache_entry(path, entry, description)
            continue

        if isfile(path):
            description.unknown.append(File(path))
        elif isdir(path):
            unknown_dir(path, description)
        else:
            # path was removed while inspecting
            pass


def get_description(root: str) -> Description:
    description = Description()

    if isfile(root):
        description.unknown.append(File(root))
        return description

    if not isdir(root):
        # path was removed while inspecting
        return description

    for item in listdir(root):
        path = join(root, item)
        if item == "pid.lock":
            if not expect_file(path, description):
                continue
        elif item == "cloud_storage_cache":
            if not expect_dir(path, description):
                continue
            description.cloud_storage_cache = CloudStorageCache(path=path)
            inspect_cloud_storage_cache(path, description.cloud_storage_cache,
                                        description)
        elif item == "startup_log":
            if not expect_file(path, description):
                continue
            try:
                description.crash_loop_tracker_file = CrashLoopFile(path)
                description.crash_loop_tracker_file.size = os.path.getsize(
                    path)
            except FileNotFoundError:
                pass
        elif item == "config_cache.yaml":
            if not expect_file(path, description):
                continue
            description.config_cache = ConfigCache(path)
        elif item == ".redpanda_data_dir":
            if not expect_file(path, description):
                continue
            description.data_dir_marker = DataDirMarker(path)
        elif item == "redpanda":
            if not expect_dir(path, description):
                continue
            inspect_redpanda(path, description)
        elif item == "kafka":
            if not expect_dir(path, description):
                continue
            inspect_kafka(path, description)
        elif item == "kafka_internal":
            if not expect_dir(path, description):
                continue
            inspect_kafka_internal(path, description)
        elif item == "syschecks":
            if not expect_dir(path, description):
                continue
            inspect_syschecks(path, description)
        else:
            if isfile(path):
                description.unknown.append(File(path))
            elif isdir(path):
                unknown_dir(path, description)
            else:
                # path was removed while inspecting
                pass

    return description


parser = argparse.ArgumentParser(
    description='Redpanda data_directory structure analyzer')
parser.add_argument('--path',
                    type=str,
                    required=True,
                    help='Path to data_directory to inspect')

options, _ = parser.parse_known_args()

print(json.dumps(get_description(options.path), cls=JSONEncoder))
