"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/datalake/v1/datalake.proto')
_sym_db = _symbol_database.Default()
from ........proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ........proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n=proto/redpanda/core/admin/internal/datalake/v1/datalake.proto\x12(redpanda.core.admin.internal.datalake.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"3\n\x1aGetCoordinatorStateRequest\x12\x15\n\rtopics_filter\x18\x01 \x03(\t"h\n\x1bGetCoordinatorStateResponse\x12I\n\x05state\x18\x01 \x01(\x0b2:.redpanda.core.admin.internal.datalake.v1.CoordinatorState"\xdf\x01\n\x10CoordinatorState\x12a\n\x0ctopic_states\x18\x01 \x03(\x0b2K.redpanda.core.admin.internal.datalake.v1.CoordinatorState.TopicStatesEntry\x1ah\n\x10TopicStatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12C\n\x05value\x18\x02 \x01(\x0b24.redpanda.core.admin.internal.datalake.v1.TopicState:\x028\x01"\x96\x01\n\x08DataFile\x12\x13\n\x0bremote_path\x18\x01 \x01(\t\x12\x11\n\trow_count\x18\x02 \x01(\x04\x12\x17\n\x0ffile_size_bytes\x18\x03 \x01(\x04\x12\x17\n\x0ftable_schema_id\x18\x04 \x01(\x05\x12\x19\n\x11partition_spec_id\x18\x05 \x01(\x05\x12\x15\n\rpartition_key\x18\x06 \x03(\x0c"\xf0\x01\n\x15TranslatedOffsetRange\x12\x14\n\x0cstart_offset\x18\x01 \x01(\x03\x12\x13\n\x0blast_offset\x18\x02 \x01(\x03\x12F\n\ndata_files\x18\x03 \x03(\x0b22.redpanda.core.admin.internal.datalake.v1.DataFile\x12E\n\tdlq_files\x18\x04 \x03(\x0b22.redpanda.core.admin.internal.datalake.v1.DataFile\x12\x1d\n\x15kafka_processed_bytes\x18\x05 \x01(\x04"w\n\x0cPendingEntry\x12M\n\x04data\x18\x01 \x01(\x0b2?.redpanda.core.admin.internal.datalake.v1.TranslatedOffsetRange\x12\x18\n\x10added_pending_at\x18\x02 \x01(\x03"\x91\x01\n\x0ePartitionState\x12O\n\x0fpending_entries\x18\x01 \x03(\x0b26.redpanda.core.admin.internal.datalake.v1.PendingEntry\x12\x1b\n\x0elast_committed\x18\x02 \x01(\x03H\x00\x88\x01\x01B\x11\n\x0f_last_committed"\x91\x03\n\nTopicState\x12\x10\n\x08revision\x18\x01 \x01(\x03\x12c\n\x10partition_states\x18\x02 \x03(\x0b2I.redpanda.core.admin.internal.datalake.v1.TopicState.PartitionStatesEntry\x12Q\n\x0flifecycle_state\x18\x03 \x01(\x0e28.redpanda.core.admin.internal.datalake.v1.LifecycleState\x12#\n\x1btotal_kafka_processed_bytes\x18\x04 \x01(\x04\x12"\n\x1alast_committed_snapshot_id\x18\x05 \x01(\x04\x1ap\n\x14PartitionStatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12G\n\x05value\x18\x02 \x01(\x0b28.redpanda.core.admin.internal.datalake.v1.PartitionState:\x028\x01*\x83\x01\n\x0eLifecycleState\x12\x1f\n\x1bLIFECYCLE_STATE_UNSPECIFIED\x10\x00\x12\x18\n\x14LIFECYCLE_STATE_LIVE\x10\x01\x12\x1a\n\x16LIFECYCLE_STATE_CLOSED\x10\x02\x12\x1a\n\x16LIFECYCLE_STATE_PURGED\x10\x032\xbe\x01\n\x0fDatalakeService\x12\xaa\x01\n\x13GetCoordinatorState\x12D.redpanda.core.admin.internal.datalake.v1.GetCoordinatorStateRequest\x1aE.redpanda.core.admin.internal.datalake.v1.GetCoordinatorStateResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._loaded_options = None
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._serialized_options = b'8\x01'
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._loaded_options = None
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_options = b'8\x01'
    _globals['_DATALAKESERVICE'].methods_by_name['GetCoordinatorState']._loaded_options = None
    _globals['_DATALAKESERVICE'].methods_by_name['GetCoordinatorState']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LIFECYCLESTATE']._serialized_start = 1640
    _globals['_LIFECYCLESTATE']._serialized_end = 1771
    _globals['_GETCOORDINATORSTATEREQUEST']._serialized_start = 185
    _globals['_GETCOORDINATORSTATEREQUEST']._serialized_end = 236
    _globals['_GETCOORDINATORSTATERESPONSE']._serialized_start = 238
    _globals['_GETCOORDINATORSTATERESPONSE']._serialized_end = 342
    _globals['_COORDINATORSTATE']._serialized_start = 345
    _globals['_COORDINATORSTATE']._serialized_end = 568
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._serialized_start = 464
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._serialized_end = 568
    _globals['_DATAFILE']._serialized_start = 571
    _globals['_DATAFILE']._serialized_end = 721
    _globals['_TRANSLATEDOFFSETRANGE']._serialized_start = 724
    _globals['_TRANSLATEDOFFSETRANGE']._serialized_end = 964
    _globals['_PENDINGENTRY']._serialized_start = 966
    _globals['_PENDINGENTRY']._serialized_end = 1085
    _globals['_PARTITIONSTATE']._serialized_start = 1088
    _globals['_PARTITIONSTATE']._serialized_end = 1233
    _globals['_TOPICSTATE']._serialized_start = 1236
    _globals['_TOPICSTATE']._serialized_end = 1637
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_start = 1525
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_end = 1637
    _globals['_DATALAKESERVICE']._serialized_start = 1774
    _globals['_DATALAKESERVICE']._serialized_end = 1964