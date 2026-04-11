"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/cloud_topics/v1/level_zero.proto')
_sym_db = _symbol_database.Default()
from ........proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ........proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from ........proto.redpanda.core.common.v1 import ntp_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_v1_dot_ntp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\nCproto/redpanda/core/admin/internal/cloud_topics/v1/level_zero.proto\x12,redpanda.core.admin.internal.cloud_topics.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\'proto/redpanda/core/common/v1/ntp.proto"4\n\x10GetStatusRequest\x12\x14\n\x07node_id\x18\x01 \x01(\x05H\x00\x88\x01\x01B\n\n\x08_node_id"\\\n\x11GetStatusResponse\x12G\n\x05nodes\x18\x01 \x03(\x0b28.redpanda.core.admin.internal.cloud_topics.v1.NodeStatus"\x86\x01\n\nNodeStatus\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12I\n\x06shards\x18\x02 \x03(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.ShardStatus\x12\x12\n\x05error\x18\x03 \x01(\tH\x00\x88\x01\x01B\x08\n\x06_error"e\n\x0bShardStatus\x12\x10\n\x08shard_id\x18\x01 \x01(\x05\x12D\n\x06status\x18\x02 \x01(\x0e24.redpanda.core.admin.internal.cloud_topics.v1.Status"2\n\x0eStartGcRequest\x12\x14\n\x07node_id\x18\x01 \x01(\x05H\x00\x88\x01\x01B\n\n\x08_node_id"\x11\n\x0fStartGcResponse"2\n\x0ePauseGcRequest\x12\x14\n\x07node_id\x18\x01 \x01(\x05H\x00\x88\x01\x01B\n\n\x08_node_id"\x11\n\x0fPauseGcResponse"2\n\x0eResetGcRequest\x12\x14\n\x07node_id\x18\x01 \x01(\x05H\x00\x88\x01\x01B\n\n\x08_node_id"\x11\n\x0fResetGcResponse"d\n\x13AdvanceEpochRequest\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition\x12\x11\n\tnew_epoch\x18\x02 \x01(\x03"^\n\x14AdvanceEpochResponse\x12F\n\x05epoch\x18\x01 \x01(\x0b27.redpanda.core.admin.internal.cloud_topics.v1.EpochInfo"Q\n\x13GetEpochInfoRequest\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition"^\n\x14GetEpochInfoResponse\x12F\n\x05epoch\x18\x01 \x01(\x0b27.redpanda.core.admin.internal.cloud_topics.v1.EpochInfo"\x91\x01\n\tEpochInfo\x12 \n\x18estimated_inactive_epoch\x18\x01 \x01(\x03\x12\x19\n\x11max_applied_epoch\x18\x02 \x01(\x03\x12"\n\x1alast_reconciled_log_offset\x18\x03 \x01(\x03\x12#\n\x1bcurrent_epoch_window_offset\x18\x04 \x01(\x03"T\n\x16GetSizeEstimateRequest\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition"/\n\x17GetSizeEstimateResponse\x12\x14\n\x0cactive_bytes\x18\x01 \x01(\x04*\xcb\x01\n\x06Status\x12\x1c\n\x18L0_GC_STATUS_UNSPECIFIED\x10\x00\x12\x17\n\x13L0_GC_STATUS_PAUSED\x10\x01\x12\x18\n\x14L0_GC_STATUS_RUNNING\x10\x02\x12\x19\n\x15L0_GC_STATUS_STOPPING\x10\x03\x12\x18\n\x14L0_GC_STATUS_STOPPED\x10\x04\x12\x1a\n\x16L0_GC_STATUS_RESETTING\x10\x05\x12\x1f\n\x1bL0_GC_STATUS_SAFETY_BLOCKED\x10\x062\xc5\x08\n\x10LevelZeroService\x12\x94\x01\n\tGetStatus\x12>.redpanda.core.admin.internal.cloud_topics.v1.GetStatusRequest\x1a?.redpanda.core.admin.internal.cloud_topics.v1.GetStatusResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8e\x01\n\x07StartGc\x12<.redpanda.core.admin.internal.cloud_topics.v1.StartGcRequest\x1a=.redpanda.core.admin.internal.cloud_topics.v1.StartGcResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8e\x01\n\x07PauseGc\x12<.redpanda.core.admin.internal.cloud_topics.v1.PauseGcRequest\x1a=.redpanda.core.admin.internal.cloud_topics.v1.PauseGcResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8e\x01\n\x07ResetGc\x12<.redpanda.core.admin.internal.cloud_topics.v1.ResetGcRequest\x1a=.redpanda.core.admin.internal.cloud_topics.v1.ResetGcResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x9d\x01\n\x0cAdvanceEpoch\x12A.redpanda.core.admin.internal.cloud_topics.v1.AdvanceEpochRequest\x1aB.redpanda.core.admin.internal.cloud_topics.v1.AdvanceEpochResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x9d\x01\n\x0cGetEpochInfo\x12A.redpanda.core.admin.internal.cloud_topics.v1.GetEpochInfoRequest\x1aB.redpanda.core.admin.internal.cloud_topics.v1.GetEpochInfoResponse"\x06\xea\x92\x19\x02\x10\x03\x12\xa6\x01\n\x0fGetSizeEstimate\x12D.redpanda.core.admin.internal.cloud_topics.v1.GetSizeEstimateRequest\x1aE.redpanda.core.admin.internal.cloud_topics.v1.GetSizeEstimateResponse"\x06\xea\x92\x19\x02\x10\x03B\x1c\xea\x92\x19\x18proto::admin::level_zerob\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x18proto::admin::level_zero'
    _globals['_LEVELZEROSERVICE'].methods_by_name['GetStatus']._loaded_options = None
    _globals['_LEVELZEROSERVICE'].methods_by_name['GetStatus']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROSERVICE'].methods_by_name['StartGc']._loaded_options = None
    _globals['_LEVELZEROSERVICE'].methods_by_name['StartGc']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROSERVICE'].methods_by_name['PauseGc']._loaded_options = None
    _globals['_LEVELZEROSERVICE'].methods_by_name['PauseGc']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROSERVICE'].methods_by_name['ResetGc']._loaded_options = None
    _globals['_LEVELZEROSERVICE'].methods_by_name['ResetGc']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROSERVICE'].methods_by_name['AdvanceEpoch']._loaded_options = None
    _globals['_LEVELZEROSERVICE'].methods_by_name['AdvanceEpoch']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROSERVICE'].methods_by_name['GetEpochInfo']._loaded_options = None
    _globals['_LEVELZEROSERVICE'].methods_by_name['GetEpochInfo']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROSERVICE'].methods_by_name['GetSizeEstimate']._loaded_options = None
    _globals['_LEVELZEROSERVICE'].methods_by_name['GetSizeEstimate']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_STATUS']._serialized_start = 1498
    _globals['_STATUS']._serialized_end = 1701
    _globals['_GETSTATUSREQUEST']._serialized_start = 236
    _globals['_GETSTATUSREQUEST']._serialized_end = 288
    _globals['_GETSTATUSRESPONSE']._serialized_start = 290
    _globals['_GETSTATUSRESPONSE']._serialized_end = 382
    _globals['_NODESTATUS']._serialized_start = 385
    _globals['_NODESTATUS']._serialized_end = 519
    _globals['_SHARDSTATUS']._serialized_start = 521
    _globals['_SHARDSTATUS']._serialized_end = 622
    _globals['_STARTGCREQUEST']._serialized_start = 624
    _globals['_STARTGCREQUEST']._serialized_end = 674
    _globals['_STARTGCRESPONSE']._serialized_start = 676
    _globals['_STARTGCRESPONSE']._serialized_end = 693
    _globals['_PAUSEGCREQUEST']._serialized_start = 695
    _globals['_PAUSEGCREQUEST']._serialized_end = 745
    _globals['_PAUSEGCRESPONSE']._serialized_start = 747
    _globals['_PAUSEGCRESPONSE']._serialized_end = 764
    _globals['_RESETGCREQUEST']._serialized_start = 766
    _globals['_RESETGCREQUEST']._serialized_end = 816
    _globals['_RESETGCRESPONSE']._serialized_start = 818
    _globals['_RESETGCRESPONSE']._serialized_end = 835
    _globals['_ADVANCEEPOCHREQUEST']._serialized_start = 837
    _globals['_ADVANCEEPOCHREQUEST']._serialized_end = 937
    _globals['_ADVANCEEPOCHRESPONSE']._serialized_start = 939
    _globals['_ADVANCEEPOCHRESPONSE']._serialized_end = 1033
    _globals['_GETEPOCHINFOREQUEST']._serialized_start = 1035
    _globals['_GETEPOCHINFOREQUEST']._serialized_end = 1116
    _globals['_GETEPOCHINFORESPONSE']._serialized_start = 1118
    _globals['_GETEPOCHINFORESPONSE']._serialized_end = 1212
    _globals['_EPOCHINFO']._serialized_start = 1215
    _globals['_EPOCHINFO']._serialized_end = 1360
    _globals['_GETSIZEESTIMATEREQUEST']._serialized_start = 1362
    _globals['_GETSIZEESTIMATEREQUEST']._serialized_end = 1446
    _globals['_GETSIZEESTIMATERESPONSE']._serialized_start = 1448
    _globals['_GETSIZEESTIMATERESPONSE']._serialized_end = 1495
    _globals['_LEVELZEROSERVICE']._serialized_start = 1704
    _globals['_LEVELZEROSERVICE']._serialized_end = 2797