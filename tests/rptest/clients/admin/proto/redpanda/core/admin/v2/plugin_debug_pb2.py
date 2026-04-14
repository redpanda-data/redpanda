"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/plugin_debug.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n/proto/redpanda/core/admin/v2/plugin_debug.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"L\n\x0fCommittedOffset\x12\x16\n\x0etransform_name\x18\x01 \x01(\t\x12\x11\n\tpartition\x18\x02 \x01(\x05\x12\x0e\n\x06offset\x18\x03 \x01(\x03"3\n\x1bListCommittedOffsetsRequest\x12\x14\n\x0cshow_unknown\x18\x01 \x01(\x08"X\n\x1cListCommittedOffsetsResponse\x128\n\x07offsets\x18\x01 \x03(\x0b2\'.redpanda.core.admin.v2.CommittedOffset"\x1e\n\x1cGarbageCollectOffsetsRequest"\x1f\n\x1dGarbageCollectOffsetsResponse2\xaf\x02\n\x12PluginDebugService\x12\x89\x01\n\x14ListCommittedOffsets\x123.redpanda.core.admin.v2.ListCommittedOffsetsRequest\x1a4.redpanda.core.admin.v2.ListCommittedOffsetsResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8c\x01\n\x15GarbageCollectOffsets\x124.redpanda.core.admin.v2.GarbageCollectOffsetsRequest\x1a5.redpanda.core.admin.v2.GarbageCollectOffsetsResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.plugin_debug_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_PLUGINDEBUGSERVICE'].methods_by_name['ListCommittedOffsets']._loaded_options = None
    _globals['_PLUGINDEBUGSERVICE'].methods_by_name['ListCommittedOffsets']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_PLUGINDEBUGSERVICE'].methods_by_name['GarbageCollectOffsets']._loaded_options = None
    _globals['_PLUGINDEBUGSERVICE'].methods_by_name['GarbageCollectOffsets']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_COMMITTEDOFFSET']._serialized_start = 153
    _globals['_COMMITTEDOFFSET']._serialized_end = 229
    _globals['_LISTCOMMITTEDOFFSETSREQUEST']._serialized_start = 231
    _globals['_LISTCOMMITTEDOFFSETSREQUEST']._serialized_end = 282
    _globals['_LISTCOMMITTEDOFFSETSRESPONSE']._serialized_start = 284
    _globals['_LISTCOMMITTEDOFFSETSRESPONSE']._serialized_end = 372
    _globals['_GARBAGECOLLECTOFFSETSREQUEST']._serialized_start = 374
    _globals['_GARBAGECOLLECTOFFSETSREQUEST']._serialized_end = 404
    _globals['_GARBAGECOLLECTOFFSETSRESPONSE']._serialized_start = 406
    _globals['_GARBAGECOLLECTOFFSETSRESPONSE']._serialized_end = 437
    _globals['_PLUGINDEBUGSERVICE']._serialized_start = 440
    _globals['_PLUGINDEBUGSERVICE']._serialized_end = 743