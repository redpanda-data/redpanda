"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/v1/debug.proto')
_sym_db = _symbol_database.Default()
from .......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from .......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n1proto/redpanda/core/admin/internal/v1/debug.proto\x12\x1fredpanda.core.admin.v2.internal\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"\xdd\x01\n\x17StartStressFiberRequest\x12&\n\x1emin_spins_per_scheduling_point\x18\x01 \x01(\x05\x12&\n\x1emax_spins_per_scheduling_point\x18\x02 \x01(\x05\x12\x13\n\x0bstack_depth\x18\x03 \x01(\x05\x12#\n\x1bmin_ms_per_scheduling_point\x18\x04 \x01(\x05\x12#\n\x1bmax_ms_per_scheduling_point\x18\x05 \x01(\x05\x12\x13\n\x0bfiber_count\x18\x06 \x01(\x05"\x1a\n\x18StartStressFiberResponse"\x18\n\x16StopStressFiberRequest"\x19\n\x17StopStressFiberResponse"\xd5\x01\n\x1fThrowStructuredExceptionRequest\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x0e\n\x06reason\x18\x02 \x01(\t\x12`\n\x08metadata\x18\x03 \x03(\x0b2N.redpanda.core.admin.v2.internal.ThrowStructuredExceptionRequest.MetadataEntry\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x028\x01""\n ThrowStructuredExceptionResponse"^\n\x11LogMessageRequest\x12\x0f\n\x07message\x18\x01 \x01(\t\x128\n\x05level\x18\x02 \x01(\x0e2).redpanda.core.admin.v2.internal.LogLevel"\x14\n\x12LogMessageResponse*\x8c\x01\n\x08LogLevel\x12\x19\n\x15LOG_LEVEL_UNSPECIFIED\x10\x00\x12\x13\n\x0fLOG_LEVEL_TRACE\x10\x01\x12\x13\n\x0fLOG_LEVEL_DEBUG\x10\x02\x12\x12\n\x0eLOG_LEVEL_INFO\x10\x03\x12\x12\n\x0eLOG_LEVEL_WARN\x10\x04\x12\x13\n\x0fLOG_LEVEL_ERROR\x10\x052\xd8\x04\n\x0cDebugService\x12\xa7\x01\n\x18ThrowStructuredException\x12@.redpanda.core.admin.v2.internal.ThrowStructuredExceptionRequest\x1aA.redpanda.core.admin.v2.internal.ThrowStructuredExceptionResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8f\x01\n\x10StartStressFiber\x128.redpanda.core.admin.v2.internal.StartStressFiberRequest\x1a9.redpanda.core.admin.v2.internal.StartStressFiberResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8c\x01\n\x0fStopStressFiber\x127.redpanda.core.admin.v2.internal.StopStressFiberRequest\x1a8.redpanda.core.admin.v2.internal.StopStressFiberResponse"\x06\xea\x92\x19\x02\x10\x03\x12}\n\nLogMessage\x122.redpanda.core.admin.v2.internal.LogMessageRequest\x1a3.redpanda.core.admin.v2.internal.LogMessageResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.v1.debug_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._loaded_options = None
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._serialized_options = b'8\x01'
    _globals['_DEBUGSERVICE'].methods_by_name['ThrowStructuredException']._loaded_options = None
    _globals['_DEBUGSERVICE'].methods_by_name['ThrowStructuredException']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_DEBUGSERVICE'].methods_by_name['StartStressFiber']._loaded_options = None
    _globals['_DEBUGSERVICE'].methods_by_name['StartStressFiber']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_DEBUGSERVICE'].methods_by_name['StopStressFiber']._loaded_options = None
    _globals['_DEBUGSERVICE'].methods_by_name['StopStressFiber']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_DEBUGSERVICE'].methods_by_name['LogMessage']._loaded_options = None
    _globals['_DEBUGSERVICE'].methods_by_name['LogMessage']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LOGLEVEL']._serialized_start = 840
    _globals['_LOGLEVEL']._serialized_end = 980
    _globals['_STARTSTRESSFIBERREQUEST']._serialized_start = 165
    _globals['_STARTSTRESSFIBERREQUEST']._serialized_end = 386
    _globals['_STARTSTRESSFIBERRESPONSE']._serialized_start = 388
    _globals['_STARTSTRESSFIBERRESPONSE']._serialized_end = 414
    _globals['_STOPSTRESSFIBERREQUEST']._serialized_start = 416
    _globals['_STOPSTRESSFIBERREQUEST']._serialized_end = 440
    _globals['_STOPSTRESSFIBERRESPONSE']._serialized_start = 442
    _globals['_STOPSTRESSFIBERRESPONSE']._serialized_end = 467
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST']._serialized_start = 470
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST']._serialized_end = 683
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._serialized_start = 636
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._serialized_end = 683
    _globals['_THROWSTRUCTUREDEXCEPTIONRESPONSE']._serialized_start = 685
    _globals['_THROWSTRUCTUREDEXCEPTIONRESPONSE']._serialized_end = 719
    _globals['_LOGMESSAGEREQUEST']._serialized_start = 721
    _globals['_LOGMESSAGEREQUEST']._serialized_end = 815
    _globals['_LOGMESSAGERESPONSE']._serialized_start = 817
    _globals['_LOGMESSAGERESPONSE']._serialized_end = 837
    _globals['_DEBUGSERVICE']._serialized_start = 983
    _globals['_DEBUGSERVICE']._serialized_end = 1583