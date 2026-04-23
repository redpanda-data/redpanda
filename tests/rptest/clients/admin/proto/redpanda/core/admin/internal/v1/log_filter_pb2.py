"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/v1/log_filter.proto')
_sym_db = _symbol_database.Default()
from .......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from .......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n6proto/redpanda/core/admin/internal/v1/log_filter.proto\x12\x1fredpanda.core.admin.v2.internal\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"\x9d\x01\n\rLogFilterRule\x12\x11\n\x04file\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x0c\n\x04line\x18\x02 \x03(\r\x12\x15\n\x08contains\x18\x03 \x01(\tH\x01\x88\x01\x01\x12>\n\x05state\x18\x04 \x01(\x0e2/.redpanda.core.admin.v2.internal.LogFilterStateB\x07\n\x05_fileB\x0b\n\t_contains"z\n\x0fLogCallsiteInfo\x12\x0c\n\x04file\x18\x01 \x01(\t\x12\x0c\n\x04line\x18\x02 \x01(\r\x12\x0b\n\x03fmt\x18\x03 \x01(\t\x12>\n\x05state\x18\x04 \x01(\x0e2/.redpanda.core.admin.v2.internal.LogFilterState"T\n\x13SetLogFilterRequest\x12=\n\x05rules\x18\x01 \x03(\x0b2..redpanda.core.admin.v2.internal.LogFilterRule"\x16\n\x14SetLogFilterResponse"\x17\n\x15ResetLogFilterRequest"\x18\n\x16ResetLogFilterResponse"\x15\n\x13GetLogFilterRequest"U\n\x14GetLogFilterResponse\x12=\n\x05rules\x18\x01 \x03(\x0b2..redpanda.core.admin.v2.internal.LogFilterRule"C\n\x17ListLogCallsitesRequest\x12\x18\n\x0bfile_filter\x18\x01 \x01(\tH\x00\x88\x01\x01B\x0e\n\x0c_file_filter"_\n\x18ListLogCallsitesResponse\x12C\n\tcallsites\x18\x01 \x03(\x0b20.redpanda.core.admin.v2.internal.LogCallsiteInfo*\x91\x01\n\x0eLogFilterState\x12 \n\x1cLOG_FILTER_STATE_UNSPECIFIED\x10\x00\x12\x1e\n\x1aLOG_FILTER_STATE_INHERITED\x10\x01\x12\x1d\n\x19LOG_FILTER_STATE_FORCE_ON\x10\x02\x12\x1e\n\x1aLOG_FILTER_STATE_FORCE_OFF\x10\x032\xbc\x04\n\x10LogFilterService\x12\x83\x01\n\x0cSetLogFilter\x124.redpanda.core.admin.v2.internal.SetLogFilterRequest\x1a5.redpanda.core.admin.v2.internal.SetLogFilterResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x89\x01\n\x0eResetLogFilter\x126.redpanda.core.admin.v2.internal.ResetLogFilterRequest\x1a7.redpanda.core.admin.v2.internal.ResetLogFilterResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x83\x01\n\x0cGetLogFilter\x124.redpanda.core.admin.v2.internal.GetLogFilterRequest\x1a5.redpanda.core.admin.v2.internal.GetLogFilterResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8f\x01\n\x10ListLogCallsites\x128.redpanda.core.admin.v2.internal.ListLogCallsitesRequest\x1a9.redpanda.core.admin.v2.internal.ListLogCallsitesResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.v1.log_filter_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_LOGFILTERSERVICE'].methods_by_name['SetLogFilter']._loaded_options = None
    _globals['_LOGFILTERSERVICE'].methods_by_name['SetLogFilter']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LOGFILTERSERVICE'].methods_by_name['ResetLogFilter']._loaded_options = None
    _globals['_LOGFILTERSERVICE'].methods_by_name['ResetLogFilter']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LOGFILTERSERVICE'].methods_by_name['GetLogFilter']._loaded_options = None
    _globals['_LOGFILTERSERVICE'].methods_by_name['GetLogFilter']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LOGFILTERSERVICE'].methods_by_name['ListLogCallsites']._loaded_options = None
    _globals['_LOGFILTERSERVICE'].methods_by_name['ListLogCallsites']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LOGFILTERSTATE']._serialized_start = 891
    _globals['_LOGFILTERSTATE']._serialized_end = 1036
    _globals['_LOGFILTERRULE']._serialized_start = 170
    _globals['_LOGFILTERRULE']._serialized_end = 327
    _globals['_LOGCALLSITEINFO']._serialized_start = 329
    _globals['_LOGCALLSITEINFO']._serialized_end = 451
    _globals['_SETLOGFILTERREQUEST']._serialized_start = 453
    _globals['_SETLOGFILTERREQUEST']._serialized_end = 537
    _globals['_SETLOGFILTERRESPONSE']._serialized_start = 539
    _globals['_SETLOGFILTERRESPONSE']._serialized_end = 561
    _globals['_RESETLOGFILTERREQUEST']._serialized_start = 563
    _globals['_RESETLOGFILTERREQUEST']._serialized_end = 586
    _globals['_RESETLOGFILTERRESPONSE']._serialized_start = 588
    _globals['_RESETLOGFILTERRESPONSE']._serialized_end = 612
    _globals['_GETLOGFILTERREQUEST']._serialized_start = 614
    _globals['_GETLOGFILTERREQUEST']._serialized_end = 635
    _globals['_GETLOGFILTERRESPONSE']._serialized_start = 637
    _globals['_GETLOGFILTERRESPONSE']._serialized_end = 722
    _globals['_LISTLOGCALLSITESREQUEST']._serialized_start = 724
    _globals['_LISTLOGCALLSITESREQUEST']._serialized_end = 791
    _globals['_LISTLOGCALLSITESRESPONSE']._serialized_start = 793
    _globals['_LISTLOGCALLSITESRESPONSE']._serialized_end = 888
    _globals['_LOGFILTERSERVICE']._serialized_start = 1039
    _globals['_LOGFILTERSERVICE']._serialized_end = 1611