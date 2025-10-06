"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/shadow_link.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.pbgen import options_pb2 as proto_dot_redpanda_dot_pbgen_dot_options__pb2
from ......proto.redpanda.pbgen import rpc_pb2 as proto_dot_redpanda_dot_pbgen_dot_rpc__pb2
from ......proto.redpanda.core.common import acl_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_acl__pb2
from ......google.api import field_behavior_pb2 as google_dot_api_dot_field__behavior__pb2
from ......google.api import field_info_pb2 as google_dot_api_dot_field__info__pb2
from ......google.api import resource_pb2 as google_dot_api_dot_resource__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import field_mask_pb2 as google_dot_protobuf_dot_field__mask__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n.proto/redpanda/core/admin/v2/shadow_link.proto\x12\x16redpanda.core.admin.v2\x1a"proto/redpanda/pbgen/options.proto\x1a\x1eproto/redpanda/pbgen/rpc.proto\x1a$proto/redpanda/core/common/acl.proto\x1a\x1fgoogle/api/field_behavior.proto\x1a\x1bgoogle/api/field_info.proto\x1a\x19google/api/resource.proto\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a google/protobuf/field_mask.proto"\xc2\x01\n\nShadowLink\x12\x11\n\x04name\x18\x01 \x01(\tB\x03\xe0A\x02\x12\x18\n\x03uid\x18\x02 \x01(\tB\x0b\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01\x12H\n\x0econfigurations\x18\x03 \x01(\x0b20.redpanda.core.admin.v2.ShadowLinkConfigurations\x12=\n\x06status\x18\x04 \x01(\x0b2(.redpanda.core.admin.v2.ShadowLinkStatusB\x03\xe0A\x03"R\n\x17CreateShadowLinkRequest\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"S\n\x18CreateShadowLinkResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"a\n\x17DeleteShadowLinkRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink"\x1a\n\x18DeleteShadowLinkResponse"^\n\x14GetShadowLinkRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink"P\n\x15GetShadowLinkResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"\x18\n\x16ListShadowLinksRequest"S\n\x17ListShadowLinksResponse\x128\n\x0cshadow_links\x18\x01 \x03(\x0b2".redpanda.core.admin.v2.ShadowLink"\x83\x01\n\x17UpdateShadowLinkRequest\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink\x12/\n\x0bupdate_mask\x18\x02 \x01(\x0b2\x1a.google.protobuf.FieldMask"S\n\x18UpdateShadowLinkResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"y\n\x0fFailOverRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink\x12\x1e\n\x11shadow_topic_name\x18\x02 \x01(\tB\x03\xe0A\x01"K\n\x10FailOverResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"\xe7\x02\n\x18ShadowLinkConfigurations\x12G\n\x0eclient_options\x18\x01 \x01(\x0b2/.redpanda.core.admin.v2.ShadowLinkClientOptions\x12U\n\x1btopic_metadata_sync_options\x18\x02 \x01(\x0b20.redpanda.core.admin.v2.TopicMetadataSyncOptions\x12W\n\x1cconsumer_offset_sync_options\x18\x03 \x01(\x0b21.redpanda.core.admin.v2.ConsumerOffsetSyncOptions\x12R\n\x15security_sync_options\x18\x04 \x01(\x0b23.redpanda.core.admin.v2.SecuritySettingsSyncOptions"\xe1\x03\n\x17ShadowLinkClientOptions\x12\x1e\n\x11bootstrap_servers\x18\x01 \x03(\tB\x03\xe0A\x02\x12\x16\n\tclient_id\x18\x02 \x01(\tB\x03\xe0A\x03\x12\x19\n\x11source_cluster_id\x18\x03 \x01(\t\x12>\n\x0ctls_settings\x18\x04 \x01(\x0b2#.redpanda.core.admin.v2.TLSSettingsH\x00\x88\x01\x01\x12^\n\x1cauthentication_configuration\x18\x05 \x01(\x0b23.redpanda.core.admin.v2.AuthenticationConfigurationH\x01\x88\x01\x01\x12\x1b\n\x13metadata_max_age_ms\x18\x06 \x01(\x05\x12\x1d\n\x15connection_timeout_ms\x18\x07 \x01(\x05\x12\x18\n\x10retry_backoff_ms\x18\x08 \x01(\x05\x12\x19\n\x11fetch_wait_max_ms\x18\t \x01(\x05\x12\x17\n\x0ffetch_min_bytes\x18\n \x01(\x05\x12\x17\n\x0ffetch_max_bytes\x18\x0b \x01(\x05B\x0f\n\r_tls_settingsB\x1f\n\x1d_authentication_configuration"\xb8\x01\n\x18TopicMetadataSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12L\n auto_create_shadow_topic_filters\x18\x02 \x03(\x0b2".redpanda.core.admin.v2.NameFilter\x12!\n\x19shadowed_topic_properties\x18\x03 \x03(\t"\x94\x01\n\x19ConsumerOffsetSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12\x0f\n\x07enabled\x18\x02 \x01(\x08\x129\n\rgroup_filters\x18\x03 \x03(\x0b2".redpanda.core.admin.v2.NameFilter"\x8d\x02\n\x1bSecuritySettingsSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12\x0f\n\x07enabled\x18\x02 \x01(\x08\x128\n\x0crole_filters\x18\x03 \x03(\x0b2".redpanda.core.admin.v2.NameFilter\x12>\n\x12scram_cred_filters\x18\x04 \x03(\x0b2".redpanda.core.admin.v2.NameFilter\x126\n\x0bacl_filters\x18\x05 \x03(\x0b2!.redpanda.core.admin.v2.ACLFilter"\xb8\x01\n\x0bTLSSettings\x12\x0f\n\x07enabled\x18\x03 \x01(\x08\x12D\n\x11tls_file_settings\x18\x01 \x01(\x0b2\'.redpanda.core.admin.v2.TLSFileSettingsH\x00\x12B\n\x10tls_pem_settings\x18\x02 \x01(\x0b2&.redpanda.core.admin.v2.TLSPEMSettingsH\x00B\x0e\n\x0ctls_settings"s\n\x1bAuthenticationConfiguration\x12B\n\x13scram_configuration\x18\x01 \x01(\x0b2#.redpanda.core.admin.v2.ScramConfigH\x00B\x10\n\x0eauthentication"G\n\x0fTLSFileSettings\x12\x0f\n\x07ca_path\x18\x01 \x01(\t\x12\x10\n\x08key_path\x18\x02 \x01(\t\x12\x11\n\tcert_path\x18\x03 \x01(\t"Z\n\x0eTLSPEMSettings\x12\n\n\x02ca\x18\x01 \x01(\t\x12\x10\n\x03key\x18\x02 \x01(\tB\x03\xe0A\x04\x12\x1c\n\x0fkey_fingerprint\x18\x03 \x01(\tB\x03\xe0A\x03\x12\x0c\n\x04cert\x18\x04 \x01(\t"\xcc\x01\n\x0bScramConfig\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x15\n\x08password\x18\x02 \x01(\tB\x03\xe0A\x04\x12\x19\n\x0cpassword_set\x18\x03 \x01(\x08B\x03\xe0A\x03\x128\n\x0fpassword_set_at\x18\x04 \x01(\x0b2\x1a.google.protobuf.TimestampB\x03\xe0A\x03\x12?\n\x0fscram_mechanism\x18\x05 \x01(\x0e2&.redpanda.core.admin.v2.ScramMechanism"\x8e\x01\n\nNameFilter\x129\n\x0cpattern_type\x18\x01 \x01(\x0e2#.redpanda.core.admin.v2.PatternType\x127\n\x0bfilter_type\x18\x02 \x01(\x0e2".redpanda.core.admin.v2.FilterType\x12\x0c\n\x04name\x18\x03 \x01(\t"\x8f\x01\n\tACLFilter\x12B\n\x0fresource_filter\x18\x01 \x01(\x0b2).redpanda.core.admin.v2.ACLResourceFilter\x12>\n\raccess_filter\x18\x02 \x01(\x0b2\'.redpanda.core.admin.v2.ACLAccessFilter"\x93\x01\n\x11ACLResourceFilter\x128\n\rresource_type\x18\x01 \x01(\x0e2!.redpanda.core.common.ACLResource\x126\n\x0cpattern_type\x18\x02 \x01(\x0e2 .redpanda.core.common.ACLPattern\x12\x0c\n\x04name\x18\x03 \x01(\t"\xab\x01\n\x0fACLAccessFilter\x12\x11\n\tprincipal\x18\x01 \x01(\t\x125\n\toperation\x18\x02 \x01(\x0e2".redpanda.core.common.ACLOperation\x12@\n\x0fpermission_type\x18\x03 \x01(\x0e2\'.redpanda.core.common.ACLPermissionType\x12\x0c\n\x04host\x18\x04 \x01(\t"\xd9\x01\n\x10ShadowLinkStatus\x126\n\x05state\x18\x01 \x01(\x0e2\'.redpanda.core.admin.v2.ShadowLinkState\x12C\n\rtask_statuses\x18\x02 \x03(\x0b2,.redpanda.core.admin.v2.ShadowLinkTaskStatus\x12H\n\x15shadow_topic_statuses\x18\x03 \x03(\x0b2).redpanda.core.admin.v2.ShadowTopicStatus"y\n\x14ShadowLinkTaskStatus\x12\x0c\n\x04name\x18\x01 \x01(\t\x120\n\x05state\x18\x02 \x01(\x0e2!.redpanda.core.admin.v2.TaskState\x12\x0e\n\x06reason\x18\x03 \x01(\t\x12\x11\n\tbroker_id\x18\x04 \x01(\x05"\xbe\x01\n\x11ShadowTopicStatus\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x10\n\x08topic_id\x18\x02 \x01(\t\x127\n\x05state\x18\x03 \x01(\x0e2(.redpanda.core.admin.v2.ShadowTopicState\x12P\n\x15partition_information\x18\x04 \x03(\x0b21.redpanda.core.admin.v2.TopicPartitionInformation"\x8b\x01\n\x19TopicPartitionInformation\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x03\x12!\n\x19source_last_stable_offset\x18\x02 \x01(\x03\x12\x1d\n\x15source_high_watermark\x18\x03 \x01(\x03\x12\x16\n\x0ehigh_watermark\x18\x04 \x01(\x03*p\n\x0fShadowLinkState\x12!\n\x1dSHADOW_LINK_STATE_UNSPECIFIED\x10\x00\x12\x1c\n\x18SHADOW_LINK_STATE_ACTIVE\x10\x01\x12\x1c\n\x18SHADOW_LINK_STATE_PAUSED\x10\x02*w\n\x0eScramMechanism\x12\x1f\n\x1bSCRAM_MECHANISM_UNSPECIFIED\x10\x00\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_256\x10\x01\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_512\x10\x02*^\n\x0bPatternType\x12\x1c\n\x18PATTERN_TYPE_UNSPECIFIED\x10\x00\x12\x18\n\x14PATTERN_TYPE_LITERAL\x10\x01\x12\x17\n\x13PATTERN_TYPE_PREFIX\x10\x02*[\n\nFilterType\x12\x1b\n\x17FILTER_TYPE_UNSPECIFIED\x10\x00\x12\x17\n\x13FILTER_TYPE_INCLUDE\x10\x01\x12\x17\n\x13FILTER_TYPE_EXCLUDE\x10\x02*\xaa\x01\n\tTaskState\x12\x1a\n\x16TASK_STATE_UNSPECIFIED\x10\x00\x12\x15\n\x11TASK_STATE_ACTIVE\x10\x01\x12\x15\n\x11TASK_STATE_PAUSED\x10\x02\x12\x1f\n\x1bTASK_STATE_LINK_UNAVAILABLE\x10\x03\x12\x1a\n\x16TASK_STATE_NOT_RUNNING\x10\x04\x12\x16\n\x12TASK_STATE_FAULTED\x10\x05*\xa0\x02\n\x10ShadowTopicState\x12"\n\x1eSHADOW_TOPIC_STATE_UNSPECIFIED\x10\x00\x12\x1d\n\x19SHADOW_TOPIC_STATE_ACTIVE\x10\x01\x12\x1e\n\x1aSHADOW_TOPIC_STATE_FAULTED\x10\x02\x12\x1d\n\x19SHADOW_TOPIC_STATE_PAUSED\x10\x03\x12#\n\x1fSHADOW_TOPIC_STATE_FAILING_OVER\x10\x04\x12"\n\x1eSHADOW_TOPIC_STATE_FAILED_OVER\x10\x05\x12 \n\x1cSHADOW_TOPIC_STATE_PROMOTING\x10\x06\x12\x1f\n\x1bSHADOW_TOPIC_STATE_PROMOTED\x10\x072\xe9\x05\n\x11ShadowLinkService\x12}\n\x10CreateShadowLink\x12/.redpanda.core.admin.v2.CreateShadowLinkRequest\x1a0.redpanda.core.admin.v2.CreateShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12}\n\x10DeleteShadowLink\x12/.redpanda.core.admin.v2.DeleteShadowLinkRequest\x1a0.redpanda.core.admin.v2.DeleteShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12t\n\rGetShadowLink\x12,.redpanda.core.admin.v2.GetShadowLinkRequest\x1a-.redpanda.core.admin.v2.GetShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12z\n\x0fListShadowLinks\x12..redpanda.core.admin.v2.ListShadowLinksRequest\x1a/.redpanda.core.admin.v2.ListShadowLinksResponse"\x06\xea\x92\x19\x02\x10\x03\x12}\n\x10UpdateShadowLink\x12/.redpanda.core.admin.v2.UpdateShadowLinkRequest\x1a0.redpanda.core.admin.v2.UpdateShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12e\n\x08FailOver\x12\'.redpanda.core.admin.v2.FailOverRequest\x1a(.redpanda.core.admin.v2.FailOverResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.shadow_link_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_SHADOWLINK'].fields_by_name['name']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['name']._serialized_options = b'\xe0A\x02'
    _globals['_SHADOWLINK'].fields_by_name['uid']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['uid']._serialized_options = b'\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01'
    _globals['_SHADOWLINK'].fields_by_name['status']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['status']._serialized_options = b'\xe0A\x03'
    _globals['_DELETESHADOWLINKREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_DELETESHADOWLINKREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_GETSHADOWLINKREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_GETSHADOWLINKREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_FAILOVERREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_FAILOVERREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_FAILOVERREQUEST'].fields_by_name['shadow_topic_name']._loaded_options = None
    _globals['_FAILOVERREQUEST'].fields_by_name['shadow_topic_name']._serialized_options = b'\xe0A\x01'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['bootstrap_servers']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['bootstrap_servers']._serialized_options = b'\xe0A\x02'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['client_id']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['client_id']._serialized_options = b'\xe0A\x03'
    _globals['_TLSPEMSETTINGS'].fields_by_name['key']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['key']._serialized_options = b'\xe0A\x04'
    _globals['_TLSPEMSETTINGS'].fields_by_name['key_fingerprint']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['key_fingerprint']._serialized_options = b'\xe0A\x03'
    _globals['_SCRAMCONFIG'].fields_by_name['password']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password']._serialized_options = b'\xe0A\x04'
    _globals['_SCRAMCONFIG'].fields_by_name['password_set']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password_set']._serialized_options = b'\xe0A\x03'
    _globals['_SCRAMCONFIG'].fields_by_name['password_set_at']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password_set_at']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['CreateShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['CreateShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['DeleteShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['DeleteShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowLinks']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowLinks']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['UpdateShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['UpdateShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['FailOver']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['FailOver']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSTATE']._serialized_start = 4994
    _globals['_SHADOWLINKSTATE']._serialized_end = 5106
    _globals['_SCRAMMECHANISM']._serialized_start = 5108
    _globals['_SCRAMMECHANISM']._serialized_end = 5227
    _globals['_PATTERNTYPE']._serialized_start = 5229
    _globals['_PATTERNTYPE']._serialized_end = 5323
    _globals['_FILTERTYPE']._serialized_start = 5325
    _globals['_FILTERTYPE']._serialized_end = 5416
    _globals['_TASKSTATE']._serialized_start = 5419
    _globals['_TASKSTATE']._serialized_end = 5589
    _globals['_SHADOWTOPICSTATE']._serialized_start = 5592
    _globals['_SHADOWTOPICSTATE']._serialized_end = 5880
    _globals['_SHADOWLINK']._serialized_start = 369
    _globals['_SHADOWLINK']._serialized_end = 563
    _globals['_CREATESHADOWLINKREQUEST']._serialized_start = 565
    _globals['_CREATESHADOWLINKREQUEST']._serialized_end = 647
    _globals['_CREATESHADOWLINKRESPONSE']._serialized_start = 649
    _globals['_CREATESHADOWLINKRESPONSE']._serialized_end = 732
    _globals['_DELETESHADOWLINKREQUEST']._serialized_start = 734
    _globals['_DELETESHADOWLINKREQUEST']._serialized_end = 831
    _globals['_DELETESHADOWLINKRESPONSE']._serialized_start = 833
    _globals['_DELETESHADOWLINKRESPONSE']._serialized_end = 859
    _globals['_GETSHADOWLINKREQUEST']._serialized_start = 861
    _globals['_GETSHADOWLINKREQUEST']._serialized_end = 955
    _globals['_GETSHADOWLINKRESPONSE']._serialized_start = 957
    _globals['_GETSHADOWLINKRESPONSE']._serialized_end = 1037
    _globals['_LISTSHADOWLINKSREQUEST']._serialized_start = 1039
    _globals['_LISTSHADOWLINKSREQUEST']._serialized_end = 1063
    _globals['_LISTSHADOWLINKSRESPONSE']._serialized_start = 1065
    _globals['_LISTSHADOWLINKSRESPONSE']._serialized_end = 1148
    _globals['_UPDATESHADOWLINKREQUEST']._serialized_start = 1151
    _globals['_UPDATESHADOWLINKREQUEST']._serialized_end = 1282
    _globals['_UPDATESHADOWLINKRESPONSE']._serialized_start = 1284
    _globals['_UPDATESHADOWLINKRESPONSE']._serialized_end = 1367
    _globals['_FAILOVERREQUEST']._serialized_start = 1369
    _globals['_FAILOVERREQUEST']._serialized_end = 1490
    _globals['_FAILOVERRESPONSE']._serialized_start = 1492
    _globals['_FAILOVERRESPONSE']._serialized_end = 1567
    _globals['_SHADOWLINKCONFIGURATIONS']._serialized_start = 1570
    _globals['_SHADOWLINKCONFIGURATIONS']._serialized_end = 1929
    _globals['_SHADOWLINKCLIENTOPTIONS']._serialized_start = 1932
    _globals['_SHADOWLINKCLIENTOPTIONS']._serialized_end = 2413
    _globals['_TOPICMETADATASYNCOPTIONS']._serialized_start = 2416
    _globals['_TOPICMETADATASYNCOPTIONS']._serialized_end = 2600
    _globals['_CONSUMEROFFSETSYNCOPTIONS']._serialized_start = 2603
    _globals['_CONSUMEROFFSETSYNCOPTIONS']._serialized_end = 2751
    _globals['_SECURITYSETTINGSSYNCOPTIONS']._serialized_start = 2754
    _globals['_SECURITYSETTINGSSYNCOPTIONS']._serialized_end = 3023
    _globals['_TLSSETTINGS']._serialized_start = 3026
    _globals['_TLSSETTINGS']._serialized_end = 3210
    _globals['_AUTHENTICATIONCONFIGURATION']._serialized_start = 3212
    _globals['_AUTHENTICATIONCONFIGURATION']._serialized_end = 3327
    _globals['_TLSFILESETTINGS']._serialized_start = 3329
    _globals['_TLSFILESETTINGS']._serialized_end = 3400
    _globals['_TLSPEMSETTINGS']._serialized_start = 3402
    _globals['_TLSPEMSETTINGS']._serialized_end = 3492
    _globals['_SCRAMCONFIG']._serialized_start = 3495
    _globals['_SCRAMCONFIG']._serialized_end = 3699
    _globals['_NAMEFILTER']._serialized_start = 3702
    _globals['_NAMEFILTER']._serialized_end = 3844
    _globals['_ACLFILTER']._serialized_start = 3847
    _globals['_ACLFILTER']._serialized_end = 3990
    _globals['_ACLRESOURCEFILTER']._serialized_start = 3993
    _globals['_ACLRESOURCEFILTER']._serialized_end = 4140
    _globals['_ACLACCESSFILTER']._serialized_start = 4143
    _globals['_ACLACCESSFILTER']._serialized_end = 4314
    _globals['_SHADOWLINKSTATUS']._serialized_start = 4317
    _globals['_SHADOWLINKSTATUS']._serialized_end = 4534
    _globals['_SHADOWLINKTASKSTATUS']._serialized_start = 4536
    _globals['_SHADOWLINKTASKSTATUS']._serialized_end = 4657
    _globals['_SHADOWTOPICSTATUS']._serialized_start = 4660
    _globals['_SHADOWTOPICSTATUS']._serialized_end = 4850
    _globals['_TOPICPARTITIONINFORMATION']._serialized_start = 4853
    _globals['_TOPICPARTITIONINFORMATION']._serialized_end = 4992
    _globals['_SHADOWLINKSERVICE']._serialized_start = 5883
    _globals['_SHADOWLINKSERVICE']._serialized_end = 6628