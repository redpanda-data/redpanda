"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/cloud_topics/v1/metastore.proto')
_sym_db = _symbol_database.Default()
from ........proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ........proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from ........proto.redpanda.core.common.v1 import ntp_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_v1_dot_ntp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\nBproto/redpanda/core/admin/internal/cloud_topics/v1/metastore.proto\x12,redpanda.core.admin.internal.cloud_topics.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\'proto/redpanda/core/common/v1/ntp.proto"O\n\x11GetOffsetsRequest\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition"\\\n\x12GetOffsetsResponse\x12F\n\x07offsets\x18\x02 \x01(\x0b25.redpanda.core.admin.internal.cloud_topics.v1.Offsets"4\n\x07Offsets\x12\x14\n\x0cstart_offset\x18\x01 \x01(\x03\x12\x13\n\x0bnext_offset\x18\x02 \x01(\x03"L\n\x0eGetSizeRequest\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition"%\n\x0fGetSizeResponse\x12\x12\n\nsize_bytes\x18\x01 \x01(\x04"6\n\x17GetDatabaseStatsRequest\x12\x1b\n\x13metastore_partition\x18\x01 \x01(\r"\xbd\x01\n\x18GetDatabaseStatsResponse\x12\x1d\n\x15active_memtable_bytes\x18\x01 \x01(\x04\x12 \n\x18immutable_memtable_bytes\x18\x02 \x01(\x04\x12\x18\n\x10total_size_bytes\x18\x03 \x01(\x04\x12F\n\x06levels\x18\x04 \x03(\x0b26.redpanda.core.admin.internal.cloud_topics.v1.LsmLevel"f\n\x08LsmLevel\x12\x14\n\x0clevel_number\x18\x01 \x01(\x05\x12D\n\x05files\x18\x02 \x03(\x0b25.redpanda.core.admin.internal.cloud_topics.v1.LsmFile"m\n\x07LsmFile\x12\r\n\x05epoch\x18\x01 \x01(\x04\x12\n\n\x02id\x18\x02 \x01(\x04\x12\x12\n\nsize_bytes\x18\x03 \x01(\x04\x12\x19\n\x11smallest_key_info\x18\x04 \x01(\t\x12\x18\n\x10largest_key_info\x18\x05 \x01(\t"5\n\x0bMetadataKey\x12\x10\n\x08topic_id\x18\x01 \x01(\t\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05"H\n\tExtentKey\x12\x10\n\x08topic_id\x18\x01 \x01(\t\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05\x12\x13\n\x0bbase_offset\x18\x03 \x01(\x03"B\n\x07TermKey\x12\x10\n\x08topic_id\x18\x01 \x01(\t\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05\x12\x0f\n\x07term_id\x18\x03 \x01(\x03"7\n\rCompactionKey\x12\x10\n\x08topic_id\x18\x01 \x01(\t\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05"\x1e\n\tObjectKey\x12\x11\n\tobject_id\x18\x01 \x01(\t"\x8e\x03\n\x06RowKey\x12M\n\x08metadata\x18\x01 \x01(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.MetadataKeyH\x00\x12I\n\x06extent\x18\x02 \x01(\x0b27.redpanda.core.admin.internal.cloud_topics.v1.ExtentKeyH\x00\x12E\n\x04term\x18\x03 \x01(\x0b25.redpanda.core.admin.internal.cloud_topics.v1.TermKeyH\x00\x12Q\n\ncompaction\x18\x04 \x01(\x0b2;.redpanda.core.admin.internal.cloud_topics.v1.CompactionKeyH\x00\x12I\n\x06object\x18\x05 \x01(\x0b27.redpanda.core.admin.internal.cloud_topics.v1.ObjectKeyH\x00B\x05\n\x03key"w\n\rMetadataValue\x12\x14\n\x0cstart_offset\x18\x01 \x01(\x03\x12\x13\n\x0bnext_offset\x18\x02 \x01(\x03\x12\x18\n\x10compaction_epoch\x18\x03 \x01(\x03\x12\x0c\n\x04size\x18\x04 \x01(\x04\x12\x13\n\x0bnum_extents\x18\x05 \x01(\x04"j\n\x0bExtentValue\x12\x13\n\x0blast_offset\x18\x01 \x01(\x03\x12\x15\n\rmax_timestamp\x18\x02 \x01(\x03\x12\x0f\n\x07filepos\x18\x03 \x01(\x04\x12\x0b\n\x03len\x18\x04 \x01(\x04\x12\x11\n\tobject_id\x18\x05 \x01(\t"&\n\tTermValue\x12\x19\n\x11term_start_offset\x18\x01 \x01(\x03"7\n\x0bOffsetRange\x12\x13\n\x0bbase_offset\x18\x01 \x01(\x03\x12\x13\n\x0blast_offset\x18\x02 \x01(\x03"j\n\x1aCleanedRangeWithTombstones\x12\x13\n\x0bbase_offset\x18\x01 \x01(\x03\x12\x13\n\x0blast_offset\x18\x02 \x01(\x03\x12"\n\x1acleaned_with_tombstones_at\x18\x03 \x01(\x03"\xd6\x01\n\x0fCompactionValue\x12Q\n\x0ecleaned_ranges\x18\x01 \x03(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.OffsetRange\x12p\n\x1ecleaned_ranges_with_tombstones\x18\x02 \x03(\x0b2H.redpanda.core.admin.internal.cloud_topics.v1.CleanedRangeWithTombstones"\x9c\x01\n\x0bObjectValue\x12\x17\n\x0ftotal_data_size\x18\x01 \x01(\x04\x12\x19\n\x11removed_data_size\x18\x02 \x01(\x04\x12\x12\n\nfooter_pos\x18\x03 \x01(\x04\x12\x13\n\x0bobject_size\x18\x04 \x01(\x04\x12\x14\n\x0clast_updated\x18\x05 \x01(\x03\x12\x1a\n\x12is_preregistration\x18\x06 \x01(\x08"\x9c\x03\n\x08RowValue\x12O\n\x08metadata\x18\x01 \x01(\x0b2;.redpanda.core.admin.internal.cloud_topics.v1.MetadataValueH\x00\x12K\n\x06extent\x18\x02 \x01(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.ExtentValueH\x00\x12G\n\x04term\x18\x03 \x01(\x0b27.redpanda.core.admin.internal.cloud_topics.v1.TermValueH\x00\x12S\n\ncompaction\x18\x04 \x01(\x0b2=.redpanda.core.admin.internal.cloud_topics.v1.CompactionValueH\x00\x12K\n\x06object\x18\x05 \x01(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.ObjectValueH\x00B\x07\n\x05value"\x94\x01\n\x08WriteRow\x12A\n\x03key\x18\x01 \x01(\x0b24.redpanda.core.admin.internal.cloud_topics.v1.RowKey\x12E\n\x05value\x18\x02 \x01(\x0b26.redpanda.core.admin.internal.cloud_topics.v1.RowValue"\xbe\x01\n\x10WriteRowsRequest\x12\x1b\n\x13metastore_partition\x18\x01 \x01(\r\x12F\n\x06writes\x18\x02 \x03(\x0b26.redpanda.core.admin.internal.cloud_topics.v1.WriteRow\x12E\n\x07deletes\x18\x03 \x03(\x0b24.redpanda.core.admin.internal.cloud_topics.v1.RowKey")\n\x11WriteRowsResponse\x12\x14\n\x0crows_written\x18\x01 \x01(\r"\x93\x01\n\x07ReadRow\x12A\n\x03key\x18\x01 \x01(\x0b24.redpanda.core.admin.internal.cloud_topics.v1.RowKey\x12E\n\x05value\x18\x02 \x01(\x0b26.redpanda.core.admin.internal.cloud_topics.v1.RowValue"\x94\x02\n\x0fReadRowsRequest\x12\x1b\n\x13metastore_partition\x18\x01 \x01(\r\x12H\n\x08seek_key\x18\x02 \x01(\x0b24.redpanda.core.admin.internal.cloud_topics.v1.RowKeyH\x00\x12\x16\n\x0craw_seek_key\x18\x03 \x01(\tH\x00\x12H\n\x08last_key\x18\x04 \x01(\x0b24.redpanda.core.admin.internal.cloud_topics.v1.RowKeyH\x01\x12\x16\n\x0craw_last_key\x18\x05 \x01(\tH\x01\x12\x10\n\x08max_rows\x18\x06 \x01(\rB\x06\n\x04seekB\x06\n\x04last"i\n\x10ReadRowsResponse\x12C\n\x04rows\x18\x01 \x03(\x0b25.redpanda.core.admin.internal.cloud_topics.v1.ReadRow\x12\x10\n\x08next_key\x18\x02 \x01(\t"\xc8\x01\n\x18ValidatePartitionRequest\x12\x10\n\x08topic_id\x18\x01 \x01(\t\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05\x12\x1d\n\x15check_object_metadata\x18\x03 \x01(\x08\x12\x1c\n\x14check_object_storage\x18\x04 \x01(\x08\x12\x1d\n\x10resume_at_offset\x18\x05 \x01(\x03H\x00\x88\x01\x01\x12\x13\n\x0bmax_extents\x18\x06 \x01(\rB\x13\n\x11_resume_at_offset"\xbd\x01\n\x19ValidatePartitionResponse\x12Q\n\tanomalies\x18\x01 \x03(\x0b2>.redpanda.core.admin.internal.cloud_topics.v1.MetastoreAnomaly\x12\x1d\n\x10resume_at_offset\x18\x02 \x01(\x03H\x00\x88\x01\x01\x12\x19\n\x11extents_validated\x18\x03 \x01(\rB\x13\n\x11_resume_at_offset"x\n\x10MetastoreAnomaly\x12O\n\x0canomaly_type\x18\x01 \x01(\x0e29.redpanda.core.admin.internal.cloud_topics.v1.AnomalyType\x12\x13\n\x0bdescription\x18\x02 \x01(\t"F\n\x16ListCloudTopicsRequest\x12\x18\n\x10after_topic_name\x18\x01 \x01(\t\x12\x12\n\nmax_topics\x18\x02 \x01(\r"y\n\x17ListCloudTopicsResponse\x12L\n\x06topics\x18\x01 \x03(\x0b2<.redpanda.core.admin.internal.cloud_topics.v1.CloudTopicInfo\x12\x10\n\x08has_more\x18\x02 \x01(\x08"O\n\x0eCloudTopicInfo\x12\x12\n\ntopic_name\x18\x01 \x01(\t\x12\x10\n\x08topic_id\x18\x02 \x01(\t\x12\x17\n\x0fpartition_count\x18\x03 \x01(\x05*\xbb\x04\n\x0bAnomalyType\x12\x1c\n\x18ANOMALY_TYPE_UNSPECIFIED\x10\x00\x12\x1f\n\x1bANOMALY_TYPE_EXTENT_OVERLAP\x10\x01\x12\x1b\n\x17ANOMALY_TYPE_EXTENT_GAP\x10\x02\x12%\n!ANOMALY_TYPE_NEXT_OFFSET_MISMATCH\x10\x03\x12&\n"ANOMALY_TYPE_START_OFFSET_MISMATCH\x10\x04\x12!\n\x1dANOMALY_TYPE_OBJECT_NOT_FOUND\x10\x05\x12%\n!ANOMALY_TYPE_OBJECT_PREREGISTERED\x10\x06\x12&\n"ANOMALY_TYPE_OBJECT_NOT_IN_STORAGE\x10\x07\x12-\n)ANOMALY_TYPE_COMPACTION_RANGE_BELOW_START\x10\x08\x12,\n(ANOMALY_TYPE_COMPACTION_RANGE_ABOVE_NEXT\x10\t\x12-\n)ANOMALY_TYPE_COMPACTION_TOMBSTONE_OVERLAP\x10\n\x125\n1ANOMALY_TYPE_COMPACTION_TOMBSTONE_OUTSIDE_CLEANED\x10\x0b\x12\x1e\n\x1aANOMALY_TYPE_TERM_ORDERING\x10\x0c\x12,\n(ANOMALY_TYPE_COMPACTION_STATE_UNEXPECTED\x10\r2\xec\x08\n\x10MetastoreService\x12\x97\x01\n\nGetOffsets\x12?.redpanda.core.admin.internal.cloud_topics.v1.GetOffsetsRequest\x1a@.redpanda.core.admin.internal.cloud_topics.v1.GetOffsetsResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x8e\x01\n\x07GetSize\x12<.redpanda.core.admin.internal.cloud_topics.v1.GetSizeRequest\x1a=.redpanda.core.admin.internal.cloud_topics.v1.GetSizeResponse"\x06\xea\x92\x19\x02\x10\x03\x12\xa9\x01\n\x10GetDatabaseStats\x12E.redpanda.core.admin.internal.cloud_topics.v1.GetDatabaseStatsRequest\x1aF.redpanda.core.admin.internal.cloud_topics.v1.GetDatabaseStatsResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x94\x01\n\tWriteRows\x12>.redpanda.core.admin.internal.cloud_topics.v1.WriteRowsRequest\x1a?.redpanda.core.admin.internal.cloud_topics.v1.WriteRowsResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x91\x01\n\x08ReadRows\x12=.redpanda.core.admin.internal.cloud_topics.v1.ReadRowsRequest\x1a>.redpanda.core.admin.internal.cloud_topics.v1.ReadRowsResponse"\x06\xea\x92\x19\x02\x10\x03\x12\xa6\x01\n\x0fListCloudTopics\x12D.redpanda.core.admin.internal.cloud_topics.v1.ListCloudTopicsRequest\x1aE.redpanda.core.admin.internal.cloud_topics.v1.ListCloudTopicsResponse"\x06\xea\x92\x19\x02\x10\x03\x12\xac\x01\n\x11ValidatePartition\x12F.redpanda.core.admin.internal.cloud_topics.v1.ValidatePartitionRequest\x1aG.redpanda.core.admin.internal.cloud_topics.v1.ValidatePartitionResponse"\x06\xea\x92\x19\x02\x10\x03B\x1b\xea\x92\x19\x17proto::admin::metastoreb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x17proto::admin::metastore'
    _globals['_METASTORESERVICE'].methods_by_name['GetOffsets']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['GetOffsets']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_METASTORESERVICE'].methods_by_name['GetSize']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['GetSize']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_METASTORESERVICE'].methods_by_name['GetDatabaseStats']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['GetDatabaseStats']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_METASTORESERVICE'].methods_by_name['WriteRows']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['WriteRows']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_METASTORESERVICE'].methods_by_name['ReadRows']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['ReadRows']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_METASTORESERVICE'].methods_by_name['ListCloudTopics']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['ListCloudTopics']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_METASTORESERVICE'].methods_by_name['ValidatePartition']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['ValidatePartition']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_ANOMALYTYPE']._serialized_start = 4673
    _globals['_ANOMALYTYPE']._serialized_end = 5244
    _globals['_GETOFFSETSREQUEST']._serialized_start = 235
    _globals['_GETOFFSETSREQUEST']._serialized_end = 314
    _globals['_GETOFFSETSRESPONSE']._serialized_start = 316
    _globals['_GETOFFSETSRESPONSE']._serialized_end = 408
    _globals['_OFFSETS']._serialized_start = 410
    _globals['_OFFSETS']._serialized_end = 462
    _globals['_GETSIZEREQUEST']._serialized_start = 464
    _globals['_GETSIZEREQUEST']._serialized_end = 540
    _globals['_GETSIZERESPONSE']._serialized_start = 542
    _globals['_GETSIZERESPONSE']._serialized_end = 579
    _globals['_GETDATABASESTATSREQUEST']._serialized_start = 581
    _globals['_GETDATABASESTATSREQUEST']._serialized_end = 635
    _globals['_GETDATABASESTATSRESPONSE']._serialized_start = 638
    _globals['_GETDATABASESTATSRESPONSE']._serialized_end = 827
    _globals['_LSMLEVEL']._serialized_start = 829
    _globals['_LSMLEVEL']._serialized_end = 931
    _globals['_LSMFILE']._serialized_start = 933
    _globals['_LSMFILE']._serialized_end = 1042
    _globals['_METADATAKEY']._serialized_start = 1044
    _globals['_METADATAKEY']._serialized_end = 1097
    _globals['_EXTENTKEY']._serialized_start = 1099
    _globals['_EXTENTKEY']._serialized_end = 1171
    _globals['_TERMKEY']._serialized_start = 1173
    _globals['_TERMKEY']._serialized_end = 1239
    _globals['_COMPACTIONKEY']._serialized_start = 1241
    _globals['_COMPACTIONKEY']._serialized_end = 1296
    _globals['_OBJECTKEY']._serialized_start = 1298
    _globals['_OBJECTKEY']._serialized_end = 1328
    _globals['_ROWKEY']._serialized_start = 1331
    _globals['_ROWKEY']._serialized_end = 1729
    _globals['_METADATAVALUE']._serialized_start = 1731
    _globals['_METADATAVALUE']._serialized_end = 1850
    _globals['_EXTENTVALUE']._serialized_start = 1852
    _globals['_EXTENTVALUE']._serialized_end = 1958
    _globals['_TERMVALUE']._serialized_start = 1960
    _globals['_TERMVALUE']._serialized_end = 1998
    _globals['_OFFSETRANGE']._serialized_start = 2000
    _globals['_OFFSETRANGE']._serialized_end = 2055
    _globals['_CLEANEDRANGEWITHTOMBSTONES']._serialized_start = 2057
    _globals['_CLEANEDRANGEWITHTOMBSTONES']._serialized_end = 2163
    _globals['_COMPACTIONVALUE']._serialized_start = 2166
    _globals['_COMPACTIONVALUE']._serialized_end = 2380
    _globals['_OBJECTVALUE']._serialized_start = 2383
    _globals['_OBJECTVALUE']._serialized_end = 2539
    _globals['_ROWVALUE']._serialized_start = 2542
    _globals['_ROWVALUE']._serialized_end = 2954
    _globals['_WRITEROW']._serialized_start = 2957
    _globals['_WRITEROW']._serialized_end = 3105
    _globals['_WRITEROWSREQUEST']._serialized_start = 3108
    _globals['_WRITEROWSREQUEST']._serialized_end = 3298
    _globals['_WRITEROWSRESPONSE']._serialized_start = 3300
    _globals['_WRITEROWSRESPONSE']._serialized_end = 3341
    _globals['_READROW']._serialized_start = 3344
    _globals['_READROW']._serialized_end = 3491
    _globals['_READROWSREQUEST']._serialized_start = 3494
    _globals['_READROWSREQUEST']._serialized_end = 3770
    _globals['_READROWSRESPONSE']._serialized_start = 3772
    _globals['_READROWSRESPONSE']._serialized_end = 3877
    _globals['_VALIDATEPARTITIONREQUEST']._serialized_start = 3880
    _globals['_VALIDATEPARTITIONREQUEST']._serialized_end = 4080
    _globals['_VALIDATEPARTITIONRESPONSE']._serialized_start = 4083
    _globals['_VALIDATEPARTITIONRESPONSE']._serialized_end = 4272
    _globals['_METASTOREANOMALY']._serialized_start = 4274
    _globals['_METASTOREANOMALY']._serialized_end = 4394
    _globals['_LISTCLOUDTOPICSREQUEST']._serialized_start = 4396
    _globals['_LISTCLOUDTOPICSREQUEST']._serialized_end = 4466
    _globals['_LISTCLOUDTOPICSRESPONSE']._serialized_start = 4468
    _globals['_LISTCLOUDTOPICSRESPONSE']._serialized_end = 4589
    _globals['_CLOUDTOPICINFO']._serialized_start = 4591
    _globals['_CLOUDTOPICINFO']._serialized_end = 4670
    _globals['_METASTORESERVICE']._serialized_start = 5247
    _globals['_METASTORESERVICE']._serialized_end = 6379