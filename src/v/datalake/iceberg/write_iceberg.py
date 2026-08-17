import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import json
import os
import os.path
import time

base_dir = "/home/jim/Software/duckdb_iceberg/data/iceberg/lineitem_iceberg"
data_dir = os.path.join(base_dir, 'data')
metadata_dir = os.path.join(base_dir, 'metadata')


data_files = [
    "00000-411-0792dcfe-4e25-4ca3-8ada-175286069a47-00001.parquet",  "00041-414-f3c73457-bbd6-4b92-9c15-17b241171b16-00001.parquet"
]
data_files = [os.path.join(data_dir, d) for d in data_files]

# manifest_files = [
#     "10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m0.avro", "10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m1.avro"
# ]
# manifest_files = [os.path.join(metadata_dir, d) for d in manifest_files]

# manifest_list_file = os.path.join(metadata_dir, "snap-7635660646343998149-1-10eaca8a-1e1c-421e-ad6d-b232e5ee23d3.avro")
manifest_list_file = "generated/manifest_list.avro"

# manifest_paths = [
#     'lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m1.avro',
#     'lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m0.avro'
# ]

base_uri = 'file://' + base_dir

def main():
    write_manifest_file('generated/manifest', data_files, schema_string)

    schemas = json.loads(schema_string)
    snapshots = make_snapshots(manifest_list_file)
    metadata = json.dumps(make_metadata_dict(schemas, snapshots))
    with open("generated/metadata.json", "w") as md_fd:
        md_fd.write(metadata)

    write_manifest_list(['generated/manifest'], "generated/manifest_list.avro")

# Return a Python dict that can be converted to JSON
def make_metadata_dict(schemas, snapshots):
    return {
        'format-version': 2,
        'location': base_uri,
        'last-sequence-number': 2,
        'last-updated-ms': int(time.time() * 1000),
        'last-column-id': 16,
        'schemas': schemas,
        'current-schema-id': 0,
        'partition-specs': [
            {'spec-id': 0, 'fields': []},
        ],
        'default-spec-id': 0,
        'last-partition-id': 999,
        'snapshots': snapshots,
        'current-snapshot-id': 1,
    }

def make_snapshots(manifest_list_file):
    return [
        {
            'snapshot-id': 1,
            'sequence-number': 2,
            'timestamp-ms': int(time.time() * 1000),
            'manifest-list': manifest_list_file,
            'summary': {'operation': 'append'},
        },
    ]

def write_manifest_list(manifest_paths, filename):
    entries = make_manifest_entries(manifest_paths)
    schema = avro.schema.parse(open("manifest_file_schema.json", "rb").read())
    writer = DataFileWriter(open(filename, "wb"), DatumWriter(), schema)
    for entry in entries:
        writer.append(entry)
    writer.close()


def make_manifest_entries(manifest_paths):
    res = []
    for manifest_path in manifest_paths:
        manifest_length = os.stat(manifest_path).st_size
        entry = {
            'manifest_path': manifest_path,
            'manifest_length': manifest_length,
            'partition_spec_id': 0,
            'content': 0, # 0 means data, 1 means deletes
            'sequence_number': 2,
            'min_sequence_number': 0,
            'added_snapshot_id': 1,
            'added_data_files_count': len(manifest_paths),
            'existing_data_files_count': 0,
            'deleted_data_files_count': 0,
            'added_rows_count': 0,
            'existing_rows_count': 0,
            'deleted_rows_count': 0,
            'partitions': []
        }
        res.append(entry)
    return res

def write_manifest_file(manifest_file_name, data_files, table_schema):
    schema = avro.schema.parse(open("manifest_entry_schema.json", "rb").read())
    print(f"writing to {manifest_file_name}")
    writer = DataFileWriter(open(manifest_file_name, "wb"), DatumWriter(), schema)
    writer.meta['schema'] = schema_string_nolist.encode('utf-8')
    # writer.meta['schema-id'] = b'1'
    writer.meta['partition-spec'] = b'[]'
    writer.meta['partition-spec-id'] = b'0'
    writer.meta['format-version'] = b'2'
    writer.meta['content'] = b'data'
    # print("METADATA", writer.meta)

    for filename in data_files:
        file_size = os.stat(filename).st_size
        print(f"adding {filename}")
        entry = {
            'status': 1, # 0:existing, 1:added, 2:deleted
            'data_file': {
                'content': 0, # 0:data, 1:positional_deletes, 2:equality_deletes
                'file_path': filename,
                'file_format': 'PARQUET',
                'partition': {},
                'record_count': 1, # FIXME
                'file_size_in_bytes': file_size,
            },
        }
        writer.append(entry)
    writer.close()

manifest_file_record =    {'status': 2,
    'snapshot_id': 1,
    'sequence_number': None,
    'data_file': {
        'content': 0,
        'file_path': 'lineitem_iceberg/data/00000-411-0792dcfe-4e25-4ca3-8ada-175286069a47-00001.parquet',
        'file_format': 'PARQUET',
        'partition': {},
        'record_count': 60175,
        'file_size_in_bytes': 1390176,
        'column_sizes': [
            {'key': 1, 'value': 84424},
            {'key': 2, 'value': 87455},
            {'key': 3, 'value': 53095},
            {'key': 4, 'value': 10336},
            {'key': 5, 'value': 45294},
            {'key': 6, 'value': 227221},
            {'key': 7, 'value': 26671},
            {'key': 8, 'value': 24651},
            {'key': 9, 'value': 10797},
            {'key': 10, 'value': 6869},
            {'key': 11, 'value': 95073},
            {'key': 12, 'value': 94808},
            {'key': 13, 'value': 95153},
            {'key': 14, 'value': 15499},
            {'key': 15, 'value': 22569},
            {'key': 16, 'value': 484914}
            ],
        'value_counts': [
            {'key': 1, 'value': 60175},
            {'key': 2, 'value': 60175},
            {'key': 3, 'value': 60175},
            {'key': 4, 'value': 60175},
            {'key': 5, 'value': 60175}, 
            {'key': 6, 'value': 60175}, 
            {'key': 7, 'value': 60175}, 
            {'key': 8, 'value': 60175}, 
            {'key': 9, 'value': 60175}, 
            {'key': 10, 'value': 60175}, 
            {'key': 11, 'value': 60175}, 
            {'key': 12, 'value': 60175}, 
            {'key': 13, 'value': 60175}, 
            {'key': 14, 'value': 60175}, 
            {'key': 15, 'value': 60175}, 
            {'key': 16, 'value': 60175}
            ], 
        'null_value_counts': [
            {'key': 1, 'value': 0}, 
            {'key': 2, 'value': 0}, 
            {'key': 3, 'value': 0}, 
            {'key': 4, 'value': 0}, 
            {'key': 5, 'value': 0}, 
            {'key': 6, 'value': 0}, 
            {'key': 7, 'value': 0}, 
            {'key': 8, 'value': 0}, 
            {'key': 9, 'value': 0}, 
            {'key': 10, 'value': 0}, 
            {'key': 11, 'value': 0}, 
            {'key': 12, 'value': 0}, 
            {'key': 13, 'value': 0}, 
            {'key': 14, 'value': 0}, 
            {'key': 15, 'value': 0}, 
            {'key': 16, 'value': 0}
        ], 
        'nan_value_counts': [], 
        'lower_bounds': [
            {'key': 1, 'value': b'\x01\x00\x00\x00'}, 
            {'key': 2, 'value': b'\x01\x00\x00\x00'}, 
            {'key': 3, 'value': b'\x01\x00\x00\x00'}, 
            {'key': 4, 'value': b'\x01\x00\x00\x00'}, 
            {'key': 5, 'value': b'\x01\x00\x00\x00'}, 
            {'key': 6, 'value': b'\x01a '}, 
            {'key': 7, 'value': b'\x00'}, 
            {'key': 8, 'value': b'\x00'}, 
            {'key': 9, 'value': b'A'}, 
            {'key': 10, 'value': b'F'}, 
            {'key': 11, 'value': b'f\x1f\x00\x00'}, 
            {'key': 12, 'value': b'\x83\x1f\x00\x00'}, 
            {'key': 13, 'value': b'k\x1f\x00\x00'}, 
            {'key': 14, 'value': b'COLLECT COD'}, 
            {'key': 15, 'value': b'AIR'}, 
            {'key': 16, 'value': b' Tiresias '}
        ], 'upper_bounds': [
            {'key': 1, 'value': b'`\xea\x00\x00'}, 
            {'key': 2, 'value': b'\xd0\x07\x00\x00'}, 
            {'key': 3, 'value': b'd\x00\x00\x00'}, 
            {'key': 4, 'value': b'\x07\x00\x00\x00'}, 
            {'key': 5, 'value': b'2\x00\x00\x00'}, 
            {'key': 6, 'value': b'\x00\x90\xe1\xa6'}, 
            {'key': 7, 'value': b'\n'}, 
            {'key': 8, 'value': b'\x08'}, 
            {'key': 9, 'value': b'R'}, 
            {'key': 10, 'value': b'O'}, 
            {'key': 11, 'value': b'?)\x00\x00'}, 
            {'key': 12, 'value': b'\x1f)\x00\x00'}, 
            {'key': 13, 'value': b'Y)\x00\x00'}, 
            {'key': 14, 'value': b'TAKE BACK RETURN'}, 
            {'key': 15, 'value': b'TRUCK'}, 
            {'key': 16, 'value': b'zzle: pending i'}
        ], 
        'key_metadata': None, 
        'split_offsets': [4], 
        'equality_ids': None, 
        'sort_order_id': 0}
    }



{'manifest_path': 'lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m0.avro',
'manifest_length': 7687,
'partition_spec_id': 0,
'content': 0,
'sequence_number': 2,
'min_sequence_number': 2,
'added_snapshot_id': 7635660646343998149,
'added_data_files_count': 0,
'existing_data_files_count': 0,
'deleted_data_files_count': 1, 'added_rows_count': 0, 'existing_rows_count': 0, 'deleted_rows_count': 60175, 'partitions': []}



schema_string = '''
[ {
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "l_orderkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "l_partkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 3,
      "name" : "l_suppkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 4,
      "name" : "l_linenumber",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 5,
      "name" : "l_quantity",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 6,
      "name" : "l_extendedprice",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 7,
      "name" : "l_discount",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 8,
      "name" : "l_tax",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 9,
      "name" : "l_returnflag",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 10,
      "name" : "l_linestatus",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 11,
      "name" : "l_shipdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 12,
      "name" : "l_commitdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 13,
      "name" : "l_receiptdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 14,
      "name" : "l_shipinstruct",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 15,
      "name" : "l_shipmode",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 16,
      "name" : "l_comment",
      "required" : false,
      "type" : "string"
    } ]
  } ]
'''

schema_string_nolist = '''
{
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "l_orderkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "l_partkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 3,
      "name" : "l_suppkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 4,
      "name" : "l_linenumber",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 5,
      "name" : "l_quantity",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 6,
      "name" : "l_extendedprice",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 7,
      "name" : "l_discount",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 8,
      "name" : "l_tax",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 9,
      "name" : "l_returnflag",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 10,
      "name" : "l_linestatus",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 11,
      "name" : "l_shipdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 12,
      "name" : "l_commitdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 13,
      "name" : "l_receiptdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 14,
      "name" : "l_shipinstruct",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 15,
      "name" : "l_shipmode",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 16,
      "name" : "l_comment",
      "required" : false,
      "type" : "string"
    } ]
  }
'''


if __name__ == '__main__':
    main()