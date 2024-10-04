import { onRecordWritten, OnRecordWrittenEvent, RecordWriter } from "@redpanda-data/transform-sdk"
import { newClient, decodeSchemaID, SchemaFormat } from "@redpanda-data/transform-sdk-sr";
import { Type } from "avsc/lib/index.js"
import { Buffer } from "buffer"

var sr_client = newClient();
const schema = {
    type: "record",
    name: "Example",
    fields: [
        { "name": "a", "type": "long", "default": 0 },
        { "name": "b", "type": "string", "default": "" }
    ]
};

const subj_schema = sr_client.createSchema(
    "avro-value",
    {
        schema: JSON.stringify(schema),
        format: SchemaFormat.Avro,
        references: [],
    }
);

const orig_t = Type.forSchema(JSON.parse(subj_schema.schema.schema));

onRecordWritten((event: OnRecordWrittenEvent, writer: RecordWriter) => {
    var arr = event.record.value.array();
    const decoded = decodeSchemaID(arr);
    var schema = sr_client.lookupSchemaById(decoded.id);
    const t = Type.forSchema(JSON.parse(schema["schema"]));
    var buf = Buffer.from(decoded.rest);
    var val = t.fromBuffer(buf);
    buf = orig_t.toBuffer(val);
    val = orig_t.fromBuffer(buf);
    writer.write({
        key: event.record.key,
        value: JSON.stringify(val),
        headers: [],
    });
});
