import {onRecordWritten} from "@redpanda-data/transform-sdk";

onRecordWritten((event, writer) => {
  console.warn(`${event.record.key.text()}:${event.record.value.text()}`);
  writer.write(event.record);
});
