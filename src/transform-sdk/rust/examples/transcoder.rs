// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;
use redpanda_transform_sdk::*;
use serde::{Deserialize, Serialize};

// This example shows a transform that converts CSV inputs into JSON outputs.
fn main() {
    on_record_written(my_transform);
}

#[derive(Serialize, Deserialize)]
struct Foo {
    a: String,
    b: i32,
}

fn my_transform(event: WriteEvent, writer: &mut RecordWriter) -> Result<()> {
    // The input data is a CSV (without a header row) that is defined as our Foo structure.
    let mut reader = csv::Reader::from_reader(event.record.value().unwrap_or_default());
    // For each record in our CSV
    for result in reader.deserialize() {
        let foo: Foo = result?;
        // Convert it to JSON
        let value = serde_json::to_vec(&foo)?;
        // Then output it with the same key.
        writer.write(BorrowedRecord::new(event.record.key(), Some(&value)))?;
    }
    Ok(())
}
