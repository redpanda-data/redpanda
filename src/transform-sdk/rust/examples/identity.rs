// Copyright 2023 Redpanda Data, Inc.
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

// This example shows the basic usage of the crate:
// This transform does nothing but copy the same data from an
// input topic to an output topic.
fn main() {
    // Make sure to register your callback and perform other setup in main
    on_record_written(my_transform);
}

// This will be called for each record in the source topic.
//
// The output records returned will be written to the destination topic.
fn my_transform(event: WriteEvent, writer: &mut RecordWriter) -> Result<()> {
    writer.write(event.record)?;
    Ok(())
}
