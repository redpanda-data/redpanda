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

// This is an example showing logging for each transformed records. These logs are output to
// _redpanda.transform_logs
fn main() {
    on_record_written(my_transform);
}

fn my_transform(event: WriteEvent, writer: &mut RecordWriter) -> Result<()> {
    let key = event.record.key().unwrap_or_default();
    let value = event.record.value().unwrap_or_default();
    let msg = format!(
        "{}:{}\n",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(value)
    );
    eprint!("{}", msg);
    writer.write(event.record)?;
    Ok(())
}
