use crate::batch_reader::BatchBuffer;
use log::info;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// TODO: consider how to handle the harder case of creating S3 objects,
/// where the key name depends on state that we don't know yet when
/// we see the first batch, incl. the overall size of the resulting segment.

/// Given a stream of batches, break it up into segments
pub struct SegmentWriter {
    cur_file: Option<File>,
    cur_file_bytes: u64,
    target_segment_size: u64,
    directory: std::path::PathBuf,
}

#[derive(Debug)]
pub enum SegmentWriterError {
    IOError(std::io::Error),
}

impl From<std::io::Error> for SegmentWriterError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e)
    }
}

impl SegmentWriter {
    pub fn new(target_segment_size: u64, directory: std::path::PathBuf) -> Self {
        Self {
            cur_file: None,
            cur_file_bytes: 0,
            target_segment_size,
            directory,
        }
    }

    pub async fn write_batch(&mut self, buffer: &BatchBuffer) -> Result<(), SegmentWriterError> {
        if self.cur_file.is_none()
            || (self.cur_file_bytes > 0
                && self.cur_file_bytes + buffer.header.size_bytes as u64 > self.target_segment_size)
        {
            self.flush_close().await?;

            // Data rewritten always looks like term 1: because we will rewrite segment boundaries
            // and potentially compact data from different terms in the same segment, we do not
            // even try to preserve original terms.
            let term = 1;

            let off = buffer.header.base_offset;
            let segment_path = self.directory.join(format!("{}-{}-v1.log", off, term));
            info!("Open Segment: {:?}", segment_path);
            self.cur_file = Some(File::create(segment_path).await?);
        }

        let f: &mut File = self.cur_file.as_mut().unwrap();

        f.write_all(&buffer.bytes).await?;
        Ok(())
    }

    pub async fn flush_close(&mut self) -> Result<(), std::io::Error> {
        if let Some(mut f) = self.cur_file.take() {
            f.sync_all().await?;
            f.flush().await?;
        }
        Ok(())
    }
}
