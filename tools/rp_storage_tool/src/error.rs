#[derive(Debug)]
pub enum BucketReaderError {
    ReadError(object_store::Error),
    StreamReadError(std::io::Error),
    ParseError(serde_json::Error),
    SyntaxError(String),
}

impl From<object_store::Error> for BucketReaderError {
    fn from(e: object_store::Error) -> Self {
        BucketReaderError::ReadError(e)
    }
}

impl From<serde_json::Error> for BucketReaderError {
    fn from(e: serde_json::Error) -> Self {
        BucketReaderError::ParseError(e)
    }
}

impl From<std::io::Error> for BucketReaderError {
    fn from(e: std::io::Error) -> Self {
        BucketReaderError::StreamReadError(e)
    }
}
