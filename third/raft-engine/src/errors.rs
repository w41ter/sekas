// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::error;
use std::io::Error as IoError;

use thiserror::Error;

use crate::codec::Error as CodecError;

#[derive(Debug, Error)]
pub enum ProstError {
    #[error("Encode Error: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("Decode Error: {0}")]
    Decode(#[from] prost::DecodeError),
}

type ProtobufError = ProstError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),
    #[error("Corruption: {0}")]
    Corruption(String),
    #[error("IO Error: {0:?}")]
    Io(#[from] IoError),
    #[error("Codec Error: {0}")]
    Codec(#[from] CodecError),
    #[error("Protobuf Error: {0}")]
    Protobuf(#[from] ProtobufError),
    #[error("TryAgain Error: {0}")]
    TryAgain(String),
    #[error("Entry Compacted")]
    EntryCompacted,
    #[error("Entry Not Found")]
    EntryNotFound,
    #[error("Full")]
    Full,
    #[error("Other Error: {0}")]
    Other(#[from] Box<dyn error::Error + Send + Sync>),
}

pub type Result<T> = ::std::result::Result<T, Error>;

/// Check whether the given error is a nospace error.
pub(crate) fn is_no_space_err(e: &IoError) -> bool {
    // TODO: make the following judgement more elegant when the error type
    // `ErrorKind::StorageFull` is stable.
    format!("{e}").contains("nospace")
}

impl From<prost::EncodeError> for Error {
    fn from(error: prost::EncodeError) -> Self {
        ProstError::Encode(error).into()
    }
}

impl From<prost::DecodeError> for Error {
    fn from(error: prost::DecodeError) -> Self {
        ProstError::Decode(error).into()
    }
}
