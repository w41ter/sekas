// Copyright 2023-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::error::Error as StdError;

use prost::Message;
use sekas_api::server::v1::{GroupDesc, ReplicaDesc, RootDesc, Value};
use vaelo::{Status, StatusCode};

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AppResult<T> = std::result::Result<T, AppError>;

pub fn rpc_status(error: vaelo::Error) -> Status {
    error.remote_status().cloned().unwrap_or_else(|| Status::unavailable(error.to_string()))
}

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("{0} not found")]
    NotFound(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("invalid argument {0}")]
    InvalidArgument(String),
    #[error("deadline exceeded {0}")]
    DeadlineExceeded(String),
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("cas condition {1} not satisfied, operation index {0}")]
    CasFailed(u64, u64, Option<Value>),
    #[error("the txn is conflict with others")]
    TxnConflict,
    #[error("network: {0}")]
    Network(Status),
    #[error("internal {0}")]
    Internal(Box<dyn StdError + Send + Sync + 'static>),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid argument {0}")]
    InvalidArgument(String),
    #[error("deadline exceeded {0}")]
    DeadlineExceeded(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0} not found")]
    NotFound(String),
    #[error("{0} is exhausted")]
    ResourceExhausted(String),
    #[error("cas condition {1} not satisfied, operation index {0}")]
    CasFailed(u64, u64, Option<Value>),
    #[error("the txn is conflict with others")]
    TxnConflict,
    #[error("the local txn is not allowed")]
    LocalTxnNotAllowed,
    #[error("group epoch not match")]
    EpochNotMatch(GroupDesc),
    #[error("group {0} not found")]
    GroupNotFound(u64),
    #[error("not root leader")]
    NotRootLeader(RootDesc, u64, Option<ReplicaDesc>),
    #[error("not leader of group {0}")]
    NotLeader(u64, u64, Option<ReplicaDesc>),
    #[error("group {0} not accessable")]
    GroupNotAccessable(u64),
    #[error("transport {0}")]
    Transport(Status),
    #[error("connect {0}")]
    Connect(Status),
    #[error("rpc {0}")]
    Rpc(Status),
    #[error("internal {0}")]
    Internal(Box<dyn StdError + Send + Sync + 'static>),
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        match status.code() {
            StatusCode::Ok => panic!("invalid OK status"),
            StatusCode::InvalidArgument => Self::InvalidArgument(status.message().into()),
            StatusCode::Canceled if status.message().contains("Timeout expired") => {
                Self::DeadlineExceeded(status.message().into())
            }
            StatusCode::DeadlineExceeded => Self::DeadlineExceeded(status.message().into()),
            StatusCode::AlreadyExists => Self::AlreadyExists(status.message().into()),
            StatusCode::ResourceExhausted => Self::ResourceExhausted(status.message().into()),
            StatusCode::NotFound => Self::NotFound(status.message().into()),
            StatusCode::Internal => Self::Internal(status.message().into()),
            StatusCode::Unknown => from_source_or_details(status),
            StatusCode::Unavailable => from_source(status),
            _ => Self::Rpc(status),
        }
    }
}

impl From<sekas_api::server::v1::Error> for Error {
    fn from(err: sekas_api::server::v1::Error) -> Self {
        use sekas_api::server::v1::error_detail_union::Value;
        if err.details.is_empty() {
            return Status::internal("ErrorDetails is empty").into();
        }
        let detail = &err.details[0];
        let msg = detail.message.clone();
        match detail.detail.as_ref().and_then(|union| union.value.clone()) {
            Some(Value::GroupNotFound(value)) => Self::GroupNotFound(value.group_id),
            Some(Value::NotLeader(value)) => {
                Self::NotLeader(value.group_id, value.term, value.leader)
            }
            Some(Value::NotRoot(value)) => {
                Self::NotRootLeader(value.root.unwrap_or_default(), value.term, value.leader)
            }
            Some(Value::NotMatch(value)) => {
                Self::EpochNotMatch(value.descriptor.unwrap_or_default())
            }
            Some(Value::StatusCode(value)) => {
                Status::new(StatusCode::from_i32(value).unwrap_or(StatusCode::Unknown), msg).into()
            }
            Some(Value::CasFailed(value)) => {
                Self::CasFailed(value.index, value.cond_index, value.prev_value)
            }
            Some(Value::TxnConflict(_)) => Self::TxnConflict,
            Some(Value::LocalTxnNotAllowed(_)) => Self::LocalTxnNotAllowed,
            _ => Status::internal(format!("unknown error detail, msg: {msg}")).into(),
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::Internal(value.into())
    }
}

impl From<Error> for AppError {
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidArgument(value) => Self::InvalidArgument(value),
            Error::DeadlineExceeded(value) => Self::DeadlineExceeded(value),
            Error::ResourceExhausted(value) => Self::ResourceExhausted(value),
            Error::NotFound(value) => Self::NotFound(value),
            Error::AlreadyExists(value) => Self::AlreadyExists(value),
            Error::CasFailed(index, cond_index, prev_value) => {
                Self::CasFailed(index, cond_index, prev_value)
            }
            Error::TxnConflict => Self::TxnConflict,
            Error::Internal(value) => Self::Internal(value),
            Error::Transport(status) | Error::Connect(status) | Error::Rpc(status) => {
                Self::Network(status)
            }
            other => unreachable!("convert err {other:?} to AppError"),
        }
    }
}

impl From<AppError> for Status {
    fn from(err: AppError) -> Self {
        match err {
            AppError::NotFound(message) => Status::not_found(message),
            AppError::AlreadyExists(message) => Status::already_exists(message),
            AppError::InvalidArgument(message) => Status::invalid_argument(message),
            AppError::DeadlineExceeded(message) => Status::deadline_exceeded(message),
            AppError::ResourceExhausted(message) => Status::resource_exhausted(message),
            AppError::Network(status) => status,
            AppError::Internal(error) => Status::internal(error.to_string()),
            AppError::CasFailed(..) | AppError::TxnConflict => Status::internal(err.to_string()),
        }
    }
}

impl From<Error> for Status {
    fn from(value: Error) -> Self {
        AppError::from(value).into()
    }
}

pub fn retryable_rpc_err(status: &Status) -> bool {
    status.code() == StatusCode::Unavailable
        && (status.message().contains("Connection refused")
            || status.message().contains("connect timeout")
            || status.message().contains("gRPC connection is unavailable")
            || status.message().contains("gRPC connection closed"))
}

pub fn transport_err(status: &Status) -> bool {
    status.message().contains("connection reset") || status.message().contains("broken pipe")
}

pub fn from_source_or_details(status: Status) -> Error {
    if !status.details().is_empty()
        && let Ok(error) = sekas_api::server::v1::Error::decode(status.details())
    {
        return error.into();
    }
    from_source(status)
}

pub fn from_source(status: Status) -> Error {
    if retryable_rpc_err(&status) {
        Error::Connect(status)
    } else if transport_err(&status) {
        Error::Transport(status)
    } else {
        Error::Rpc(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_exhausted_is_an_application_error() {
        let error = AppError::from(Error::ResourceExhausted("limit".to_owned()));
        assert!(matches!(error, AppError::ResourceExhausted(message) if message == "limit"));
    }
}
