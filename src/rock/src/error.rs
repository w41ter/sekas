// Copyright 2024-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;
use std::fmt;
use std::panic::Location;

/// A virtual stack frame with a message.
///
/// Its location is constructed via the [`track_caller`] macro.
///
/// - [`track_caller`]: https://doc.rust-lang.org/reference/attributes/codegen.html#the-track_caller-attribute
struct StackFrame {
    /// The message attached to this frame.
    message: &'static str,
    /// The location of this frame.
    location: Location<'static>,
}

/// An implementation of [`Error`] wrapped a generic [`Error`] and would attach
/// a set of virtual stack frames (containing a static message and a caller
/// location).
///
/// - [`Error`]: std::error::Error
pub struct ContextError<E: Error + 'static> {
    /// The frames attached to this error.
    frames: Vec<StackFrame>,
    /// The source error.
    pub source: E,
}

/// Construct [`ContextError`] from a generic [`Error`].
///
/// - [`Error`]: std::error::Error
impl<E: Error> From<E> for ContextError<E> {
    #[inline]
    fn from(source: E) -> Self {
        ContextError { frames: Vec::default(), source }
    }
}

/// A trait to attach a message as a context to a [`ContextError`].
pub trait Context<T, E>
where
    E: Error,
{
    /// Attach a static message to a [`ContextError`].
    #[track_caller]
    fn context(self, message: &'static str) -> Result<T, ContextError<E>>;
}

impl<T, E> Context<T, E> for Result<T, E>
where
    E: Error,
{
    #[inline]
    #[track_caller]
    fn context(self, message: &'static str) -> Result<T, ContextError<E>> {
        match self {
            Ok(val) => Ok(val),
            Err(source) => {
                let location = Location::caller().clone();
                let context = StackFrame { message, location };
                Err(ContextError { frames: vec![context], source })
            }
        }
    }
}

impl<T, E> Context<T, E> for Result<T, ContextError<E>>
where
    E: Error,
{
    #[inline]
    #[track_caller]
    fn context(self, message: &'static str) -> Result<T, ContextError<E>> {
        match self {
            Ok(val) => Ok(val),
            Err(mut ctx) => {
                let location = Location::caller().clone();
                ctx.frames.push(StackFrame { message, location });
                Err(ctx)
            }
        }
    }
}

impl<E: Error> fmt::Debug for ContextError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\n", self.source)?;
        for (index, ctx) in self.frames.iter().enumerate() {
            write!(f, "{}: {}, at {}\n", index, ctx.message, ctx.location)?;
        }
        Ok(())
    }
}

impl<E: Error> fmt::Display for ContextError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for frame in self.frames.iter().rev() {
            write!(f, "{}: ", frame.message)?;
        }
        write!(f, "{}", self.source)
    }
}

impl<E: Error> Error for ContextError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(test)]
mod tests {
    use thiserror::Error;

    use super::*;

    type Result<T, E> = std::result::Result<T, ContextError<E>>;

    #[derive(Debug, Error)]
    enum MyError {
        #[error("invalid argument: {0}")]
        InvalidArgument(String),
    }

    fn do_some_thing() -> Result<(), MyError> {
        Err(MyError::InvalidArgument("name is illegal".into()).into())
    }

    fn foo() -> Result<(), MyError> {
        do_some_thing().context("do some thing")
    }

    fn bar() -> Result<(), MyError> {
        foo().context("invoke foo")
    }

    #[test]
    fn test_context_error_debug_and_display() {
        let err = bar().err().unwrap();
        println!("err with display: {}", err);
        println!("err with debug: {:?}", err);
    }
}
