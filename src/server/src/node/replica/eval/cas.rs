// Copyright 2023 The Sekas Authors.
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
use sekas_api::v1::*;

use crate::{Error, Result};

pub(super) fn eval_conditions(
    value_result: Option<&[u8]>,
    conditions: &[WriteCondition],
) -> Result<()> {
    for cond in conditions {
        match WriteConditionType::from_i32(cond.r#type) {
            Some(WriteConditionType::Exists) if value_result.is_none() => {
                return Err(Error::CasFailed("user key not exists".into()));
            }
            Some(WriteConditionType::NotExists) if value_result.is_some() => {
                return Err(Error::CasFailed("user key already exists".into()));
            }
            Some(WriteConditionType::ExpectValue)
                if !value_result.map(|v| v == cond.value).unwrap_or_default() =>
            {
                return Err(Error::CasFailed("user key is not expected value".into()));
            }
            Some(WriteConditionType::ExpectVersion) => {}
            None => {
                return Err(Error::InvalidArgument(format!(
                    "Invalid WriteConditionType {}",
                    cond.r#type
                )))
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sekas_api::v1::{WriteCondition, WriteConditionType};

    use super::eval_conditions;
    use crate::Error;

    #[test]
    fn eval_not_exists() {
        let cond =
            WriteCondition { r#type: WriteConditionType::NotExists.into(), ..Default::default() };
        let value_result = Some(vec![b'1']);
        let r = eval_conditions(value_result.as_deref(), &[cond.clone()]);
        assert!(matches!(r, Err(Error::CasFailed(_))));

        let r = eval_conditions(None, &[cond]);
        assert!(r.is_ok());
    }

    #[test]
    fn eval_exists() {
        let cond =
            WriteCondition { r#type: WriteConditionType::Exists.into(), ..Default::default() };
        let value_result = Some(vec![b'1']);
        let r = eval_conditions(value_result.as_deref(), &[cond.clone()]);
        assert!(r.is_ok());

        let r = eval_conditions(None, &[cond]);
        assert!(matches!(r, Err(Error::CasFailed(_))));
    }

    #[test]
    fn eval_expected_value() {
        let cond = WriteCondition {
            r#type: WriteConditionType::ExpectValue.into(),
            value: vec![b'1'],
            ..Default::default()
        };

        let r = eval_conditions(None, &[cond.clone()]);
        assert!(matches!(r, Err(Error::CasFailed(_))));

        let r = eval_conditions(Some(&[]), &[cond.clone()]);
        assert!(matches!(r, Err(Error::CasFailed(_))));

        let r = eval_conditions(Some(&[b'1', b'1']), &[cond.clone()]);
        assert!(matches!(r, Err(Error::CasFailed(_))));

        let r = eval_conditions(Some(&[b'1']), &[cond]);
        assert!(r.is_ok());
    }
}
