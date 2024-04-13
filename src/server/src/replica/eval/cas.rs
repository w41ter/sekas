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

use log::debug;
use sekas_api::server::v1::*;

use crate::{Error, Result};

/// Eval conditions, return the first unsatisfied values.
pub(super) fn eval_conditions(
    value: Option<&Value>,
    conditions: &[WriteCondition],
) -> Result<Option<usize>> {
    for (idx, cond) in conditions.iter().enumerate() {
        if !eval_condition(cond, value)? {
            return Ok(Some(idx));
        }
    }
    Ok(None)
}

fn eval_condition(cond: &WriteCondition, value: Option<&Value>) -> Result<bool> {
    let Some(r#type) = WriteConditionType::from_i32(cond.r#type) else {
        return Err(Error::InvalidArgument(format!("Invalid WriteConditionType {}", cond.r#type)));
    };
    if let Some(value) = value {
        debug!("eval condition {:?} with value {value:?}", r#type);
        match r#type {
            WriteConditionType::ExpectNotExists if value.content.is_some() => Ok(false),
            WriteConditionType::ExpectExists if value.content.is_none() => Ok(false),
            WriteConditionType::ExpectValue => {
                Ok(value.content.as_ref().map(|v| v == &cond.value).unwrap_or_default())
            }
            WriteConditionType::ExpectVersion if value.version != cond.version => Ok(false),
            WriteConditionType::ExpectVersionLt if value.version >= cond.version => Ok(false),
            WriteConditionType::ExpectVersionLe if value.version > cond.version => Ok(false),
            WriteConditionType::ExpectVersionGt if value.version <= cond.version => Ok(false),
            WriteConditionType::ExpectVersionGe if value.version < cond.version => Ok(false),
            WriteConditionType::ExpectStartsWith => {
                Ok(value.content.as_ref().map(|v| v.starts_with(&cond.value)).unwrap_or_default())
            }
            WriteConditionType::ExpectEndsWith => {
                Ok(value.content.as_ref().map(|v| v.ends_with(&cond.value)).unwrap_or_default())
            }
            WriteConditionType::ExpectSlice => {
                let idx = cond.begin as usize;
                Ok(value
                    .content
                    .as_ref()
                    .filter(|v| idx < v.len())
                    .map(|v| v[idx..].starts_with(&cond.value))
                    .unwrap_or_default())
            }
            _ => Ok(true),
        }
    } else {
        match r#type {
            WriteConditionType::ExpectNotExists => Ok(true),
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_not_exists() {
        let cond = WriteCondition {
            r#type: WriteConditionType::ExpectNotExists.into(),
            ..Default::default()
        };
        let value_result = Some(Value::with_value(vec![b'1'], 0));
        let r = eval_condition(&cond, value_result.as_ref());
        assert!(matches!(r, Ok(false)));

        let value_result = Some(Value::tombstone(1));
        let r = eval_condition(&cond, value_result.as_ref());
        assert!(matches!(r, Ok(true)));

        let r = eval_condition(&cond, None);
        assert!(matches!(r, Ok(true)));
    }

    #[test]
    fn eval_exists() {
        let cond = WriteCondition {
            r#type: WriteConditionType::ExpectExists.into(),
            ..Default::default()
        };
        let value_result = Some(Value::with_value(vec![b'1'], 0));
        let r = eval_condition(&cond, value_result.as_ref());
        assert!(matches!(r, Ok(true)));

        let value_result = Some(Value::tombstone(1));
        let r = eval_condition(&cond, value_result.as_ref());
        assert!(matches!(r, Ok(false)));

        let r = eval_condition(&cond, None);
        assert!(matches!(r, Ok(false)));
    }

    #[test]
    fn eval_expected_value() {
        struct TestCase {
            value: Option<Value>,
            expect: bool,
        }

        let cond = WriteCondition {
            r#type: WriteConditionType::ExpectValue.into(),
            value: vec![b'1'],
            ..Default::default()
        };

        let cases = vec![
            TestCase { value: None, expect: false },
            TestCase { value: Some(Value::tombstone(0)), expect: false },
            TestCase { value: Some(Value::with_value(vec![], 0)), expect: false },
            TestCase { value: Some(Value::with_value(vec![b'1', b'1'], 0)), expect: false },
            TestCase { value: Some(Value::with_value(vec![b'1'], 0)), expect: true },
        ];
        for TestCase { value, expect } in cases {
            let r = eval_condition(&cond, value.as_ref()).unwrap();
            assert_eq!(r, expect, "value: {value:?}");
        }
    }

    #[test]
    fn eval_expect_version() {
        struct TestCase {
            r#type: WriteConditionType,
            value: Option<Value>,
            expect: bool,
        }

        let expect_version = 10;
        let cases = vec![
            // For expect version
            TestCase { r#type: WriteConditionType::ExpectVersion, value: None, expect: false },
            TestCase {
                r#type: WriteConditionType::ExpectVersion,
                value: Some(Value::tombstone(1)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersion,
                value: Some(Value::tombstone(10)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersion,
                value: Some(Value::with_value(vec![], 1)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersion,
                value: Some(Value::with_value(vec![], 10)),
                expect: true,
            },
            // For expect version less than.
            TestCase { r#type: WriteConditionType::ExpectVersionLt, value: None, expect: false },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLt,
                value: Some(Value::tombstone(1)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLt,
                value: Some(Value::tombstone(10)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLt,
                value: Some(Value::with_value(vec![], 1)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLt,
                value: Some(Value::with_value(vec![], 10)),
                expect: false,
            },
            // For expect version less than or equals to.
            TestCase { r#type: WriteConditionType::ExpectVersionLe, value: None, expect: false },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLe,
                value: Some(Value::tombstone(1)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLe,
                value: Some(Value::tombstone(10)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLe,
                value: Some(Value::tombstone(11)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLe,
                value: Some(Value::with_value(vec![], 1)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLe,
                value: Some(Value::with_value(vec![], 10)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionLe,
                value: Some(Value::with_value(vec![], 11)),
                expect: false,
            },
            // For expect version great than.
            TestCase { r#type: WriteConditionType::ExpectVersionGt, value: None, expect: false },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGt,
                value: Some(Value::tombstone(1)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGt,
                value: Some(Value::tombstone(11)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGt,
                value: Some(Value::with_value(vec![], 1)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGt,
                value: Some(Value::with_value(vec![], 11)),
                expect: true,
            },
            // For expect version great than or equals to.
            TestCase { r#type: WriteConditionType::ExpectVersionGe, value: None, expect: false },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGe,
                value: Some(Value::tombstone(1)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGe,
                value: Some(Value::tombstone(10)),
                expect: true,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGe,
                value: Some(Value::with_value(vec![], 1)),
                expect: false,
            },
            TestCase {
                r#type: WriteConditionType::ExpectVersionGe,
                value: Some(Value::with_value(vec![], 10)),
                expect: true,
            },
        ];
        for TestCase { r#type, value, expect } in cases {
            let cond = WriteCondition {
                r#type: r#type.into(),
                version: expect_version,
                ..Default::default()
            };
            let r = eval_condition(&cond, value.as_ref()).unwrap();
            assert_eq!(r, expect, "type: {} value: {value:?}", r#type.as_str_name());
        }
    }

    #[test]
    fn expect_starts_with() {
        struct TestCase {
            value: Option<Value>,
            expect: bool,
        }

        let prefix = b"123".to_vec();
        let cases = vec![
            TestCase { value: None, expect: false },
            TestCase { value: Some(Value::tombstone(10)), expect: false },
            TestCase { value: Some(Value::with_value(b"12".to_vec(), 10)), expect: false },
            TestCase { value: Some(Value::with_value(b"123".to_vec(), 10)), expect: true },
            TestCase { value: Some(Value::with_value(b"1234".to_vec(), 10)), expect: true },
            TestCase { value: Some(Value::with_value(b"0123".to_vec(), 10)), expect: false },
        ];
        let cond = WriteCondition {
            r#type: WriteConditionType::ExpectStartsWith.into(),
            value: prefix,
            ..Default::default()
        };
        for TestCase { value, expect } in cases {
            let r = eval_condition(&cond, value.as_ref()).unwrap();
            assert_eq!(r, expect);
        }
    }

    #[test]
    fn expect_ends_with() {
        struct TestCase {
            value: Option<Value>,
            expect: bool,
        }

        let suffix = b"123".to_vec();
        let cases = vec![
            TestCase { value: None, expect: false },
            TestCase { value: Some(Value::tombstone(10)), expect: false },
            TestCase { value: Some(Value::with_value(b"23".to_vec(), 10)), expect: false },
            TestCase { value: Some(Value::with_value(b"123".to_vec(), 10)), expect: true },
            TestCase { value: Some(Value::with_value(b"0123".to_vec(), 10)), expect: true },
            TestCase { value: Some(Value::with_value(b"1234".to_vec(), 10)), expect: false },
        ];
        let cond = WriteCondition {
            r#type: WriteConditionType::ExpectEndsWith.into(),
            value: suffix,
            ..Default::default()
        };
        for TestCase { value, expect } in cases {
            let r = eval_condition(&cond, value.as_ref()).unwrap();
            assert_eq!(r, expect);
        }
    }

    #[test]
    fn expect_slice() {
        struct TestCase {
            value: Option<Value>,
            start: u64,
            expect: bool,
        }

        let suffix = b"123".to_vec();
        let cases = vec![
            TestCase { value: None, start: 0, expect: false },
            TestCase { value: Some(Value::tombstone(10)), start: 0, expect: false },
            TestCase {
                value: Some(Value::with_value(b"12".to_vec(), 10)),
                start: 0,
                expect: false,
            },
            TestCase {
                value: Some(Value::with_value(b"23".to_vec(), 10)),
                start: 0,
                expect: false,
            },
            TestCase {
                value: Some(Value::with_value(b"123".to_vec(), 10)),
                start: 0,
                expect: true,
            },
            TestCase {
                value: Some(Value::with_value(b"123".to_vec(), 10)),
                start: 1,
                expect: false,
            },
            TestCase {
                value: Some(Value::with_value(b"123".to_vec(), 10)),
                start: 4,
                expect: false,
            },
            TestCase {
                value: Some(Value::with_value(b"0123".to_vec(), 10)),
                start: 0,
                expect: false,
            },
            TestCase {
                value: Some(Value::with_value(b"0123".to_vec(), 10)),
                start: 1,
                expect: true,
            },
            TestCase {
                value: Some(Value::with_value(b"1234".to_vec(), 10)),
                start: 0,
                expect: true,
            },
            TestCase {
                value: Some(Value::with_value(b"1234".to_vec(), 10)),
                start: 1,
                expect: false,
            },
            TestCase {
                value: Some(Value::with_value(b"123123".to_vec(), 10)),
                start: 1,
                expect: false,
            },
            TestCase {
                value: Some(Value::with_value(b"123123".to_vec(), 10)),
                start: 3,
                expect: true,
            },
        ];
        for TestCase { value, start, expect } in cases {
            let cond = WriteCondition {
                r#type: WriteConditionType::ExpectSlice.into(),
                value: suffix.clone(),
                begin: start,
                ..Default::default()
            };
            let r = eval_condition(&cond, value.as_ref()).unwrap();
            assert_eq!(r, expect);
        }
    }

    #[test]
    fn eval_failed_return_first_index() {
        let value = Some(Value::with_value(b"123".to_vec(), 10));
        let conds = vec![
            WriteCondition {
                r#type: WriteConditionType::ExpectVersion.into(),
                version: 10,
                ..Default::default()
            },
            WriteCondition {
                r#type: WriteConditionType::ExpectValue.into(),
                value: b"124".to_vec(),
                ..Default::default()
            },
        ];
        let r = eval_conditions(value.as_ref(), &conds);
        assert!(matches!(r, Ok(Some(1))));

        let value = Some(Value::with_value(b"124".to_vec(), 10));
        let r = eval_conditions(value.as_ref(), &conds);
        assert!(matches!(r, Ok(None)));
    }
}
