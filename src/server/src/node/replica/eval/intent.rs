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

#![allow(unused)]

/// The flag for put intent.
const PUT_INTENT: u8 = 0;

/// The flag for delete intent.
const DELETE_INTENT: u8 = 1;

/// The write intent of an txn.
#[derive(Debug, PartialEq, Eq)]
pub struct WriteIntent {
    /// The flag of write intent.
    flag: u8,
    /// The txn version.
    pub version: u64,
    /// The value to put, empty for delete.
    pub value: Vec<u8>,
}

impl WriteIntent {
    pub fn put(value: Vec<u8>, version: u64) -> Self {
        WriteIntent { flag: PUT_INTENT, version, value }
    }

    pub fn delete(version: u64) -> Self {
        WriteIntent { flag: DELETE_INTENT, version, value: vec![] }
    }

    /// Whether the intent is a put.
    pub fn is_put(&self) -> bool {
        self.flag == PUT_INTENT
    }

    /// Whether the intent is a delete.
    pub fn is_delete(&self) -> bool {
        self.flag == DELETE_INTENT
    }

    /// Encode write intent to bytes.
    pub fn encode(&self) -> Vec<u8> {
        if self.flag == DELETE_INTENT && !self.value.is_empty() {
            panic!("encode a delete intent but value isn't empty?");
        }

        let mut v = Vec::with_capacity(core::mem::size_of::<u64>() + self.value.len() + 1);
        v.push(self.flag);
        v.extend_from_slice(self.version.to_le_bytes().as_slice());
        v.extend_from_slice(self.value.as_slice());
        v
    }

    /// Try decode write intent from bytes.
    pub fn decode(value: &[u8]) -> Option<WriteIntent> {
        if value.len() < 1 + core::mem::size_of::<u64>() {
            None
        } else {
            let flag = value[0];
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&value[1..9]);
            let version = u64::from_le_bytes(buf);
            if flag != PUT_INTENT && flag != DELETE_INTENT {
                None
            } else {
                Some(WriteIntent { flag, version, value: value[9..].to_vec() })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_intent_encode_and_decode() {
        let version = 0x179394;

        {
            let intent = WriteIntent::put(b"hello".to_vec(), version);
            let encoded = intent.encode();
            let decoded = WriteIntent::decode(&encoded).unwrap();
            assert!(decoded.is_put());
            assert!(!decoded.is_delete());
            assert_eq!(decoded.version, version);
            assert_eq!(decoded.value, b"hello".to_vec());
        }

        {
            let intent = WriteIntent::delete(version);
            let encoded = intent.encode();
            let decoded = WriteIntent::decode(&encoded).unwrap();
            assert!(!decoded.is_put());
            assert!(decoded.is_delete());
            assert_eq!(decoded.version, version);
        }
    }
}
