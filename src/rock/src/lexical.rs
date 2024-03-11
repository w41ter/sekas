// Copyright 2024 The Sekas Authors.
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

/// Returns the next boundary bytes in lexicographically sorted key space.
///
/// See [`tests::lexical_next_boundary_basic`] for details.
pub fn lexical_next_boundary(bytes: &[u8]) -> Vec<u8> {
    let mut r = bytes.to_owned();
    while let Some(&last) = r.last() {
        if last != 0xFF {
            break;
        }
        r.pop();
    }
    if let Some(last) = r.last_mut() {
        *last += 0x1;
    }
    r
}

/// Returns the next bytes in lexicographically sorted key space.
pub fn lexical_next(bytes: &[u8]) -> Vec<u8> {
    let mut r = bytes.to_owned();
    r.push(0x0);
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexical_next_boundary_basic() {
        struct TestCase {
            input: &'static [u8],
            expect: &'static [u8],
        }
        let cases = vec![
            TestCase { input: b"", expect: b"" },
            TestCase { input: b"1", expect: b"2" },
            TestCase { input: b"1\xFF", expect: b"2" },
            TestCase { input: b"1\xFF\xFF\xFF", expect: b"2" },
            TestCase { input: b"\xFF\xFF\xFF", expect: b"" },
            TestCase { input: b"123", expect: b"124" },
        ];
        for TestCase { input, expect } in cases {
            let got = lexical_next_boundary(input);
            assert_eq!(&got, expect);
        }
    }
}
