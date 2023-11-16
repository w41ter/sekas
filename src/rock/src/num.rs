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
use paste::paste;

macro_rules! decode {
    ($num_type:ty) => {
        paste! {
            pub fn [<decode_ $num_type>](bytes: &[u8]) -> Option<$num_type> {
                if bytes.len() != core::mem::size_of::<$num_type>() {
                    return None;
                }

                let mut buf = [0u8; core::mem::size_of::<$num_type>()];
                buf.copy_from_slice(bytes);
                Some($num_type::from_be_bytes(buf))
            }
        }
    };
}

decode!(i16);
decode!(u16);
decode!(i32);
decode!(u32);
decode!(u64);
decode!(i64);
