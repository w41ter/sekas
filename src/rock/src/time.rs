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

#[inline]
pub fn timestamp_nanos() -> u64 {
    #[cfg(target_os = "linux")]
    {
        use std::mem::MaybeUninit;

        let mut t = MaybeUninit::uninit();
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, t.as_mut_ptr()) };
        let now: libc::timespec = unsafe { t.assume_init() };
        (now.tv_sec * 1000 * 1000 * 1000 + now.tv_nsec) as u64
    }

    #[cfg(not(target_os = "linux"))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};

        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
    }
}

#[inline]
pub fn timestamp_micros() -> u64 {
    timestamp_nanos() / 1000
}

#[inline]
pub fn timestamp_millis() -> u64 {
    timestamp_micros() / 1000
}

#[inline]
pub fn timestamp() -> u64 {
    timestamp_millis() / 1000
}
