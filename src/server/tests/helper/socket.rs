// Copyright 2022 The Engula Authors.
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

use std::net::SocketAddr;

use socket2::{Domain, Socket, Type};

/// Find the next available port to listen on.
pub fn next_avail_port() -> u16 {
    next_n_avail_port(1)[0]
}

pub fn next_n_avail_port(n: usize) -> Vec<u16> {
    #[allow(clippy::needless_collect)]
    let sockets = (0..n)
        .map(|_| {
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
            socket.set_reuse_address(true).unwrap();
            #[cfg(not(any(target_os = "solaris", target_os = "illumos", target_os = "windows")))]
            socket.set_reuse_port(true).unwrap();
            socket.bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into()).unwrap();
            socket
        })
        .collect::<Vec<_>>();
    sockets
        .into_iter()
        .map(|socket| socket.local_addr().unwrap().as_socket_ipv4().unwrap().port())
        .collect()
}
