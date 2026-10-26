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

use std::collections::HashMap;
use std::sync::Arc;

use vaelo::http;

#[crate::async_trait]
pub(super) trait HttpHandle: Send + Sync {
    async fn call(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> crate::Result<http::Response<String>>;
}

pub(super) struct Router {
    handles: HashMap<String, Box<dyn HttpHandle>>,
}

pub struct AdminService
where
    Self: Send + Sync,
{
    inner: Arc<Router>,
}

impl AdminService {
    pub(super) fn new(inner: Router) -> Self {
        AdminService { inner: Arc::new(inner) }
    }

    pub async fn call_path(
        &self,
        path: &str,
        params: HashMap<String, String>,
    ) -> http::Response<String> {
        self.inner.call_text(path, params).await
    }
}

impl Clone for AdminService {
    fn clone(&self) -> Self {
        AdminService { inner: self.inner.clone() }
    }
}

impl Router {
    pub fn empty() -> Self {
        Router { handles: HashMap::default() }
    }

    pub fn nest(path: &str, r: Router) -> Self {
        if path.is_empty() || !path.starts_with('/') {
            panic!("Paths must start with a `/`");
        }

        let handles =
            r.handles.into_iter().map(|(url, handle)| (format!("{path}{url}"), handle)).collect();
        Router { handles }
    }

    pub fn route(mut self, path: &str, handle: impl HttpHandle + 'static) -> Self {
        if path.is_empty() || !path.starts_with('/') {
            panic!("Paths must start with a `/`");
        }

        self.handles.insert(path.to_owned(), Box::new(handle));

        self
    }

    async fn call_text(
        &self,
        path: &str,
        params: HashMap<String, String>,
    ) -> http::Response<String> {
        let handle = match self.handles.get(path) {
            Some(handle) => handle,
            None => {
                return http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(String::new())
                    .unwrap();
            }
        };

        match handle.call(path, &params).await {
            Ok(resp) => resp,
            Err(e) => http::Response::builder()
                .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
                .unwrap(),
        }
    }
}
