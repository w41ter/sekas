// Copyright 2023-present The Sekas Authors.
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

use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::ItemFn;

#[proc_macro_attribute]
pub fn test(_args: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = syn::parse_macro_input!(item as ItemFn);
    if input.sig.asyncness.is_none() {
        return syn::Error::new(input.sig.span(), "async fn is required").to_compile_error().into();
    }
    input.sig.asyncness = None;
    let body = input.block;
    input.block = syn::parse_quote! {
        {
            sekas_runtime::ExecutorOwner::new(1)
                .executor()
                .block_on(async move { #body });
        }
    };

    quote! {
        #[::core::prelude::v1::test]
        #input
    }
    .into()
}
