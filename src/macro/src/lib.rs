use proc_macro::TokenStream;
use quote::quote;
use syn::ItemFn;

#[proc_macro_attribute]
pub fn test(_args: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = syn::parse_macro_input!(item as ItemFn);
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
