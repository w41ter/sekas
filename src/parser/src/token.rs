// Copyright 2024-present The Sekas Authors.
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

use crate::{Coord, ParseError, ParseResult};

pub(crate) trait TokenRule<'a>
where
    Self: Sized,
{
    fn parse(tokenizer: &mut Tokenizer<'a>) -> ParseResult<Self>;

    #[allow(unused)]
    fn coord(&self) -> Coord;

    #[allow(unused)]
    fn content(&self) -> &[u8];
}

macro_rules! keyword {
    ($name:ident) => {
        paste::paste! {
            #[derive(Debug, Clone, Copy)]
            pub(crate) struct [<$name:camel>]<'a> {
                content: &'a [u8],
                coord: Coord,
            }

            impl<'a> TokenRule<'a> for [<$name:camel>]<'a> {
                fn parse(tokenizer: &mut Tokenizer<'a>) -> ParseResult<Self> {
                    if !tokenizer.input.starts_with(stringify!([<$name:lower>]).as_bytes()) &&
                        !tokenizer.input.starts_with(stringify!([<$name:upper>]).as_bytes()) {
                        let msg = format!("keyword {}", stringify!([<$name>]));
                        return Err(ParseError::Expect(msg, tokenizer.coord));
                    }
                    let (content, coord) = tokenizer.split_at(stringify!($name).len());
                    Ok(Self { content, coord })
                }

                #[inline]
                fn coord(&self) -> Coord { self.coord }

                #[inline]
                fn content(&self) -> &[u8] { self.content }
            }
        }
    };
}

keyword!(config);
keyword!(create);
keyword!(database);
keyword!(debug);
keyword!(delete);
keyword!(echo);
keyword!(exists);
keyword!(from);
keyword!(get);
keyword!(help);
keyword!(if);
keyword!(into);
keyword!(not);
keyword!(put);
keyword!(show);
keyword!(table);

macro_rules! symbol {
    ($name:ident, $value:literal) => {
        paste::paste! {
            pub(crate) struct [<$name:camel>] {
                coord: Coord,
            }

            impl<'a> TokenRule<'a> for [<$name:camel>] {
                fn parse(tokenizer: &mut Tokenizer<'a>) -> ParseResult<Self> {
                    if tokenizer.input.starts_with($value) {
                        let (_, coord) = tokenizer.split_at($value.len());
                        Ok(Self { coord })
                    } else {
                        Err(ParseError::Expect(format!("symbol {}", String::from_utf8_lossy($value)), tokenizer.coord))
                    }
                }

                #[inline]
                fn content(&self) -> &[u8] { $value }

                #[inline]
                fn coord(&self) -> Coord { self.coord }
            }
        }
    };
}

symbol!(Dot, b".");

pub(crate) struct Semicolon {
    coord: Coord,
}

impl<'a> TokenRule<'a> for Semicolon {
    fn parse(tokenizer: &mut Tokenizer<'a>) -> ParseResult<Self> {
        let coord = tokenizer.coord();
        if !tokenizer.input.starts_with(&[b';']) && !tokenizer.input.is_empty() {
            return Err(ParseError::Expect("symbol ;".to_owned(), coord));
        }

        if !tokenizer.input.is_empty() {
            tokenizer.split_at(1);
        }
        Ok(Semicolon { coord })
    }

    #[inline]
    fn coord(&self) -> Coord {
        self.coord
    }

    #[inline]
    fn content(&self) -> &[u8] {
        b";"
    }
}

// An ident must be utf8 encoded.
#[derive(Debug, Clone)]
pub(crate) struct Ident<'a> {
    content: &'a [u8],
    coord: Coord,
}

impl<'a> Ident<'a> {
    #[inline]
    pub fn value(&self) -> &str {
        std::str::from_utf8(self.content).expect("an ident must be utf8 encoded, see Ident::parse")
    }
}

impl<'a> TokenRule<'a> for Ident<'a> {
    fn parse(tokenizer: &mut Tokenizer<'a>) -> ParseResult<Self> {
        let (content, coord) = tokenizer
            .split_when(|c| !matches!(c, b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'-' | b'0'..=b'9'));
        if content.is_empty() {
            return Err(ParseError::Expect("ident [a-zA-Z0-9_]*".to_owned(), coord));
        }
        if std::str::from_utf8(content).is_err() {
            return Err(ParseError::InvalidEncoding(coord));
        }
        Ok(Self { content, coord })
    }

    #[inline]
    fn coord(&self) -> Coord {
        self.coord
    }

    #[inline]
    fn content(&self) -> &[u8] {
        self.content
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Literal<'a> {
    value: Vec<u8>,
    content: &'a [u8],
    coord: Coord,
}

impl<'a> Literal<'a> {
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl<'a> TokenRule<'a> for Literal<'a> {
    fn parse(tokenizer: &mut Tokenizer<'a>) -> ParseResult<Self> {
        if tokenizer.input.starts_with(&[b'"']) {
            let mut value = vec![];
            let len = tokenizer.input.len();
            let mut i = 1;
            let mut closed = false;
            while i < len {
                match tokenizer.input[i] {
                    b'\\' => {
                        if i + 1 == len {
                            return Err(ParseError::UnexpectedEOS("escape value".to_owned()));
                        }
                        match tokenizer.input[i + 1] {
                            b'n' => value.push(b'\n'),
                            b't' => value.push(b'\t'),
                            b'r' => value.push(b'\r'),
                            b' ' => value.push(b' '),
                            b'\\' => value.push(b'\\'),
                            c => {
                                return Err(ParseError::Unknown(
                                    format!("escape value: {}", c),
                                    tokenizer.coord(),
                                ));
                            }
                        }
                        i += 2;
                    }
                    b'"' => {
                        i += 1;
                        closed = true;
                        break;
                    }
                    c => {
                        value.push(c);
                        i += 1;
                    }
                }
            }
            if !closed {
                Err(ParseError::Expect("close \"".to_owned(), tokenizer.coord))
            } else {
                let (content, coord) = tokenizer.split_at(i);
                Ok(Self { value, content, coord })
            }
        } else {
            let (content, coord) =
                tokenizer.split_when(|c| matches!(c, b' ' | b'\t' | b'\r' | b'\n'));
            if content.is_empty() {
                Err(ParseError::Expect("literal".to_owned(), coord))
            } else {
                Ok(Literal { value: Vec::from(content), content, coord })
            }
        }
    }

    fn content(&self) -> &[u8] {
        self.content
    }

    fn coord(&self) -> Coord {
        self.coord
    }
}

#[macro_export]
macro_rules! Token {
    // keywords
    [config] =>         { $crate::token::Config };
    [create] =>         { $crate::token::Create };
    [database] =>       { $crate::token::Database };
    [debug] =>          { $crate::token::Debug };
    [delete] =>         { $crate::token::Delete };
    [echo] =>           { $crate::token::Echo };
    [exists] =>         { $crate::token::Exists };
    [from] =>           { $crate::token::From };
    [get] =>            { $crate::token::Get };
    [help] =>           { $crate::token::Help };
    [if] =>             { $crate::token::If };
    [into] =>           { $crate::token::Into };
    [not] =>            { $crate::token::Not };
    [put] =>            { $crate::token::Put };
    [table] =>          { $crate::token::Table };
    [show] =>           { $crate::token::Show };

    // symbols
    [.] =>              { $crate::token::Dot };
    [;] =>              { $crate::token::Semicolon }; // ; or END

    [ident] =>          { $crate::token::Ident };
    [literal] =>        { $crate::token::Literal };
}

#[derive(Debug, Clone)]
pub struct Tokenizer<'a> {
    input: &'a [u8],
    coord: Coord,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Tokenizer { input: input.as_bytes(), coord: Coord { row: 1, col: 1, pos: 0 } }
    }

    #[inline]
    pub fn coord(&self) -> Coord {
        self.coord
    }

    pub fn peek<T: TokenRule<'a>>(&mut self) -> bool {
        self.clone().next::<T>().is_ok()
    }

    pub fn next<T: TokenRule<'a>>(&mut self) -> ParseResult<T> {
        self.skip_whitespace();
        T::parse(self)
    }

    pub fn has_more(&mut self) -> bool {
        self.skip_whitespace();
        !self.input.is_empty()
    }

    fn split_at(&mut self, pos: usize) -> (&'a [u8], Coord) {
        let coord = self.coord;
        self.coord.row += pos;
        self.coord.pos += pos;
        let (left, right) = self.input.split_at(pos);
        self.input = right;
        (left, coord)
    }

    fn split_when<P>(&mut self, pred: P) -> (&'a [u8], Coord)
    where
        P: Fn(u8) -> bool,
    {
        let n = self.input.len();
        let mut i = 0usize;
        while i < n && !pred(self.input[i]) {
            i += 1;
        }
        let coord = self.coord;
        let content = &self.input[..i];
        self.input = &self.input[i..];
        self.coord.col += i;
        self.coord.pos += i;
        (content, coord)
    }

    fn skip_whitespace(&mut self) {
        let n = self.input.len();
        let mut i = 0usize;
        while i < n {
            match self.input[i] {
                b'\n' => {
                    self.coord.row += 1;
                    self.coord.col = 1;
                }
                b' ' | b'\t' | b'\r' => {
                    self.coord.col += 1;
                }
                _ => {
                    break;
                }
            }
            i += 1;
        }
        self.input = &self.input[i..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_whitespace() {
        let input = "    ";
        let mut tok = Tokenizer::new(input);
        tok.skip_whitespace();
        assert!(tok.input.is_empty());
    }

    #[test]
    fn parse_semicolon_or_end() {
        {
            let input = "   ;";
            let mut tok = Tokenizer::new(input);
            assert!(tok.next::<Token![;]>().is_ok());
        }

        {
            let input = "   ";
            let mut tok = Tokenizer::new(input);
            assert!(tok.next::<Token![;]>().is_ok());
        }
    }

    #[test]
    fn parse_literal() {
        {
            let input = r#""literal with quotation""#;
            let mut tokenizer = Tokenizer::new(input);
            let tok = tokenizer.next::<Token![literal]>();
            assert!(tok.is_ok());
            assert_eq!(tok.unwrap().value(), b"literal with quotation");
        }

        {
            let input = r#""literal with missing quotation"#;
            let mut tokenizer = Tokenizer::new(input);
            let tok = tokenizer.next::<Token![literal]>();
            assert!(tok.is_err());
        }

        {
            let input = r#"literal-without-quotation"#;
            let mut tokenizer = Tokenizer::new(input);
            let tok = tokenizer.next::<Token![literal]>();
            assert!(tok.is_ok());
            assert_eq!(tok.unwrap().value(), b"literal-without-quotation");
        }

        {
            // espacing
            let input = r#""\n\t\r\ \\""#;
            let mut tokenizer = Tokenizer::new(input);
            let tok = tokenizer.next::<Token![literal]>();
            assert!(tok.is_ok());
            assert_eq!(tok.unwrap().value(), b"\n\t\r \\");
        }
    }
}
