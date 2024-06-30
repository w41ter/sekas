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

use crate::ast::*;
use crate::token::{TokenRule, Tokenizer};
use crate::{ParseError, ParseResult, Token};

#[derive(Debug)]
struct Parser<'a> {
    tokenizer: Tokenizer<'a>,
}

impl<'a> Parser<'a> {
    #[inline]
    fn next<T: TokenRule<'a>>(&mut self) -> ParseResult<T> {
        self.tokenizer.next::<T>()
    }

    #[inline]
    fn peek<T: TokenRule<'a>>(&mut self) -> bool {
        self.tokenizer.peek::<T>()
    }

    fn parse(&mut self) -> ParseResult<Option<Statement>> {
        if !self.tokenizer.has_more() {
            return Ok(None);
        }

        let stmt = if self.peek::<Token![echo]>() {
            parse_echo_statement(self)?
        } else if self.peek::<Token![config]>() {
            parse_config_stmt(self)?
        } else if self.peek::<Token![create]>() {
            parse_create_stmt(self)?
        } else if self.peek::<Token![get]>() {
            parse_get_stmt(self)?
        } else if self.peek::<Token![put]>() {
            parse_put_stmt(self)?
        } else if self.peek::<Token![delete]>() {
            parse_delete_stmt(self)?
        } else if self.peek::<Token![show]>() {
            parse_show_stmt(self)?
        } else if self.peek::<Token![help]>() {
            parse_help_stmt(self)?
        } else if self.peek::<Token![debug]>() {
            parse_debug_stmt(self)?
        } else {
            return Err(ParseError::Unexpected(self.tokenizer.coord()));
        };
        Ok(Some(stmt))
    }
}

pub fn parse(input: &str) -> ParseResult<Option<Statement>> {
    let tokenizer = Tokenizer::new(input);
    let mut input = Parser { tokenizer };
    input.parse()
}

// Syntax:
// ECHO <message:literal>
fn parse_echo_statement(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![echo]>()?;
    let msg = parser.next::<Token![literal]>()?;
    parser.next::<Token![;]>()?;
    Ok(Statement::Echo(EchoStatement { message: String::from_utf8_lossy(msg.value()).to_string() }))
}

// Syntax:
// CONFIG <name:literal> <value:literal>
fn parse_config_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![config]>()?;
    let key = parser.next::<Token![literal]>()?;
    let value = parser.next::<Token![literal]>()?;
    parser.next::<Token![;]>()?;
    Ok(Statement::Config(ConfigStatement {
        key: key.value().to_owned().into(),
        value: value.value().to_owned().into(),
    }))
}

// Syntax:
// CREATE DATABASE [IF NOT EXISTS] <db name:ident>
// CREATE TABLE [IF NOT EXISTS] <db name:ident> . <table name:ident>
fn parse_create_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![create]>()?;
    if parser.peek::<Token![database]>() {
        // create database
        parser.next::<Token![database]>()?;
        let create_if_not_exists = try_parse_if_not_exists(parser)?;
        let db_name = parser.next::<Token![ident]>()?;
        parser.next::<Token![;]>()?;
        Ok(Statement::CreateDb(CreateDbStatement {
            db_name: db_name.value().to_owned(),
            create_if_not_exists,
        }))
    } else if parser.peek::<Token![table]>() {
        // create table
        parser.next::<Token![table]>()?;
        let create_if_not_exists = try_parse_if_not_exists(parser)?;
        let db_name = parser.next::<Token![ident]>()?.value().to_owned();
        parser.next::<Token![.]>()?;
        let table_name = parser.next::<Token![ident]>()?.value().to_owned();
        parser.next::<Token![;]>()?;
        Ok(Statement::CreateTable(CreateTableStatement {
            db_name,
            table_name,
            create_if_not_exists,
        }))
    } else {
        Err(ParseError::UnexpectedToken("database or table".to_owned(), parser.tokenizer.coord()))
    }
}

// Syntax:
// GET <key:literal> FROM <db_name:ident>.<table_name:ident>
fn parse_get_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![get]>()?;
    let key = parser.next::<Token![literal]>()?.value().to_owned();
    parser.next::<Token![from]>()?;
    let db_name = parser.next::<Token![ident]>()?.value().to_owned();
    parser.next::<Token![.]>()?;
    let table_name = parser.next::<Token![ident]>()?.value().to_owned();
    Ok(Statement::Get(GetStatement { key, db_name, table_name }))
}

// Syntax:
// PUT <key:literal> <value:literal> INTO <db_name:ident>.<table_name:ident>
fn parse_put_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![put]>()?;
    let key = parser.next::<Token![literal]>()?.value().to_owned();
    let value = parser.next::<Token![literal]>()?.value().to_owned();
    parser.next::<Token![into]>()?;
    let db_name = parser.next::<Token![ident]>()?.value().to_owned();
    parser.next::<Token![.]>()?;
    let table_name = parser.next::<Token![ident]>()?.value().to_owned();
    Ok(Statement::Put(PutStatement { key, value, db_name, table_name }))
}

// Syntax:
// DELETE <key:literal> FROM <db_name:ident>.<table_name:ident>
fn parse_delete_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![delete]>()?;
    let key = parser.next::<Token![literal]>()?.value().to_owned();
    parser.next::<Token![from]>()?;
    let db_name = parser.next::<Token![ident]>()?.value().to_owned();
    parser.next::<Token![.]>()?;
    let table_name = parser.next::<Token![ident]>()?.value().to_owned();
    Ok(Statement::Delete(DeleteStatement { key, db_name, table_name }))
}

// Syntax:
// SHOW <property:ident> [FROM <name:ident>]
fn parse_show_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![show]>()?;
    let ident = parser.next::<Token![ident]>()?;
    let from = if parser.peek::<Token![from]>() {
        parser.next::<Token![from]>()?;
        let name = parser.next::<Token![ident]>()?;
        Some(name.value().to_owned())
    } else {
        None
    };
    parser.next::<Token![;]>()?;
    Ok(Statement::Show(ShowStatement { property: ident.value().to_owned(), from }))
}

// Syntax:
// HELP <topic:ident>
fn parse_help_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![help]>()?;
    let topic = if parser.peek::<Token![ident]>() {
        Some(parser.next::<Token![ident]>()?.value().to_owned())
    } else {
        None
    };
    parser.next::<Token![;]>()?;
    Ok(Statement::Help(HelpStatement { topic }))
}

// Syntax:
// DEBUG <statement>
fn parse_debug_stmt(parser: &mut Parser) -> ParseResult<Statement> {
    parser.next::<Token![debug]>()?;

    let Some(stmt) = parser.parse()? else {
        return Err(ParseError::UnexpectedEOS("statement".to_owned()));
    };
    Ok(Statement::Debug(DebugStatement { stmt: Box::new(stmt) }))
}

fn try_parse_if_not_exists(parser: &mut Parser) -> ParseResult<bool> {
    if parser.peek::<Token![if]>() {
        parser.next::<Token![if]>()?;
        parser.next::<Token![not]>()?;
        parser.next::<Token![exists]>()?;
        Ok(true)
    } else {
        Ok(false)
    }
}
