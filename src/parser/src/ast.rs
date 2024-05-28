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

use crate::ExecuteResult;

#[derive(Debug)]
pub enum Statement {
    CreateDb(CreateDbStatement),
    CreateTable(CreateTableStatement),
    Config(ConfigStatement),
    Debug(DebugStatement),
    Echo(EchoStatement),
    Help(HelpStatement),
    Show(ShowStatement),
}

#[derive(Debug)]
pub struct EchoStatement {
    pub message: String,
}

#[derive(Debug)]
pub struct CreateDbStatement {
    pub db_name: String,
    pub create_if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableStatement {
    pub db_name: Option<String>,
    pub table_name: String,
    pub create_if_not_exists: bool,
}

#[derive(Debug)]
pub struct ConfigStatement {
    pub key: Box<[u8]>,
    pub value: Box<[u8]>,
}

#[derive(Debug)]
pub struct DebugStatement {
    pub stmt: Box<Statement>,
}

#[derive(Debug)]
pub struct HelpStatement {
    pub topic: Option<String>,
}

#[derive(Debug)]
pub struct ShowStatement {
    pub property: String,
    pub from: Option<String>,
}

impl DebugStatement {
    #[inline]
    pub fn execute(&self) -> ExecuteResult {
        ExecuteResult::Msg(format!("{:?}", self.stmt))
    }
}

impl HelpStatement {
    pub fn execute(&self) -> ExecuteResult {
        let msg = if let Some(topic) = self.topic.as_ref() {
            Self::display_topic(topic)
        } else {
            Self::display()
        };
        ExecuteResult::Msg(msg)
    }

    fn display_topic(topic: &str) -> String {
        match topic {
            "create" | "CREATE" => Self::display_create_topic(),
            "show" | "SHOW" => Self::display_show_topic(),
            _ => {
                format!("unkonwn command `{}`. Try `help`?", topic)
            }
        }
    }

    fn display_create_topic() -> String {
        r##"
CREATE DATABASE [IF NOT EXISTS] <name:ident>
    Create a new database.

CREATE TABLE [IF NOT EXISTS] [<db:ident>.]<name:ident>
    Create a new table.

Note:
    The ident accepts characters [a-zA-Z0-9_-].
"##
        .to_owned()
    }

    fn display_show_topic() -> String {
        r##"
SHOW <property:ident> [FROM <name:ident>]
    Show properties. supported properties:
    - databases
    - tables FROM <database>

Note:
    The ident accepts characters [a-zA-Z0-9_-].
"##
        .to_owned()
    }

    fn display() -> String {
        r##"
List of commands:

create      create database, table ...
help        get help about a topic or command

For information on a specific command, type `help <command>'.
"##
        .to_owned()
    }
}
