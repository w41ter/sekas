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

use log::warn;
use sekas_parser::{ColumnResult, ConfigStatement, ExecuteResult, Row, ShowStatement};

use super::Root;
use crate::{Error, Result};

impl Root {
    /// Handle statement and return with json.
    pub async fn handle_statement(&self, input: &str) -> Result<Vec<u8>> {
        let result = self.handle_statement_inner(input).await?;
        match serde_json::to_vec(&result) {
            Ok(bytes) => Ok(bytes),
            Err(err) => {
                warn!("serialize result {:?}: {:?}", result, err);
                Ok(br#"{"Msg":"internal error, serialize execute result failed"}"#.to_vec())
            }
        }
    }

    async fn handle_statement_inner(&self, input: &str) -> Result<ExecuteResult> {
        use sekas_parser::Statement::*;

        let Some(stmt) = sekas_parser::parse(input).unwrap() else {
            return Ok(ExecuteResult::None);
        };
        match stmt {
            Config(config) => self.handle_config_stmt(config).await,
            Show(show) => self.handle_show_stmt(show).await,
            CreateDb(_) | CreateTable(_) | Debug(_) | Echo(_) | Help(_) => {
                Err(Error::InvalidArgument(", local stmt is sent to root server".to_owned()))
            }
        }
    }

    async fn handle_config_stmt(&self, config_stmt: ConfigStatement) -> Result<ExecuteResult> {
        let _ = config_stmt;
        Ok(ExecuteResult::Msg("the CONFIG statement is not supported yet".to_owned()))
    }

    async fn handle_show_stmt(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        match show_stmt.property.as_str() {
            "databases" => {
                if show_stmt.from.is_some() {
                    return Ok(ExecuteResult::Msg(
                        "FROM clause is not required by 'databases' property".to_owned(),
                    ));
                }
                let databases = self.list_database().await?;
                let columns =
                    ["id", "name"].into_iter().map(ToString::to_string).collect::<Vec<_>>();
                let rows = databases
                    .into_iter()
                    .map(|db| Row { values: vec![db.id.into(), db.name.into()] })
                    .collect::<Vec<_>>();
                Ok(ExecuteResult::Data(ColumnResult { columns, rows }))
            }
            "tables" => {
                let Some(db) = show_stmt.from.as_ref() else {
                    return Ok(ExecuteResult::Msg(
                        "the database is not specified, add it via the FROM clause".to_owned(),
                    ));
                };
                let Some(db_desc) = self.get_database(db).await? else {
                    return Ok(ExecuteResult::Msg(format!("database '{db}' is not exists")));
                };

                let tables = self.list_table(&db_desc).await?;
                let columns =
                    ["id", "name"].into_iter().map(ToString::to_string).collect::<Vec<_>>();
                let rows = tables
                    .into_iter()
                    .map(|table| Row { values: vec![table.id.into(), table.name.into()] })
                    .collect::<Vec<_>>();
                Ok(ExecuteResult::Data(ColumnResult { columns, rows }))
            }
            others => Ok(ExecuteResult::Msg(format!("unknown property: {others}"))),
        }
    }
}
