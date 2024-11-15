// Copyright 2023-present The Sekas Authors.
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
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Error};
use clap::Parser;
use log::error;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use sekas_client::{AppError, ClientOptions, Database, SekasClient};
use sekas_parser::*;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser)]
#[clap(about = "Start sekas shell")]
pub struct Command {
    /// Sets the address of the target cluster to operate
    #[clap(long, default_value = "0.0.0.0:21805")]
    addrs: Vec<String>,

    /// Sets the connection timeout.
    #[clap(long, parse(try_from_str = parse_duration))]
    connection_timeout: Option<Duration>,

    /// Sets the rpc timeout.
    #[clap(long, parse(try_from_str = parse_duration))]
    rpc_timeout: Option<Duration>,

    /// Sets the log level.
    #[clap(long)]
    log_level: Option<tracing::Level>,
}

fn parse_duration(arg: &str) -> Result<std::time::Duration, std::num::ParseIntError> {
    let seconds = arg.parse()?;
    Ok(std::time::Duration::from_secs(seconds))
}

impl Command {
    pub fn run(self) {
        tracing_subscriber::fmt()
            .with_max_level(self.log_level.unwrap_or(tracing::Level::ERROR))
            .with_ansi(atty::is(atty::Stream::Stderr))
            .init();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime with current thread");
        runtime.block_on(async move {
            editor_main(self).await;
        });
    }
}

struct Session {
    sekas_client: SekasClient,

    database_cache: HashMap<String, Database>,
    table_cache: HashMap<(String, String), u64>,
}

impl Session {
    /// Parse user input into statements and execute it in local or remote
    /// server.
    async fn parse_and_execute(&mut self, input: &str) -> Result<()> {
        let Some(statement) = sekas_parser::parse(input).context("parse into statement")? else {
            return Ok(());
        };
        let execute_result = if let Some(result) = self.try_execute_in_local(statement).await? {
            result
        } else {
            let body = self.sekas_client.handle_statement(input).await?;
            serde_json::from_slice(&body).context("deserialize execute result")?
        };

        self.show_result(execute_result);
        Ok(())
    }

    /// Execute statement in local as possible. `None` is returned if the
    /// statement could not be executed in local.
    async fn try_execute_in_local(
        &mut self,
        statement: Statement,
    ) -> Result<Option<ExecuteResult>> {
        let result = match statement {
            Statement::Debug(debug) => debug.execute(),
            Statement::Help(help) => help.execute(),
            Statement::Echo(echo) => ExecuteResult::Msg(echo.message),
            Statement::CreateDb(create_db) => self.create_database(create_db).await?,
            Statement::CreateTable(create_table) => self.create_table(create_table).await?,
            Statement::Put(put) => self.put_key_value(put).await?,
            Statement::Delete(delete) => self.delete_key(delete).await?,
            Statement::Get(get) => self.get_key(get).await?,
            Statement::Config(_) | Statement::Show(_) => return Ok(None),
        };
        Ok(Some(result))
    }

    /// Execute create db statement.
    async fn create_database(&self, create_db_stmt: CreateDbStatement) -> Result<ExecuteResult> {
        if create_db_stmt.create_if_not_exists {
            match self.sekas_client.open_database(create_db_stmt.db_name.clone()).await {
                Ok(_) => {
                    return Ok(ExecuteResult::Msg(format!(
                        "db {} already exists",
                        create_db_stmt.db_name
                    )))
                }
                Err(AppError::NotFound(_)) => {
                    // no such db exists, create it.
                }
                Err(err) => return Err(err).context("failed to open database"),
            };
        }

        self.sekas_client
            .create_database(create_db_stmt.db_name.clone())
            .await
            .context("failed to create database")?;

        Ok(ExecuteResult::Msg("Ok".to_owned()))
    }

    /// Execute create table statement.
    async fn create_table(&self, create_table_stmt: CreateTableStatement) -> Result<ExecuteResult> {
        let database = self
            .sekas_client
            .open_database(create_table_stmt.db_name.clone())
            .await
            .context("failed to open database")?;

        if create_table_stmt.create_if_not_exists {
            match database.open_table(create_table_stmt.table_name.clone()).await {
                Ok(_) => {
                    return Ok(ExecuteResult::Msg(format!(
                        "table {} already exists",
                        create_table_stmt.table_name.clone()
                    )));
                }
                Err(AppError::NotFound(_)) => {
                    // no such table exists, create it.
                }
                Err(err) => return Err(err).context("failed to open table"),
            }
        }

        database
            .create_table(create_table_stmt.table_name)
            .await
            .context("failed to create table")?;

        Ok(ExecuteResult::Msg("Ok".to_owned()))
    }

    /// Put key value into table.
    async fn put_key_value(&mut self, stmt: PutStatement) -> Result<ExecuteResult> {
        let db = self.open_database(&stmt.db_name).await?;
        let table_id = self.get_table(&stmt.db_name, &stmt.table_name).await?;
        db.put(table_id, stmt.key, stmt.value).await?;
        Ok(ExecuteResult::Msg("OK".to_owned()))
    }

    /// Get value from table.
    async fn get_key(&mut self, stmt: GetStatement) -> Result<ExecuteResult> {
        let db = self.open_database(&stmt.db_name).await?;
        let table_id = self.get_table(&stmt.db_name, &stmt.table_name).await?;
        let result = db.get_raw_value(table_id, stmt.key).await?;

        let columns = ["value", "version", "is_tombstone"]
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let rows = if let Some(value) = result {
            let is_tombstone = value.content.is_none();
            vec![Row {
                values: vec![
                    if let Some(content) = value.content {
                        content.into()
                    } else {
                        serde_json::Value::Null
                    },
                    value.version.into(),
                    is_tombstone.into(),
                ],
            }]
        } else {
            vec![]
        };
        Ok(ExecuteResult::Data(ColumnResult { columns, rows }))
    }

    /// Delete key from table.
    async fn delete_key(&mut self, stmt: DeleteStatement) -> Result<ExecuteResult> {
        let db = self.open_database(&stmt.db_name).await?;
        let table_id = self.get_table(&stmt.db_name, &stmt.table_name).await?;
        db.delete(table_id, stmt.key).await?;
        Ok(ExecuteResult::Msg("OK".to_owned()))
    }

    /// Open the database.
    async fn open_database(&mut self, db_name: &str) -> Result<Database> {
        if let Some(db) = self.database_cache.get(db_name) {
            return Ok(db.clone());
        }

        let db = self.sekas_client.open_database(db_name.to_owned()).await?;
        self.database_cache.insert(db_name.to_owned(), db.clone());
        Ok(db)
    }

    /// Get the table id of the database.
    async fn get_table(&mut self, db_name: &str, table_name: &str) -> Result<u64> {
        let key = (db_name.to_owned(), table_name.to_owned());
        if let Some(&table_id) = self.table_cache.get(&key) {
            return Ok(table_id);
        }

        let db = self.open_database(db_name).await?;
        let table_desc = db.open_table(table_name.to_owned()).await?;
        self.table_cache.insert(key, table_desc.id);
        Ok(table_desc.id)
    }

    /// Show the execute result.
    fn show_result(&self, result: ExecuteResult) {
        use tabled::builder::Builder;
        use tabled::settings::Style;

        match result {
            ExecuteResult::Data(data) => {
                let total_columns = data.columns.len();
                let mut builder = Builder::new();
                builder.push_record(data.columns);
                for row in data.rows {
                    if row.values.len() != total_columns {
                        error!(
                            "the result row len {} is not equals to columns len {}",
                            row.values.len(),
                            total_columns
                        );
                    }
                    builder.push_record(row.values.iter().map(|v| match v {
                        serde_json::Value::String(str) => str.clone(),
                        _ => v.to_string(),
                    }));
                }

                let table = builder.build().with(Style::ascii_rounded()).to_string();
                println!("{}", table);
            }
            ExecuteResult::Msg(msg) => {
                println!("{}", msg);
            }
            ExecuteResult::None => (),
        }
    }
}

async fn editor_main(cmd: Command) {
    use rustyline::history::MemHistory;
    use rustyline::Config;

    let mut session = new_session(cmd).await.expect("new session");
    let cfg = Config::builder().build();
    let history = MemHistory::new();
    let mut editor = Editor::<(), _>::with_history(cfg, history).expect("Editor::new");
    loop {
        let readline = editor.readline(">> ");
        match readline {
            Ok(line) => {
                let _ = editor.add_history_entry(line.as_str());
                if let Err(err) = session.parse_and_execute(line.trim()).await {
                    std::io::stderr()
                        .write_fmt(format_args!("ERROR: {:?}\n", err))
                        .unwrap_or_default();
                }
            }
            Err(ReadlineError::Interrupted) => {
                std::io::stderr().write_all(b"CTRL-C\n").unwrap_or_default();
                break;
            }
            Err(ReadlineError::Eof) => {
                std::io::stderr().write_all(b"CTRL-D\n").unwrap_or_default();
                break;
            }
            Err(err) => {
                std::io::stderr().write_fmt(format_args!("{:?}\n", err)).unwrap_or_default();
                break;
            }
        }
    }
}

async fn new_session(cmd: Command) -> Result<Session> {
    let opts = ClientOptions { connect_timeout: cmd.connection_timeout, timeout: cmd.rpc_timeout };
    let sekas_client = SekasClient::new(opts, cmd.addrs).await?;
    Ok(Session {
        sekas_client,
        database_cache: HashMap::default(),
        table_cache: HashMap::default(),
    })
}
