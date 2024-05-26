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

use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Error};
use clap::Parser;
use log::error;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use sekas_client::{ClientOptions, SekasClient};
use sekas_parser::{ExecuteResult, Statement};

type Result<T> = std::result::Result<T, Error>;

#[derive(Parser)]
#[clap(about = "Start sekas shell")]
pub struct Command {
    /// Sets the address of the target cluster to operate
    #[clap(long, default_value = "0.0.0.0:21805")]
    addrs: Vec<String>,

    /// Sets the log level.
    #[clap(long)]
    log_level: Option<tracing::Level>,
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
            editor_main(self.addrs).await;
        });
    }
}

struct Session {
    client: SekasClient,
}

impl Session {
    async fn parse_and_execute(&mut self, input: &str) -> Result<()> {
        let Some(statement) = sekas_parser::parse(input).context("parse into statement")? else {
            return Ok(());
        };
        let execute_result = if let Some(result) = self.try_handle_local_statement(statement)? {
            result
        } else {
            let body = self.client.handle_statement(input).await?;
            serde_json::from_slice(&body).context("deserialize execute result")?
        };

        self.show_result(execute_result);
        Ok(())
    }

    fn try_handle_local_statement(&self, statement: Statement) -> Result<Option<ExecuteResult>> {
        match statement {
            Statement::Debug(debug) => Ok(Some(debug.execute())),
            Statement::Help(help) => Ok(Some(help.execute())),
            Statement::Echo(echo) => Ok(Some(ExecuteResult::Msg(echo.message))),
            Statement::CreateDb(_) => todo!(),
            Statement::CreateTable(_) => todo!(),
            Statement::Config(_) => Ok(None),
        }
    }

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
                    builder.push_record(row.values.iter().map(ToString::to_string));
                }

                let table = builder.build().with(Style::ascii_rounded()).to_string();
                println!("{}", table);
            }
            ExecuteResult::Msg(msg) => {
                println!("{}", msg);
            }
        }
    }
}

async fn editor_main(addrs: Vec<String>) {
    use rustyline::history::MemHistory;
    use rustyline::Config;

    let mut session = new_session(addrs).await.expect("new session");
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

async fn new_session(addrs: Vec<String>) -> Result<Session> {
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(200)),
        timeout: Some(Duration::from_millis(500)),
    };
    let client = SekasClient::new(opts, addrs).await?;
    Ok(Session { client })
}
