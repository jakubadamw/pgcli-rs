#![feature(slice_patterns, type_ascription)]

#[macro_use] extern crate matches;

use linefeed::{Interface, Prompter, ReadResult, Signal};
use linefeed::complete::{Completer, Completion};
use linefeed::terminal::Terminal;

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Word};

use std::sync::{Arc, Mutex};

const HISTORY_FILE: &str = ".pgcli.rs.hst";

#[derive(Debug)]
enum Error {
    IOError(std::io::Error),
    PostgresError(postgres::Error),
    CommandUnknown
}

impl std::convert::From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IOError(error)
    }
}

impl std::convert::From<postgres::Error> for Error {
    fn from(error: postgres::Error) -> Self {
        Error::PostgresError(error)
    }
}

struct SQLCompleter {
    connection: Arc<Mutex<postgres::Client>>,
    completions: Vec<Arc<Mutex<Box<dyn WordCompletion>>>>
}

impl SQLCompleter {
    const DIALECT: GenericDialect = GenericDialect {};

    fn new(connection: postgres::Client) -> SQLCompleter {
        SQLCompleter {
            connection: Arc::new(Mutex::new(connection)),
            completions: vec![Arc::new(Mutex::new(Box::new(TableNameCompletion)))]
        }
    }
}

impl<Term: Terminal> Completer<Term> for SQLCompleter {
    fn complete(&self,
        _word: &str, prompter: &Prompter<Term>,
        _start: usize, end: usize)
    -> Option<Vec<Completion>> {
        let line = prompter.buffer();
        let mut tokenizer = Tokenizer::new(&Self::DIALECT, &line[..end]);

        match tokenizer.tokenize() {
        Ok(ref rows) if matches!(rows.last(), Some(Token::Word(_))) => {
            let tokens = tokens_without_whitespaces(rows.as_slice());
                for completion in self.completions.iter() {
                    let mut connection = self.connection.lock().unwrap();
                    let completion = completion.lock().unwrap();
                let rows = completion.complete(&mut connection, &tokens);
                if let Ok(words) = rows {
                        return Some(words
                            .into_iter()
                            .map(|word| Completion::simple(word.to_owned()))
                            .collect());
                    }
                }
                None
            },
        Ok(_) | Err(_) => None
        }
    }
}

fn tokens_without_whitespaces(tokens: &[Token]) -> Vec<Token> {
    tokens
        .iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .cloned()
        .collect()
}

trait SpecialQueryCommand: Send {
    fn result_title(&self) -> &'static str;
    fn prepare_query(&self, args: &str) -> Result<String, Error>;
}

struct ListTablesCommand;
impl ListTablesCommand {
    const LIST_TABLES_QUERY: &'static str = "\
        SELECT schemaname AS Schema, tablename AS Name, tableowner AS Type, tablespace AS Owner FROM pg_catalog.pg_tables \
        WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
}
impl SpecialQueryCommand for ListTablesCommand {
    fn result_title(&self) -> &'static str {
        "Table list"
    }
    fn prepare_query(&self, args: &str) -> Result<String, Error> {
        assert!(args.is_empty());
        Ok(Self::LIST_TABLES_QUERY.to_owned())
    }
}

trait WordCompletion: Send {
    fn complete(&self, conn: &mut postgres::Client, tokens: &[Token])
        -> Result<Vec<String>, Error>;
}

struct TableNameCompletion;
impl TableNameCompletion {
    const LIST_TABLES_QUERY: &'static str = "\
        SELECT tablename FROM pg_catalog.pg_tables \
        WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";

    fn table_names(&self, connection: &mut postgres::Client) -> Result<Vec<String>, Error> {
        Ok(connection.query(Self::LIST_TABLES_QUERY, &[])?
            .into_iter()
            .map(|row| row.get("tablename"))
            .collect())
    }
}
impl WordCompletion for TableNameCompletion {
    fn complete(&self, conn: &mut postgres::Client, tokens: &[Token]) -> Result<Vec<String>, Error> {
        match tokens {
            [..,
             Token::Word(Word { keyword: keyword_before, .. }),
             Token::Word(Word { keyword, value, .. })]
                if keyword_before == "FROM" && keyword == "" => {
                Ok(self
                    .table_names(conn)?
                    .into_iter()
                    .filter(|name| name.starts_with(value))
                    .collect())
             }
             _ => Ok(vec![])
        }
    }
}

const CONNECTION_OPTIONS: &'static str
    = "host=/var/run/postgresql port=5432 user=postgres password=postgres";

fn connect() -> Result<postgres::Client, postgres::Error> {
    postgres::Client::connect(CONNECTION_OPTIONS, postgres::NoTls)
}

struct PrettyPrinter {
    types: Vec<postgres::types::Type>
}

impl PrettyPrinter {
    fn from_columns(columns: &[postgres::Column]) -> PrettyPrinter {
        PrettyPrinter {
            types: columns.iter().map(|col| col.type_()).cloned().collect()
        }
    }

    fn pretty_print(&self, row: &postgres::Row) -> Result<Vec<String>, Error> {
        use postgres::types::Type;
        use std::error::Error as StdError;

        (0..row.len()).map(|idx| {
            match self.types[idx] {
                Type::BOOL =>
                    row.try_get::<_, bool>(idx).map(|v| v.to_string()),
                Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME | Type::UNKNOWN
                    => row.try_get::<_, String>(idx).map(|v| v.to_string()),
                Type::INT4 =>
                    row.try_get::<_, i32>(idx).map(|v| v.to_string()),
                Type::INT8 =>
                    row.try_get::<_, i64>(idx).map(|v| v.to_string()),
                _ =>
                    Ok("[unrepresentable]".to_string())
            }
        }).map(|value| {
            match value {
                Err(err) => match err.source() {
                    Some(ref source) if source.is::<postgres::types::WasNull>() =>
                        Ok("NULL".to_string()),
                    _ => Err(err)
                }
                value => value
            }.map_err(|err| err.into(): Error)
        }).collect()
    }
}

fn pretty_print_result(rows: Vec<postgres::Row>) -> Result<(), Error> {
    use prettytable::{Table, Row, Cell};
    use prettytable::format;

    let row_count = rows.len();

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    if row_count > 0 {
        let first_row = &rows[0];
        table.set_titles(Row::new(
            first_row
                .columns()
                .iter()
                .map(|c| Cell::new(c.name()))
                .collect()
        ));

        let pretty_printer = PrettyPrinter::from_columns(first_row.columns());
        for row in rows {
            table.add_row(Row::new(
                pretty_printer.pretty_print(&row)?
                    .into_iter()
                    .map(|value| Cell::new(&value))
                    .collect()
            ));
        }

        table.printstd();
        println!();
    }

    println!("({} rows)", row_count);

    Ok(())
}

fn main() -> Result<(), Error> {
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(HISTORY_FILE)?;

    let interface = Arc::new(Interface::new("demo")?);
    if let Err(e) = interface.load_history(HISTORY_FILE) {
        eprintln!("Could not load history file `{}`: {}", HISTORY_FILE, e);
    }

    interface.set_report_signal(Signal::Interrupt, true);
    interface.set_report_signal(Signal::Quit, true);

    interface.set_completer(Arc::new(SQLCompleter::new(connect()?)));
    interface.set_prompt("postgres> ")?;

    let special_commands: Vec<(&'static str, Box<dyn SpecialQueryCommand>)> = vec![
        ("dt", Box::new(ListTablesCommand))
    ];

    loop {
        let res = interface.read_line()?;
        match res {
            ReadResult::Input(line) => {
                let mut query = line.trim().to_owned();
                if query.is_empty() { continue; }
                interface.add_history_unique(line.clone());

                if query.starts_with("\\") {
                    let (name, args) = match query.find(|ch: char| ch.is_whitespace()) {
                        Some(pos) => (&query[1..pos], query[pos..].trim_start()),
                        None => (&query[1..], "")
                    };

                    let command = special_commands
                        .iter()
                        .find(|c| c.0 == name)
                        .map(|c| &*c.1)
                        .ok_or(Error::CommandUnknown)?;

                    query = command.prepare_query(args)?;
                    println!("{}\n", command.result_title());
                }

                let mut connection = connect()?;
                let rows = connection.query(&query[..], &[])?;
                pretty_print_result(rows)?;
            }

            ReadResult::Signal(Signal::Interrupt) => {
                interface.cancel_read_line()?;
            }

            ReadResult::Signal(Signal::Quit) | ReadResult::Eof => {
                if let Err(e) = interface.save_history(HISTORY_FILE) {
                    eprintln!("Could not save history file `{}`: {}", HISTORY_FILE, e);
                }
                println!("Goodbye.");
                break;
            }

            ReadResult::Signal(_) => unreachable!()
        }
    }

    Ok(())
}
