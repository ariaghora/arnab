#[derive(Debug)]
pub enum ArnabError {
    Error(String),
    StatementExecutionError {
        msg: String,
        sql: String,
        path: String,
    },
    UnknownModelType(String),
}
