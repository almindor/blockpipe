use postgres;
use std;
use web3;

#[derive(Debug)]
pub enum PipeError {
    Fmt(std::fmt::Error),
    Web3(web3::Error),
    Postgres(postgres::Error),
}

impl std::fmt::Display for PipeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            PipeError::Fmt(ref err) => err.fmt(f),
            PipeError::Web3(ref err) => err.fmt(f),
            PipeError::Postgres(ref err) => err.fmt(f),
        }
    }
}

impl std::error::Error for PipeError {
    fn description(&self) -> &str {
        match *self {
            PipeError::Fmt(ref err) => err.description(),
            PipeError::Web3(ref err) => err.description(),
            PipeError::Postgres(ref err) => err.description(),
        }
    }
}

impl From<std::fmt::Error> for PipeError {
    fn from(err: std::fmt::Error) -> PipeError {
        PipeError::Fmt(err)
    }
}

impl From<web3::Error> for PipeError {
    fn from(err: web3::Error) -> PipeError {
        PipeError::Web3(err)
    }
}

impl From<postgres::Error> for PipeError {
    fn from(err: postgres::Error) -> PipeError {
        PipeError::Postgres(err)
    }
}
