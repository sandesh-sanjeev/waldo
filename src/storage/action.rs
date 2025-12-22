use crate::{
    runtime::IoBuf,
    storage::{AppendError, BufResult, QueryError, fate::FateSender},
};

pub(super) type Sender<T, E> = FateSender<BufResult<T, E>>;

#[derive(Debug)]
pub(super) enum Action {
    Query(Query),
    Append(Append),
}

/// A request to query for some bytes from page.
#[derive(Debug)]
pub(super) struct Query {
    pub(super) buf: IoBuf,
    pub(super) after_seq_no: u64,
    pub(super) tx: Sender<(), QueryError>,
}

/// A request to append some bytes into page.
#[derive(Debug)]
pub(super) struct Append {
    pub(super) buf: IoBuf,
    pub(super) tx: Sender<(), AppendError>,
}

#[derive(Debug)]
pub(super) enum ActionCtx {
    Query { tx: Sender<(), QueryError> },
    Append { tx: Sender<(), AppendError> },
}

impl ActionCtx {
    pub(super) fn query(tx: Sender<(), QueryError>) -> ActionCtx {
        ActionCtx::Query { tx }
    }

    pub(super) fn append(tx: Sender<(), AppendError>) -> ActionCtx {
        ActionCtx::Append { tx }
    }
}
