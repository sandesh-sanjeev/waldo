use crate::{
    runtime::IoBuf,
    storage::{AppendError, BufResult, QueryError, fate::FateSender},
};

#[derive(Debug)]
pub(super) enum Action {
    Query(Query),
    Append(Append),
}

/// A request to append some bytes into page.
#[derive(Debug)]
pub(super) struct Append {
    pub(super) buf: IoBuf,
    pub(super) tx: FateSender<BufResult<(), AppendError>>,
}

/// A request to query for some bytes from page.
#[derive(Debug)]
pub(super) struct Query {
    pub(super) after: u64,
    pub(super) buf: IoBuf,
    pub(super) tx: FateSender<BufResult<(), QueryError>>,
}

#[derive(Debug)]
pub(super) enum ActionCtx {
    Query {
        #[allow(dead_code)]
        after: u64,
        len: usize,
        tx: FateSender<BufResult<(), QueryError>>,
    },

    Append {
        after: u64,
        len: usize,
        tx: FateSender<BufResult<(), AppendError>>,
    },
}
