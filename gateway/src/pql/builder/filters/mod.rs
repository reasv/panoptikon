mod has_unprocessed;
mod in_bookmarks;
mod match_filter;
mod match_path;
mod match_tags;
mod match_text;
mod processed_by;

use super::{CteRef, QueryState};
use crate::pql::preprocess::PqlError;

pub(crate) trait FilterCompiler {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError>;
}
