use crate::pql::model::{
    InBookmarks,
    Match,
    MatchAnd,
    MatchOps,
    MatchOr,
    MatchPath,
    MatchTags,
    MatchText,
    MatchValue,
    MatchValues,
    Matches,
    ProcessedBy,
    QueryElement,
    HasUnprocessedData,
};
use crate::pql::utils::parse_and_escape_query;

#[derive(Debug)]
pub(crate) struct PqlError {
    pub message: String,
}

impl PqlError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for PqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PqlError {}

pub(crate) fn preprocess_query(el: QueryElement) -> Result<Option<QueryElement>, PqlError> {
    match el {
        QueryElement::And(mut op) => {
            let mut cleaned = Vec::new();
            for sub_element in op.and_ {
                if let Some(subquery) = preprocess_query(sub_element)? {
                    cleaned.push(subquery);
                }
            }
            if cleaned.is_empty() {
                Ok(None)
            } else if cleaned.len() == 1 {
                Ok(Some(cleaned.remove(0)))
            } else {
                op.and_ = cleaned;
                Ok(Some(QueryElement::And(op)))
            }
        }
        QueryElement::Or(mut op) => {
            let mut cleaned = Vec::new();
            for sub_element in op.or_ {
                if let Some(subquery) = preprocess_query(sub_element)? {
                    cleaned.push(subquery);
                }
            }
            if cleaned.is_empty() {
                Ok(None)
            } else if cleaned.len() == 1 {
                Ok(Some(cleaned.remove(0)))
            } else {
                op.or_ = cleaned;
                Ok(Some(QueryElement::Or(op)))
            }
        }
        QueryElement::Not(mut op) => {
            if let Some(subquery) = preprocess_query(*op.not_)? {
                op.not_ = Box::new(subquery);
                Ok(Some(QueryElement::Not(op)))
            } else {
                Ok(None)
            }
        }
        QueryElement::Match(filter) => Ok(filter.validate().map(QueryElement::Match)),
        QueryElement::MatchPath(filter) => Ok(filter.validate().map(QueryElement::MatchPath)),
        QueryElement::MatchText(filter) => Ok(filter.validate().map(QueryElement::MatchText)),
        QueryElement::MatchTags(filter) => Ok(filter.validate().map(QueryElement::MatchTags)),
        QueryElement::InBookmarks(filter) => Ok(filter.validate().map(QueryElement::InBookmarks)),
        QueryElement::ProcessedBy(filter) => Ok(filter.validate().map(QueryElement::ProcessedBy)),
        QueryElement::HasUnprocessedData(filter) => {
            Ok(filter.validate().map(QueryElement::HasUnprocessedData))
        }
    }
}

impl Match {
    fn validate(mut self) -> Option<Self> {
        if clean_matches(&mut self.match_) {
            Some(self)
        } else {
            None
        }
    }
}

impl MatchPath {
    fn validate(mut self) -> Option<Self> {
        if self.match_path.r#match.trim().is_empty() {
            return None;
        }
        if !self.match_path.raw_fts5_match {
            self.match_path.r#match = parse_and_escape_query(&self.match_path.r#match);
        }
        Some(self)
    }
}

impl MatchText {
    fn validate(mut self) -> Option<Self> {
        if !self.match_text.filter_only && self.match_text.r#match.trim().is_empty() {
            return None;
        }
        if self.match_text.filter_only {
            self.match_text.select_snippet_as = None;
            self.sort.order_by = false;
            self.sort.select_as = None;
            self.sort.row_n = false;
            self.match_text.r#match.clear();
        }
        if !self.match_text.raw_fts5_match {
            self.match_text.r#match = parse_and_escape_query(&self.match_text.r#match);
        }
        Some(self)
    }
}

impl MatchTags {
    fn validate(mut self) -> Option<Self> {
        if self.match_tags.tags.is_empty() {
            return None;
        }
        if self.match_tags.all_setters_required && self.match_tags.setters.is_empty() {
            self.match_tags.all_setters_required = false;
        }
        Some(self)
    }
}

impl InBookmarks {
    fn validate(self) -> Option<Self> {
        if self.in_bookmarks.filter {
            Some(self)
        } else {
            None
        }
    }
}

impl ProcessedBy {
    fn validate(self) -> Option<Self> {
        if self.processed_by.trim().is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

impl HasUnprocessedData {
    fn validate(self) -> Option<Self> {
        if self.has_data_unprocessed.setter_name.trim().is_empty() {
            return None;
        }
        if self.has_data_unprocessed.data_types.is_empty() {
            return None;
        }
        Some(self)
    }
}

fn clean_matches(matches: &mut Matches) -> bool {
    match matches {
        Matches::Ops(ops) => clean_match_ops(ops),
        Matches::And(and_ops) => clean_match_and(and_ops),
        Matches::Or(or_ops) => clean_match_or(or_ops),
        Matches::Not(not_ops) => clean_match_ops(&mut not_ops.not_),
    }
}

fn clean_match_and(and_ops: &mut MatchAnd) -> bool {
    let mut cleaned = Vec::new();
    for mut op in std::mem::take(&mut and_ops.and_) {
        if clean_match_ops(&mut op) {
            cleaned.push(op);
        }
    }
    and_ops.and_ = cleaned;
    !and_ops.and_.is_empty()
}

fn clean_match_or(or_ops: &mut MatchOr) -> bool {
    let mut cleaned = Vec::new();
    for mut op in std::mem::take(&mut or_ops.or_) {
        if clean_match_ops(&mut op) {
            cleaned.push(op);
        }
    }
    or_ops.or_ = cleaned;
    !or_ops.or_.is_empty()
}

fn clean_match_ops(ops: &mut MatchOps) -> bool {
    let mut has_valid = false;

    if let Some(value) = ops.eq.as_ref() {
        if value.is_empty() {
            ops.eq = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.neq.as_ref() {
        if value.is_empty() {
            ops.neq = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.in_.as_ref() {
        if value.is_empty() {
            ops.in_ = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.nin.as_ref() {
        if value.is_empty() {
            ops.nin = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.gt.as_ref() {
        if value.is_empty() {
            ops.gt = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.gte.as_ref() {
        if value.is_empty() {
            ops.gte = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.lt.as_ref() {
        if value.is_empty() {
            ops.lt = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.lte.as_ref() {
        if value.is_empty() {
            ops.lte = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.startswith.as_ref() {
        if value.is_empty() {
            ops.startswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.not_startswith.as_ref() {
        if value.is_empty() {
            ops.not_startswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.endswith.as_ref() {
        if value.is_empty() {
            ops.endswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.not_endswith.as_ref() {
        if value.is_empty() {
            ops.not_endswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.contains.as_ref() {
        if value.is_empty() {
            ops.contains = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.not_contains.as_ref() {
        if value.is_empty() {
            ops.not_contains = None;
        } else {
            has_valid = true;
        }
    }

    has_valid
}

impl MatchValue {
    fn is_empty(&self) -> bool {
        self.file_id.is_none()
            && self.item_id.is_none()
            && self.path.is_none()
            && self.filename.is_none()
            && self.sha256.is_none()
            && self.last_modified.is_none()
            && self.r#type.is_none()
            && self.size.is_none()
            && self.width.is_none()
            && self.height.is_none()
            && self.duration.is_none()
            && self.time_added.is_none()
            && self.md5.is_none()
            && self.audio_tracks.is_none()
            && self.video_tracks.is_none()
            && self.subtitle_tracks.is_none()
            && self.blurhash.is_none()
            && self.data_id.is_none()
            && self.language.is_none()
            && self.language_confidence.is_none()
            && self.text.is_none()
            && self.confidence.is_none()
            && self.text_length.is_none()
            && self.job_id.is_none()
            && self.setter_id.is_none()
            && self.setter_name.is_none()
            && self.data_index.is_none()
            && self.source_id.is_none()
    }
}

impl MatchValues {
    fn is_empty(&self) -> bool {
        self.file_id.is_none()
            && self.item_id.is_none()
            && self.path.is_none()
            && self.filename.is_none()
            && self.sha256.is_none()
            && self.last_modified.is_none()
            && self.r#type.is_none()
            && self.size.is_none()
            && self.width.is_none()
            && self.height.is_none()
            && self.duration.is_none()
            && self.time_added.is_none()
            && self.md5.is_none()
            && self.audio_tracks.is_none()
            && self.video_tracks.is_none()
            && self.subtitle_tracks.is_none()
            && self.blurhash.is_none()
            && self.data_id.is_none()
            && self.language.is_none()
            && self.language_confidence.is_none()
            && self.text.is_none()
            && self.confidence.is_none()
            && self.text_length.is_none()
            && self.job_id.is_none()
            && self.setter_id.is_none()
            && self.setter_name.is_none()
            && self.data_index.is_none()
            && self.source_id.is_none()
    }
}
