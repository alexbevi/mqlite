use std::collections::BTreeMap;

use bson::Document;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct CursorBatch {
    pub cursor_id: i64,
    pub namespace: String,
    pub documents: Vec<Document>,
}

#[derive(Debug, Clone)]
struct CursorState {
    namespace: String,
    documents: Vec<Document>,
    position: usize,
}

#[derive(Debug, Default)]
pub struct CursorManager {
    next_cursor_id: i64,
    cursors: BTreeMap<i64, CursorState>,
}

#[derive(Debug, Error)]
pub enum CursorError {
    #[error("cursor {0} was not found")]
    CursorNotFound(i64),
}

impl CursorManager {
    pub fn new() -> Self {
        Self {
            next_cursor_id: 1,
            cursors: BTreeMap::new(),
        }
    }

    pub fn open(
        &mut self,
        namespace: impl Into<String>,
        documents: Vec<Document>,
        batch_size: Option<i64>,
        single_batch: bool,
    ) -> CursorBatch {
        let namespace = namespace.into();
        let resolved_batch_size = normalize_batch_size(batch_size, documents.len());
        let first_batch = documents
            .iter()
            .take(resolved_batch_size)
            .cloned()
            .collect::<Vec<_>>();

        if single_batch || first_batch.len() >= documents.len() {
            return CursorBatch {
                cursor_id: 0,
                namespace,
                documents: first_batch,
            };
        }

        let cursor_id = self.allocate_cursor_id();
        self.cursors.insert(
            cursor_id,
            CursorState {
                namespace: namespace.clone(),
                documents,
                position: resolved_batch_size,
            },
        );

        CursorBatch {
            cursor_id,
            namespace,
            documents: first_batch,
        }
    }

    pub fn get_more(
        &mut self,
        cursor_id: i64,
        batch_size: Option<i64>,
    ) -> Result<CursorBatch, CursorError> {
        let state = self
            .cursors
            .get_mut(&cursor_id)
            .ok_or(CursorError::CursorNotFound(cursor_id))?;
        let resolved_batch_size = normalize_batch_size(
            batch_size,
            state.documents.len().saturating_sub(state.position),
        );
        let next_position = (state.position + resolved_batch_size).min(state.documents.len());
        let batch = state.documents[state.position..next_position].to_vec();
        state.position = next_position;
        let namespace = state.namespace.clone();

        if state.position >= state.documents.len() {
            self.cursors.remove(&cursor_id);
            return Ok(CursorBatch {
                cursor_id: 0,
                namespace,
                documents: batch,
            });
        }

        Ok(CursorBatch {
            cursor_id,
            namespace,
            documents: batch,
        })
    }

    pub fn kill(&mut self, cursor_id: i64) -> bool {
        self.cursors.remove(&cursor_id).is_some()
    }

    fn allocate_cursor_id(&mut self) -> i64 {
        let cursor_id = self.next_cursor_id;
        self.next_cursor_id += 1;
        cursor_id
    }
}

fn normalize_batch_size(batch_size: Option<i64>, fallback: usize) -> usize {
    match batch_size {
        Some(value) if value > 0 => value as usize,
        _ => fallback.max(1),
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use pretty_assertions::assert_eq;

    use super::{CursorError, CursorManager};

    #[test]
    fn creates_cursor_and_consumes_batches() {
        let mut manager = CursorManager::new();
        let first = manager.open(
            "app.widgets",
            vec![doc! { "_id": 1 }, doc! { "_id": 2 }, doc! { "_id": 3 }],
            Some(2),
            false,
        );
        assert_eq!(first.documents.len(), 2);
        assert!(first.cursor_id > 0);

        let second = manager
            .get_more(first.cursor_id, Some(2))
            .expect("get more");
        assert_eq!(second.documents, vec![doc! { "_id": 3 }]);
        assert_eq!(second.cursor_id, 0);
    }

    #[test]
    fn reports_missing_cursor() {
        let mut manager = CursorManager::new();
        let error = manager.get_more(99, Some(1)).expect_err("missing cursor");
        assert!(matches!(error, CursorError::CursorNotFound(99)));
    }
}
