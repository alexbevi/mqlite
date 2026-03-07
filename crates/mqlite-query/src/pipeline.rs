use std::{
    collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use bson::{Bson, Document, doc};
use chrono::{DateTime, Datelike, Duration, Months, Utc};
use mqlite_bson::{compare_bson, lookup_path, lookup_path_owned, remove_path, set_path};

use crate::{
    QueryError,
    capabilities::SUPPORTED_AGGREGATION_STAGES,
    expression::{
        eval_expression_with_variables, integer_value, number_bson, numeric_value,
        validate_expression,
    },
    filter::document_matches_with_variables,
    parse_filter,
    projection::apply_projection_with_variables,
};

pub trait CollectionResolver {
    fn resolve_collection(&self, database: &str, collection: &str) -> Vec<Document>;

    fn resolve_change_events(&self) -> Vec<Document> {
        Vec::new()
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct NoopResolver;

impl CollectionResolver for NoopResolver {
    fn resolve_collection(&self, _database: &str, _collection: &str) -> Vec<Document> {
        Vec::new()
    }
}

const MAX_BSON_OBJECT_SIZE_BYTES: usize = 16 * 1024 * 1024;
const CHANGE_STREAM_SPLIT_TOKEN_FIELD: &str = "fragmentNum";
const CHANGE_STREAM_SPLIT_EVENT_FIELD: &str = "splitEvent";
const CHANGE_STREAM_SPLIT_EVENT_FRAGMENT_FIELD: &str = "fragment";
const CHANGE_STREAM_SPLIT_EVENT_TOTAL_FIELD: &str = "of";

pub fn run_pipeline(
    documents: Vec<Document>,
    pipeline: &[Document],
) -> Result<Vec<Document>, QueryError> {
    let resolver = NoopResolver;
    run_pipeline_with_resolver(documents, pipeline, "", None, &resolver)
}

pub fn run_pipeline_with_resolver<R: CollectionResolver>(
    documents: Vec<Document>,
    pipeline: &[Document],
    database: &str,
    source_collection: Option<&str>,
    resolver: &R,
) -> Result<Vec<Document>, QueryError> {
    let context = PipelineContext {
        inside_facet: false,
        database,
        source_collection,
        resolver,
        variables: BTreeMap::new(),
    };
    run_pipeline_with_context(documents, pipeline, &context)
}

#[derive(Debug, Clone)]
struct PipelineContext<'a, R> {
    inside_facet: bool,
    database: &'a str,
    source_collection: Option<&'a str>,
    resolver: &'a R,
    variables: BTreeMap<String, Bson>,
}

fn run_pipeline_with_context<R: CollectionResolver>(
    documents: Vec<Document>,
    pipeline: &[Document],
    context: &PipelineContext<'_, R>,
) -> Result<Vec<Document>, QueryError> {
    let mut current = documents;
    let has_change_stream_split_large_event = pipeline
        .iter()
        .any(|stage| stage.len() == 1 && stage.contains_key("$changeStreamSplitLargeEvent"));
    let mut saw_change_stream = false;
    let mut saw_change_stream_split_large_event = false;
    let mut change_stream_split_resume = None;

    for (stage_index, stage) in pipeline.iter().enumerate() {
        if stage.len() != 1 {
            return Err(QueryError::InvalidStage);
        }

        let (stage_name, stage_spec) = stage.iter().next().expect("single stage");
        current = match stage_name.as_str() {
            "$changeStream" if context.inside_facet || stage_index != 0 => {
                return Err(QueryError::InvalidStage);
            }
            "$changeStream" => {
                if saw_change_stream {
                    return Err(QueryError::InvalidStage);
                }
                saw_change_stream = true;
                let output = change_stream_documents(
                    stage_spec,
                    context,
                    has_change_stream_split_large_event,
                )?;
                change_stream_split_resume = output.resume_from_split;
                output.documents
            }
            "$changeStreamSplitLargeEvent" if context.inside_facet => {
                return Err(QueryError::InvalidStage);
            }
            "$changeStreamSplitLargeEvent" if !saw_change_stream => {
                return Err(QueryError::InvalidArgument(
                    "$changeStreamSplitLargeEvent can only be used in a $changeStream pipeline"
                        .to_string(),
                ));
            }
            "$changeStreamSplitLargeEvent" => {
                if saw_change_stream_split_large_event || stage_index + 1 != pipeline.len() {
                    return Err(QueryError::InvalidStage);
                }
                saw_change_stream_split_large_event = true;
                split_change_stream_large_events(
                    current,
                    stage_spec,
                    change_stream_split_resume.take(),
                )?
            }
            "$bucket" => bucket_documents(current, stage_spec, &context.variables)?,
            "$bucketAuto" => bucket_auto_documents(current, stage_spec, &context.variables)?,
            "$collStats" => coll_stats_documents(current, stage_index, stage_spec)?,
            "$currentOp" => current_op_documents(stage_index, stage_spec)?,
            "$densify" => densify_documents(current, stage_spec)?,
            "$fill" => fill_documents(current, stage_spec, &context.variables)?,
            "$indexStats" => index_stats_documents(stage_index, stage_spec)?,
            "$listCatalog" => list_catalog_documents(stage_index, stage_spec)?,
            "$listClusterCatalog" => list_cluster_catalog_documents(stage_index, stage_spec)?,
            "$listCachedAndActiveUsers" => {
                list_cached_and_active_users_documents(stage_index, stage_spec)?
            }
            "$listLocalSessions" => list_local_sessions_documents(stage_index, stage_spec)?,
            "$listSampledQueries" => list_sampled_queries_documents(stage_index, stage_spec)?,
            "$listSearchIndexes" => list_search_indexes_documents(stage_index, stage_spec)?,
            "$listSessions" => list_sessions_documents(stage_index, stage_spec)?,
            "$listMqlEntities" => list_mql_entities_documents(stage_index, stage_spec)?,
            "$planCacheStats" => plan_cache_stats_documents(stage_index, stage_spec)?,
            "$documents" if context.inside_facet => return Err(QueryError::InvalidStage),
            "$documents" => documents_stage(stage_index, stage_spec)?,
            "$facet" if context.inside_facet => return Err(QueryError::InvalidStage),
            "$facet" => facet_documents(current, stage_spec, context)?,
            "$geoNear" if stage_index != 0 => return Err(QueryError::InvalidStage),
            "$geoNear" => geo_near_documents(current, stage_spec, &context.variables)?,
            "$graphLookup" => graph_lookup_documents(current, stage_spec, context)?,
            "$lookup" => lookup_documents(current, stage_spec, context)?,
            "$match" => {
                let filter = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current
                    .into_iter()
                    .map(|document| {
                        document_matches_with_variables(&document, filter, &context.variables)
                            .map(|matches| (document, matches))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter_map(|(document, matches)| matches.then_some(document))
                    .collect()
            }
            "$out" => {
                if stage_index + 1 != pipeline.len() {
                    return Err(QueryError::InvalidStage);
                }
                parse_out_stage(stage_spec)?;
                Vec::new()
            }
            "$merge" => {
                if stage_index + 1 != pipeline.len() {
                    return Err(QueryError::InvalidStage);
                }
                parse_merge_stage(stage_spec)?;
                Vec::new()
            }
            "$project" => {
                let projection = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current
                    .into_iter()
                    .map(|document| {
                        apply_projection_with_variables(
                            &document,
                            Some(projection),
                            &context.variables,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            "$querySettings" => query_settings_documents(stage_index, stage_spec)?,
            "$set" | "$addFields" => {
                let spec = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current
                    .into_iter()
                    .map(|mut document| {
                        for (field, expr) in spec {
                            let value = eval_expression_with_variables(
                                &document,
                                expr,
                                &context.variables,
                            )?;
                            set_path(&mut document, field, value)
                                .map_err(|_| QueryError::InvalidStructure)?;
                        }
                        Ok(document)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            "$unset" => {
                let fields = parse_unset(stage_spec)?;
                current
                    .into_iter()
                    .map(|mut document| {
                        for field in &fields {
                            remove_path(&mut document, field)
                                .map_err(|_| QueryError::InvalidStructure)?;
                        }
                        Ok(document)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            "$limit" => {
                let limit = integer_value(stage_spec).ok_or(QueryError::InvalidStage)?;
                current.into_iter().take(limit.max(0) as usize).collect()
            }
            "$sample" => sample_documents(current, stage_spec)?,
            "$setWindowFields" => {
                set_window_fields_documents(current, stage_spec, &context.variables)?
            }
            "$skip" => {
                let skip = integer_value(stage_spec).ok_or(QueryError::InvalidStage)?;
                current.into_iter().skip(skip.max(0) as usize).collect()
            }
            "$sortByCount" => sort_by_count(current, stage_spec, &context.variables)?,
            "$sort" => {
                let sort = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current.sort_by(|left, right| compare_documents_by_sort(left, right, sort));
                current
            }
            "$count" => {
                let field = stage_spec.as_str().ok_or(QueryError::InvalidStage)?;
                let mut counted = Document::new();
                counted.insert(field, current.len() as i64);
                vec![counted]
            }
            "$unwind" => unwind_documents(current, stage_spec)?,
            "$group" => group_documents(
                current,
                stage_spec.as_document().ok_or(QueryError::InvalidStage)?,
                &context.variables,
            )?,
            "$redact" => current
                .into_iter()
                .filter_map(|document| {
                    redact_document(&document, stage_spec, &context.variables).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
            "$replaceRoot" => replace_root(
                current,
                stage_spec.as_document().ok_or(QueryError::InvalidStage)?,
                &context.variables,
            )?,
            "$replaceWith" => replace_with(current, stage_spec, &context.variables)?,
            "$unionWith" => union_with_documents(current, stage_spec, context)?,
            other => return Err(QueryError::UnsupportedStage(other.to_string())),
        };
    }

    if saw_change_stream && !saw_change_stream_split_large_event {
        validate_change_stream_output_sizes(&current)?;
    }

    Ok(current)
}

fn facet_documents<R: CollectionResolver>(
    documents: Vec<Document>,
    spec: &Bson,
    context: &PipelineContext<'_, R>,
) -> Result<Vec<Document>, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    if spec.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    let mut output = Document::new();
    for (facet_name, facet_pipeline) in spec {
        validate_facet_name(facet_name)?;
        let stages = facet_pipeline.as_array().ok_or(QueryError::InvalidStage)?;
        let stages = stages
            .iter()
            .map(|value| value.as_document().cloned().ok_or(QueryError::InvalidStage))
            .collect::<Result<Vec<_>, _>>()?;
        let facet_context = PipelineContext {
            inside_facet: true,
            database: context.database,
            source_collection: context.source_collection,
            resolver: context.resolver,
            variables: context.variables.clone(),
        };
        let results = run_pipeline_with_context(documents.clone(), &stages, &facet_context)?;
        output.insert(
            facet_name.clone(),
            Bson::Array(results.into_iter().map(Bson::Document).collect()),
        );
    }

    Ok(vec![output])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeStreamFullDocumentMode {
    Default,
    UpdateLookup,
    WhenAvailable,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeStreamFullDocumentBeforeChangeMode {
    Off,
    WhenAvailable,
    Required,
}

#[derive(Debug, Clone)]
struct ChangeStreamSpec {
    resume_after: Option<Document>,
    start_after: Option<Document>,
    start_at_operation_time: Option<bson::Timestamp>,
    full_document: ChangeStreamFullDocumentMode,
    full_document_before_change: ChangeStreamFullDocumentBeforeChangeMode,
    all_changes_for_cluster: bool,
    show_expanded_events: bool,
    resume_from_split: Option<ChangeStreamSplitResume>,
}

#[derive(Debug, Clone)]
struct ChangeStreamSplitResume {
    token: Document,
    skip_first_fragments: usize,
}

#[derive(Debug)]
struct ChangeStreamStageOutput {
    documents: Vec<Document>,
    resume_from_split: Option<ChangeStreamSplitResume>,
}

#[derive(Debug)]
struct SplitChangeStreamDocuments {
    total_fragments: usize,
    fragments: Vec<Document>,
}

#[derive(Debug, Clone)]
struct InternalChangeEvent {
    token: Document,
    cluster_time: bson::Timestamp,
    wall_time: bson::DateTime,
    database: String,
    collection: Option<String>,
    operation_type: String,
    document_key: Option<Document>,
    full_document: Option<Document>,
    full_document_before_change: Option<Document>,
    update_description: Option<Document>,
    expanded: bool,
    extra_fields: Document,
}

fn change_stream_documents<R: CollectionResolver>(
    spec: &Bson,
    context: &PipelineContext<'_, R>,
    has_change_stream_split_large_event: bool,
) -> Result<ChangeStreamStageOutput, QueryError> {
    let spec = parse_change_stream_spec(spec, context, has_change_stream_split_large_event)?;
    let mut events = context
        .resolver
        .resolve_change_events()
        .into_iter()
        .map(parse_internal_change_event)
        .collect::<Result<Vec<_>, _>>()?;

    events.retain(|event| change_stream_namespace_matches(event, &spec, context));
    if !spec.show_expanded_events {
        events.retain(|event| !event.expanded);
    }

    if let Some(start_at_operation_time) = spec.start_at_operation_time {
        events.retain(|event| {
            compare_bson(
                &Bson::Timestamp(event.cluster_time),
                &Bson::Timestamp(start_at_operation_time),
            )
            .is_ge()
        });
    }

    if let Some(token) = spec.resume_after.as_ref().or(spec.start_after.as_ref()) {
        let match_token = spec
            .resume_from_split
            .as_ref()
            .map(|resume| &resume.token)
            .unwrap_or(token);
        let Some(position) = events.iter().position(|event| {
            compare_bson(
                &Bson::Document(event.token.clone()),
                &Bson::Document(match_token.clone()),
            )
            .is_eq()
        }) else {
            return Err(QueryError::InvalidArgument(
                "resume token does not exist in the local change-event log".to_string(),
            ));
        };
        let skip = if spec.resume_from_split.is_some() {
            position
        } else {
            position + 1
        };
        events = events.into_iter().skip(skip).collect();
    }

    let documents = events
        .into_iter()
        .map(|event| materialize_change_stream_event(event, &spec))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ChangeStreamStageOutput {
        documents,
        resume_from_split: spec.resume_from_split,
    })
}

fn parse_change_stream_spec<R: CollectionResolver>(
    spec: &Bson,
    context: &PipelineContext<'_, R>,
    has_change_stream_split_large_event: bool,
) -> Result<ChangeStreamSpec, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut resume_after = None;
    let mut start_after = None;
    let mut start_at_operation_time = None;
    let mut full_document = ChangeStreamFullDocumentMode::Default;
    let mut full_document_before_change = ChangeStreamFullDocumentBeforeChangeMode::Off;
    let mut all_changes_for_cluster = false;
    let mut show_expanded_events = false;

    for (field, value) in document {
        match field.as_str() {
            "resumeAfter" => {
                resume_after = Some(value.as_document().ok_or(QueryError::InvalidStage)?.clone())
            }
            "startAfter" => {
                start_after = Some(value.as_document().ok_or(QueryError::InvalidStage)?.clone())
            }
            "startAtOperationTime" => {
                start_at_operation_time =
                    Some(value.as_timestamp().ok_or(QueryError::InvalidStage)?)
            }
            "fullDocument" => {
                full_document = match value.as_str().ok_or(QueryError::InvalidStage)? {
                    "default" => ChangeStreamFullDocumentMode::Default,
                    "updateLookup" => ChangeStreamFullDocumentMode::UpdateLookup,
                    "whenAvailable" => ChangeStreamFullDocumentMode::WhenAvailable,
                    "required" => ChangeStreamFullDocumentMode::Required,
                    _ => return Err(QueryError::InvalidStage),
                };
            }
            "fullDocumentBeforeChange" => {
                full_document_before_change =
                    match value.as_str().ok_or(QueryError::InvalidStage)? {
                        "off" => ChangeStreamFullDocumentBeforeChangeMode::Off,
                        "whenAvailable" => ChangeStreamFullDocumentBeforeChangeMode::WhenAvailable,
                        "required" => ChangeStreamFullDocumentBeforeChangeMode::Required,
                        _ => return Err(QueryError::InvalidStage),
                    };
            }
            "allChangesForCluster" => {
                all_changes_for_cluster = value.as_bool().ok_or(QueryError::InvalidStage)?
            }
            "showExpandedEvents" => {
                show_expanded_events = value.as_bool().ok_or(QueryError::InvalidStage)?
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let resume_options = usize::from(resume_after.is_some())
        + usize::from(start_after.is_some())
        + usize::from(start_at_operation_time.is_some());
    if resume_options > 1 {
        return Err(QueryError::InvalidStage);
    }
    if all_changes_for_cluster
        && !(context.database == "admin" && context.source_collection.is_none())
    {
        return Err(QueryError::InvalidStage);
    }

    let resume_from_split = resume_after
        .as_ref()
        .or(start_after.as_ref())
        .map(parse_change_stream_split_resume)
        .transpose()?
        .flatten();
    if resume_from_split.is_some() && !has_change_stream_split_large_event {
        return Err(QueryError::ChangeStreamFatalError(
            "To resume from a split event, the $changeStream pipeline must include a $changeStreamSplitLargeEvent stage"
                .to_string(),
        ));
    }

    Ok(ChangeStreamSpec {
        resume_after,
        start_after,
        start_at_operation_time,
        full_document,
        full_document_before_change,
        all_changes_for_cluster,
        show_expanded_events,
        resume_from_split,
    })
}

fn parse_change_stream_split_resume(
    token: &Document,
) -> Result<Option<ChangeStreamSplitResume>, QueryError> {
    let Some(fragment_value) = token.get(CHANGE_STREAM_SPLIT_TOKEN_FIELD) else {
        return Ok(None);
    };
    let Some(fragment_number) = non_negative_usize(fragment_value) else {
        return Err(QueryError::InvalidStage);
    };
    let mut base_token = token.clone();
    base_token.remove(CHANGE_STREAM_SPLIT_TOKEN_FIELD);
    Ok(Some(ChangeStreamSplitResume {
        token: base_token,
        skip_first_fragments: fragment_number + 1,
    }))
}

fn non_negative_usize(value: &Bson) -> Option<usize> {
    match value {
        Bson::Int32(value) if *value >= 0 => Some(*value as usize),
        Bson::Int64(value) if *value >= 0 => usize::try_from(*value).ok(),
        _ => None,
    }
}

fn split_change_stream_large_events(
    documents: Vec<Document>,
    spec: &Bson,
    resume_from_split: Option<ChangeStreamSplitResume>,
) -> Result<Vec<Document>, QueryError> {
    validate_change_stream_split_large_event_spec(spec)?;
    let mut output = Vec::new();
    let mut matched_resume_token = resume_from_split.is_none();

    for document in documents {
        let resume_target = resume_from_split
            .as_ref()
            .is_some_and(|resume| change_stream_document_has_token(&document, &resume.token));
        let document_size = bson_document_size(&document)?;

        if document_size <= MAX_BSON_OBJECT_SIZE_BYTES {
            if resume_target {
                return Err(incompatible_split_resume_error());
            }
            output.push(document);
            continue;
        }

        let skip_first_fragments = if resume_target {
            resume_from_split
                .as_ref()
                .expect("resume target has split resume state")
                .skip_first_fragments
        } else {
            0
        };
        let split = split_change_stream_document(&document, skip_first_fragments)?;
        if resume_target {
            if split.total_fragments < skip_first_fragments {
                return Err(incompatible_split_resume_error());
            }
            matched_resume_token = true;
        }
        output.extend(split.fragments);
    }

    if !matched_resume_token {
        return Err(incompatible_split_resume_error());
    }

    Ok(output)
}

fn validate_change_stream_split_large_event_spec(spec: &Bson) -> Result<(), QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    if !spec.is_empty() {
        return Err(QueryError::InvalidStage);
    }
    Ok(())
}

fn change_stream_document_has_token(document: &Document, token: &Document) -> bool {
    document
        .get_document("_id")
        .ok()
        .is_some_and(|document_token| {
            compare_bson(
                &Bson::Document(document_token.clone()),
                &Bson::Document(token.clone()),
            )
            .is_eq()
        })
}

fn split_change_stream_document(
    document: &Document,
    skip_first_fragments: usize,
) -> Result<SplitChangeStreamDocuments, QueryError> {
    let base_token = document
        .get_document("_id")
        .map_err(|_| QueryError::InvalidStage)?
        .clone();
    let mut fields = document
        .iter()
        .filter(|(field, _)| field.as_str() != "_id")
        .map(|(field, value)| {
            Ok((
                bson_element_size(field, value)?,
                field.clone(),
                value.clone(),
            ))
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    if fields.is_empty() {
        return Err(QueryError::ChangeStreamFatalError(
            "cannot split a change stream event that contains only _id".to_string(),
        ));
    }
    fields.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut fragments = Vec::new();
    let mut field_index = 0;
    while field_index < fields.len() {
        let fragment_index = fragments.len();
        let mut fragment_token = base_token.clone();
        fragment_token.insert(
            CHANGE_STREAM_SPLIT_TOKEN_FIELD,
            Bson::Int64(fragment_index as i64),
        );
        let mut fragment = doc! {
            "_id": Bson::Document(fragment_token),
            CHANGE_STREAM_SPLIT_EVENT_FIELD: {
                CHANGE_STREAM_SPLIT_EVENT_FRAGMENT_FIELD: (fragment_index + 1) as i32,
                CHANGE_STREAM_SPLIT_EVENT_TOTAL_FIELD: 0_i32,
            }
        };
        let mut added_any = false;

        while field_index < fields.len() {
            let (_, field_name, field_value) = &fields[field_index];
            fragment.insert(field_name.clone(), field_value.clone());
            if bson_document_size(&fragment)? <= MAX_BSON_OBJECT_SIZE_BYTES {
                added_any = true;
                field_index += 1;
                continue;
            }

            fragment.remove(field_name);
            if !added_any {
                return Err(QueryError::BsonObjectTooLarge(
                    "change stream event contains a top-level field that cannot fit in a split fragment"
                        .to_string(),
                ));
            }
            break;
        }

        fragments.push(fragment);
    }

    let total_fragments = fragments.len();
    for fragment in &mut fragments {
        if let Some(Bson::Document(split_event)) = fragment.get_mut(CHANGE_STREAM_SPLIT_EVENT_FIELD)
        {
            split_event.insert(
                CHANGE_STREAM_SPLIT_EVENT_TOTAL_FIELD,
                Bson::Int32(total_fragments as i32),
            );
        }
    }

    let fragments = if skip_first_fragments >= total_fragments {
        Vec::new()
    } else {
        fragments
            .into_iter()
            .skip(skip_first_fragments)
            .collect::<Vec<_>>()
    };

    Ok(SplitChangeStreamDocuments {
        total_fragments,
        fragments,
    })
}

fn bson_document_size(document: &Document) -> Result<usize, QueryError> {
    bson::to_vec(document)
        .map(|bytes| bytes.len())
        .map_err(|_| QueryError::InvalidStage)
}

fn bson_element_size(field: &str, value: &Bson) -> Result<usize, QueryError> {
    let mut document = Document::new();
    document.insert(field, value.clone());
    bson_document_size(&document).map(|size| size.saturating_sub(5))
}

fn validate_change_stream_output_sizes(documents: &[Document]) -> Result<(), QueryError> {
    if documents
        .iter()
        .try_fold(false, |found_oversized, document| {
            if found_oversized {
                return Ok(true);
            }
            Ok(bson_document_size(document)? > MAX_BSON_OBJECT_SIZE_BYTES)
        })?
    {
        return Err(QueryError::BsonObjectTooLarge(
            "change stream event exceeds maximum BSON size; add $changeStreamSplitLargeEvent to the pipeline"
                .to_string(),
        ));
    }
    Ok(())
}

fn incompatible_split_resume_error() -> QueryError {
    QueryError::ChangeStreamFatalError(
        "resumed change stream pipeline no longer reproduces the split event referenced by the resume token"
            .to_string(),
    )
}

fn parse_internal_change_event(document: Document) -> Result<InternalChangeEvent, QueryError> {
    Ok(InternalChangeEvent {
        token: document
            .get_document("token")
            .map_err(|_| QueryError::InvalidStage)?
            .clone(),
        cluster_time: document
            .get_timestamp("clusterTime")
            .map_err(|_| QueryError::InvalidStage)?,
        wall_time: *document
            .get_datetime("wallTime")
            .map_err(|_| QueryError::InvalidStage)?,
        database: document
            .get_str("database")
            .map_err(|_| QueryError::InvalidStage)?
            .to_string(),
        collection: document.get_str("collection").ok().map(ToString::to_string),
        operation_type: document
            .get_str("operationType")
            .map_err(|_| QueryError::InvalidStage)?
            .to_string(),
        document_key: document.get_document("documentKey").ok().cloned(),
        full_document: document.get_document("fullDocument").ok().cloned(),
        full_document_before_change: document
            .get_document("fullDocumentBeforeChange")
            .ok()
            .cloned(),
        update_description: document.get_document("updateDescription").ok().cloned(),
        expanded: document.get_bool("expanded").unwrap_or(false),
        extra_fields: document
            .get_document("extraFields")
            .ok()
            .cloned()
            .unwrap_or_default(),
    })
}

fn change_stream_namespace_matches<R: CollectionResolver>(
    event: &InternalChangeEvent,
    spec: &ChangeStreamSpec,
    context: &PipelineContext<'_, R>,
) -> bool {
    if spec.all_changes_for_cluster {
        return true;
    }
    if event.database != context.database {
        return false;
    }
    match context.source_collection {
        Some(collection) => event.collection.as_deref() == Some(collection),
        None => true,
    }
}

fn materialize_change_stream_event(
    event: InternalChangeEvent,
    spec: &ChangeStreamSpec,
) -> Result<Document, QueryError> {
    let mut document = Document::new();
    document.insert("_id", Bson::Document(event.token.clone()));
    document.insert("operationType", event.operation_type.clone());
    document.insert("clusterTime", Bson::Timestamp(event.cluster_time));
    document.insert("wallTime", Bson::DateTime(event.wall_time));

    let mut namespace = doc! { "db": event.database.clone() };
    if let Some(collection) = &event.collection {
        namespace.insert("coll", collection.clone());
    }
    document.insert("ns", Bson::Document(namespace));

    if let Some(document_key) = event.document_key.clone() {
        document.insert("documentKey", Bson::Document(document_key));
    }
    if let Some(update_description) = event.update_description.clone() {
        document.insert("updateDescription", Bson::Document(update_description));
    }

    let include_full_document = match event.operation_type.as_str() {
        "insert" | "replace" => true,
        "update" => !matches!(spec.full_document, ChangeStreamFullDocumentMode::Default),
        _ => false,
    };
    if include_full_document {
        match (event.full_document.clone(), spec.full_document) {
            (Some(full_document), _) => {
                document.insert("fullDocument", Bson::Document(full_document));
            }
            (None, ChangeStreamFullDocumentMode::WhenAvailable) => {
                document.insert("fullDocument", Bson::Null);
            }
            (None, ChangeStreamFullDocumentMode::Required) => {
                return Err(QueryError::InvalidArgument(
                    "change stream event is missing a required fullDocument".to_string(),
                ));
            }
            (None, _) => {}
        }
    }

    match spec.full_document_before_change {
        ChangeStreamFullDocumentBeforeChangeMode::Off => {}
        ChangeStreamFullDocumentBeforeChangeMode::WhenAvailable => {
            if let Some(full_document_before_change) = event.full_document_before_change.clone() {
                document.insert(
                    "fullDocumentBeforeChange",
                    Bson::Document(full_document_before_change),
                );
            }
        }
        ChangeStreamFullDocumentBeforeChangeMode::Required => {
            let Some(full_document_before_change) = event.full_document_before_change.clone()
            else {
                return Err(QueryError::InvalidArgument(
                    "change stream event is missing a required fullDocumentBeforeChange"
                        .to_string(),
                ));
            };
            document.insert(
                "fullDocumentBeforeChange",
                Bson::Document(full_document_before_change),
            );
        }
    }

    for (field, value) in event.extra_fields {
        document.insert(field, value);
    }

    Ok(document)
}

fn union_with_documents<R: CollectionResolver>(
    mut documents: Vec<Document>,
    spec: &Bson,
    context: &PipelineContext<'_, R>,
) -> Result<Vec<Document>, QueryError> {
    let union_spec = parse_union_with_spec(spec, context.database)?;
    let union_database = union_spec.database.as_deref().unwrap_or(context.database);
    let input = match union_spec.collection.as_deref() {
        Some(collection) => context
            .resolver
            .resolve_collection(union_database, collection),
        None => Vec::new(),
    };
    let union_context = PipelineContext {
        inside_facet: false,
        database: union_database,
        source_collection: union_spec.collection.as_deref(),
        resolver: context.resolver,
        variables: context.variables.clone(),
    };
    let results = run_pipeline_with_context(input, &union_spec.pipeline, &union_context)?;
    documents.extend(results);
    Ok(documents)
}

fn lookup_documents<R: CollectionResolver>(
    documents: Vec<Document>,
    spec: &Bson,
    context: &PipelineContext<'_, R>,
) -> Result<Vec<Document>, QueryError> {
    let lookup_spec = parse_lookup_spec(spec)?;
    let lookup_database = lookup_spec.database.as_deref().unwrap_or(context.database);
    let initial_input = lookup_spec
        .collection
        .as_deref()
        .map(|collection| {
            context
                .resolver
                .resolve_collection(lookup_database, collection)
        })
        .unwrap_or_default();

    documents
        .into_iter()
        .map(|mut local_document| {
            let mut variables = context.variables.clone();
            for (name, expression) in &lookup_spec.let_variables {
                let value = eval_expression_with_variables(
                    &local_document,
                    expression,
                    &context.variables,
                )?;
                variables.insert(name.clone(), value);
            }

            let joined = if lookup_spec.collection.is_none()
                && lookup_spec.local_field.is_some()
                && lookup_spec.foreign_field.is_some()
            {
                let (source_stage, remaining_stages) = lookup_spec
                    .pipeline
                    .split_first()
                    .ok_or(QueryError::InvalidStage)?;
                let lookup_context = PipelineContext {
                    inside_facet: false,
                    database: lookup_database,
                    source_collection: lookup_spec.collection.as_deref(),
                    resolver: context.resolver,
                    variables: variables.clone(),
                };
                let source_documents = run_pipeline_with_context(
                    Vec::new(),
                    std::slice::from_ref(source_stage),
                    &lookup_context,
                )?;
                let matched = apply_lookup_join(
                    source_documents,
                    &local_document,
                    lookup_spec.local_field.as_deref(),
                    lookup_spec.foreign_field.as_deref(),
                )?;
                run_pipeline_with_context(matched, remaining_stages, &lookup_context)?
            } else {
                let matched = apply_lookup_join(
                    initial_input.clone(),
                    &local_document,
                    lookup_spec.local_field.as_deref(),
                    lookup_spec.foreign_field.as_deref(),
                )?;
                let lookup_context = PipelineContext {
                    inside_facet: false,
                    database: lookup_database,
                    source_collection: lookup_spec.collection.as_deref(),
                    resolver: context.resolver,
                    variables: variables.clone(),
                };
                run_pipeline_with_context(matched, &lookup_spec.pipeline, &lookup_context)?
            };

            set_path(
                &mut local_document,
                &lookup_spec.as_field,
                Bson::Array(joined.into_iter().map(Bson::Document).collect()),
            )
            .map_err(|_| QueryError::InvalidStructure)?;
            Ok(local_document)
        })
        .collect()
}

fn graph_lookup_documents<R: CollectionResolver>(
    documents: Vec<Document>,
    spec: &Bson,
    context: &PipelineContext<'_, R>,
) -> Result<Vec<Document>, QueryError> {
    let spec = parse_graph_lookup_spec(spec)?;
    let foreign_documents = context
        .resolver
        .resolve_collection(context.database, &spec.from_collection);

    documents
        .into_iter()
        .map(|mut local_document| {
            let start_with = eval_expression_with_variables(
                &local_document,
                &spec.start_with,
                &context.variables,
            )?;
            let mut frontier = graph_lookup_start_values(start_with);
            let mut results = Vec::new();
            let mut visited = BTreeSet::new();

            while let Some((value, depth)) = frontier.pop_front() {
                if spec.max_depth.is_some_and(|max_depth| depth > max_depth) {
                    continue;
                }

                for foreign in &foreign_documents {
                    if let Some(filter) = &spec.restrict_search_with_match {
                        if !document_matches_with_variables(foreign, filter, &context.variables)? {
                            continue;
                        }
                    }

                    if !join_values_match(
                        std::slice::from_ref(&value),
                        foreign,
                        &spec.connect_to_field,
                    ) {
                        continue;
                    }

                    let identity = bson::to_vec(foreign).map_err(|_| QueryError::InvalidStage)?;
                    if !visited.insert(identity) {
                        continue;
                    }

                    let mut result = foreign.clone();
                    if let Some(depth_field) = &spec.depth_field {
                        set_path(&mut result, depth_field, Bson::Int64(depth))
                            .map_err(|_| QueryError::InvalidStructure)?;
                    }

                    if spec.max_depth.is_none_or(|max_depth| depth < max_depth) {
                        for next in
                            graph_lookup_connect_from_values(foreign, &spec.connect_from_field)
                        {
                            frontier.push_back((next, depth + 1));
                        }
                    }

                    results.push(result);
                }
            }

            set_path(
                &mut local_document,
                &spec.as_field,
                Bson::Array(results.into_iter().map(Bson::Document).collect()),
            )
            .map_err(|_| QueryError::InvalidStructure)?;
            Ok(local_document)
        })
        .collect()
}

#[derive(Debug, Clone)]
struct GraphLookupStage {
    from_collection: String,
    start_with: Bson,
    connect_from_field: String,
    connect_to_field: String,
    as_field: String,
    max_depth: Option<i64>,
    depth_field: Option<String>,
    restrict_search_with_match: Option<Document>,
}

fn parse_graph_lookup_spec(spec: &Bson) -> Result<GraphLookupStage, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;

    let mut from_collection = None;
    let mut start_with = None;
    let mut connect_from_field = None;
    let mut connect_to_field = None;
    let mut as_field = None;
    let mut max_depth = None;
    let mut depth_field = None;
    let mut restrict_search_with_match = None;

    for (field, value) in document {
        match field.as_str() {
            "from" => {
                let collection = value.as_str().ok_or(QueryError::InvalidStage)?;
                if collection.is_empty() {
                    return Err(QueryError::InvalidStage);
                }
                from_collection = Some(collection.to_string());
            }
            "startWith" => start_with = Some(value.clone()),
            "connectFromField" => connect_from_field = Some(parse_field_name(value)?),
            "connectToField" => connect_to_field = Some(parse_field_name(value)?),
            "as" => as_field = Some(parse_field_name(value)?),
            "maxDepth" => {
                let depth = integer_value(value).ok_or(QueryError::InvalidStage)?;
                if depth < 0 {
                    return Err(QueryError::InvalidStage);
                }
                max_depth = Some(depth);
            }
            "depthField" => depth_field = Some(parse_field_name(value)?),
            "restrictSearchWithMatch" => {
                let filter = value.as_document().ok_or(QueryError::InvalidStage)?.clone();
                parse_filter(&filter)?;
                restrict_search_with_match = Some(filter);
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(GraphLookupStage {
        from_collection: from_collection.ok_or(QueryError::InvalidStage)?,
        start_with: start_with.ok_or(QueryError::InvalidStage)?,
        connect_from_field: connect_from_field.ok_or(QueryError::InvalidStage)?,
        connect_to_field: connect_to_field.ok_or(QueryError::InvalidStage)?,
        as_field: as_field.ok_or(QueryError::InvalidStage)?,
        max_depth,
        depth_field,
        restrict_search_with_match,
    })
}

fn graph_lookup_start_values(value: Bson) -> std::collections::VecDeque<(Bson, i64)> {
    let mut queue = std::collections::VecDeque::new();
    match value {
        Bson::Array(values) => {
            for value in values {
                queue.push_back((value, 0));
            }
        }
        value => queue.push_back((value, 0)),
    }
    queue
}

fn graph_lookup_connect_from_values(document: &Document, field: &str) -> Vec<Bson> {
    match mqlite_bson::lookup_path_owned(document, field) {
        Some(Bson::Array(values)) => values
            .into_iter()
            .filter(|value| !matches!(value, Bson::Null))
            .collect(),
        Some(Bson::Null) | None => Vec::new(),
        Some(value) => vec![value],
    }
}

#[derive(Debug, Clone)]
struct GeoNearStage {
    near: GeoPoint,
    distance_field: Option<String>,
    max_distance: Option<f64>,
    min_distance: Option<f64>,
    query: Option<Document>,
    spherical: bool,
    distance_multiplier: f64,
    include_locs: Option<String>,
    key: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct GeoPoint {
    x: f64,
    y: f64,
}

fn geo_near_documents(
    documents: Vec<Document>,
    spec: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let spec = parse_geo_near_spec(spec)?;
    let mut results = Vec::new();

    for mut document in documents {
        if let Some(query) = &spec.query {
            if !document_matches_with_variables(&document, query, variables)? {
                continue;
            }
        }

        let Some((location, location_bson)) =
            geo_point_for_document(&document, spec.key.as_deref())?
        else {
            continue;
        };
        let distance = if spec.spherical {
            spherical_distance(spec.near, location)
        } else {
            planar_distance(spec.near, location)
        };

        if spec.min_distance.is_some_and(|minimum| distance < minimum) {
            continue;
        }
        if spec.max_distance.is_some_and(|maximum| distance > maximum) {
            continue;
        }

        if let Some(distance_field) = &spec.distance_field {
            set_path(
                &mut document,
                distance_field,
                Bson::Double(distance * spec.distance_multiplier),
            )
            .map_err(|_| QueryError::InvalidStructure)?;
        }
        if let Some(include_locs) = &spec.include_locs {
            set_path(&mut document, include_locs, location_bson)
                .map_err(|_| QueryError::InvalidStructure)?;
        }

        results.push((distance, document));
    }

    results.sort_by(|(left_distance, _), (right_distance, _)| {
        left_distance
            .partial_cmp(right_distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(results.into_iter().map(|(_, document)| document).collect())
}

fn parse_geo_near_spec(spec: &Bson) -> Result<GeoNearStage, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;

    let mut near = None;
    let mut distance_field = None;
    let mut max_distance = None;
    let mut min_distance = None;
    let mut query = None;
    let mut spherical = false;
    let mut distance_multiplier = 1.0;
    let mut include_locs = None;
    let mut key = None;

    for (field, value) in document {
        match field.as_str() {
            "near" => near = Some(parse_geo_point(value)?),
            "distanceField" => distance_field = Some(parse_field_name(value)?),
            "maxDistance" => max_distance = Some(numeric_value(value)?),
            "minDistance" => min_distance = Some(numeric_value(value)?),
            "query" => {
                let filter = value.as_document().ok_or(QueryError::InvalidStage)?.clone();
                parse_filter(&filter)?;
                query = Some(filter);
            }
            "spherical" => spherical = value.as_bool().ok_or(QueryError::InvalidStage)?,
            "distanceMultiplier" => {
                distance_multiplier = numeric_value(value)?;
                if distance_multiplier < 0.0 {
                    return Err(QueryError::InvalidStage);
                }
            }
            "includeLocs" => include_locs = Some(parse_field_name(value)?),
            "key" => {
                let parsed = parse_field_name(value)?;
                if parsed.is_empty() {
                    return Err(QueryError::InvalidStage);
                }
                key = Some(parsed);
            }
            "limit" | "num" | "start" => return Err(QueryError::InvalidStage),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(GeoNearStage {
        near: near.ok_or(QueryError::InvalidStage)?,
        distance_field,
        max_distance,
        min_distance,
        query,
        spherical,
        distance_multiplier,
        include_locs,
        key,
    })
}

fn geo_point_for_document(
    document: &Document,
    key: Option<&str>,
) -> Result<Option<(GeoPoint, Bson)>, QueryError> {
    if let Some(key) = key {
        return lookup_path(document, key)
            .map(parse_geo_point_with_source)
            .transpose();
    }

    for (field, value) in document {
        if let Ok((point, source)) = parse_geo_point_with_source(value) {
            if field != "_id" {
                return Ok(Some((point, source)));
            }
        }
    }
    Ok(None)
}

fn parse_geo_point(value: &Bson) -> Result<GeoPoint, QueryError> {
    parse_geo_point_with_source(value).map(|(point, _)| point)
}

fn parse_geo_point_with_source(value: &Bson) -> Result<(GeoPoint, Bson), QueryError> {
    match value {
        Bson::Array(values) if values.len() == 2 => Ok((
            GeoPoint {
                x: numeric_value(&values[0])?,
                y: numeric_value(&values[1])?,
            },
            Bson::Array(values.clone()),
        )),
        Bson::Document(document) => {
            if document.get_str("type").ok() != Some("Point") {
                return Err(QueryError::InvalidStage);
            }
            let coordinates = document
                .get_array("coordinates")
                .map_err(|_| QueryError::InvalidStage)?;
            if coordinates.len() != 2 {
                return Err(QueryError::InvalidStage);
            }
            Ok((
                GeoPoint {
                    x: numeric_value(&coordinates[0])?,
                    y: numeric_value(&coordinates[1])?,
                },
                Bson::Document(document.clone()),
            ))
        }
        _ => Err(QueryError::InvalidStage),
    }
}

fn planar_distance(left: GeoPoint, right: GeoPoint) -> f64 {
    let delta_x = left.x - right.x;
    let delta_y = left.y - right.y;
    (delta_x * delta_x + delta_y * delta_y).sqrt()
}

fn spherical_distance(left: GeoPoint, right: GeoPoint) -> f64 {
    const EARTH_RADIUS_METERS: f64 = 6_378_137.0;

    let left_lat = left.y.to_radians();
    let left_lon = left.x.to_radians();
    let right_lat = right.y.to_radians();
    let right_lon = right.x.to_radians();

    let delta_lat = right_lat - left_lat;
    let delta_lon = right_lon - left_lon;
    let haversine = (delta_lat / 2.0).sin().powi(2)
        + left_lat.cos() * right_lat.cos() * (delta_lon / 2.0).sin().powi(2);
    let arc = 2.0 * haversine.sqrt().atan2((1.0 - haversine).sqrt());
    EARTH_RADIUS_METERS * arc
}

#[derive(Debug, Clone)]
struct LookupStage {
    database: Option<String>,
    collection: Option<String>,
    as_field: String,
    local_field: Option<String>,
    foreign_field: Option<String>,
    pipeline: Vec<Document>,
    let_variables: BTreeMap<String, Bson>,
}

fn parse_lookup_spec(spec: &Bson) -> Result<LookupStage, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;

    let mut database = None;
    let mut collection = None;
    let mut as_field = None;
    let mut local_field = None;
    let mut foreign_field = None;
    let mut pipeline = Vec::new();
    let mut has_pipeline = false;
    let mut let_variables = BTreeMap::new();
    let mut from_is_namespace_document = false;

    for (field, value) in document {
        match field.as_str() {
            "from" => match value {
                Bson::String(collection_name) => collection = Some(collection_name.clone()),
                Bson::Document(namespace) => {
                    from_is_namespace_document = true;
                    for (field, value) in namespace {
                        match field.as_str() {
                            "db" => {
                                database = Some(
                                    value.as_str().ok_or(QueryError::InvalidStage)?.to_string(),
                                );
                            }
                            "coll" => {
                                collection = Some(
                                    value.as_str().ok_or(QueryError::InvalidStage)?.to_string(),
                                );
                            }
                            _ => return Err(QueryError::InvalidStage),
                        }
                    }
                }
                _ => return Err(QueryError::InvalidStage),
            },
            "as" => {
                as_field = Some(value.as_str().ok_or(QueryError::InvalidStage)?.to_string());
            }
            "localField" => {
                local_field = Some(value.as_str().ok_or(QueryError::InvalidStage)?.to_string());
            }
            "foreignField" => {
                foreign_field = Some(value.as_str().ok_or(QueryError::InvalidStage)?.to_string());
            }
            "pipeline" => {
                has_pipeline = true;
                pipeline = value
                    .as_array()
                    .ok_or(QueryError::InvalidStage)?
                    .iter()
                    .map(|value| value.as_document().cloned().ok_or(QueryError::InvalidStage))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "let" => {
                for (name, expression) in value.as_document().ok_or(QueryError::InvalidStage)? {
                    let_variables.insert(name.clone(), expression.clone());
                }
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let starts_with_documents = pipeline
        .first()
        .and_then(|stage| stage.keys().next())
        .is_some_and(|stage_name| stage_name == "$documents");
    let has_local_field = local_field.is_some();
    let has_foreign_field = foreign_field.is_some();

    if as_field.is_none() {
        return Err(QueryError::InvalidStage);
    }
    if has_pipeline {
        if has_local_field != has_foreign_field {
            return Err(QueryError::InvalidStage);
        }
    } else {
        if !has_local_field || !has_foreign_field || !let_variables.is_empty() {
            return Err(QueryError::InvalidStage);
        }
    }
    if from_is_namespace_document && collection.is_none() {
        return Err(QueryError::InvalidStage);
    }
    if collection.is_none() && (!has_pipeline || !starts_with_documents) {
        return Err(QueryError::InvalidStage);
    }

    Ok(LookupStage {
        database,
        collection,
        as_field: as_field.expect("validated"),
        local_field,
        foreign_field,
        pipeline,
        let_variables,
    })
}

fn apply_lookup_join(
    documents: Vec<Document>,
    local_document: &Document,
    local_field: Option<&str>,
    foreign_field: Option<&str>,
) -> Result<Vec<Document>, QueryError> {
    let (Some(local_field), Some(foreign_field)) = (local_field, foreign_field) else {
        return Ok(documents);
    };

    let local_values = join_values_at_path(local_document, local_field);
    if local_values.is_empty() {
        return Ok(Vec::new());
    }

    Ok(documents
        .into_iter()
        .filter(|document| join_values_match(&local_values, document, foreign_field))
        .collect())
}

fn join_values_at_path(document: &Document, path: &str) -> Vec<Bson> {
    match mqlite_bson::lookup_path_owned(document, path) {
        Some(Bson::Array(values)) if values.is_empty() => Vec::new(),
        Some(Bson::Array(values)) => values,
        Some(value) => vec![value],
        None => vec![Bson::Null],
    }
}

fn join_values_match(
    local_values: &[Bson],
    foreign_document: &Document,
    foreign_field: &str,
) -> bool {
    let foreign_values = join_values_at_path(foreign_document, foreign_field);
    local_values.iter().any(|local| {
        foreign_values
            .iter()
            .any(|foreign| compare_bson(local, foreign).is_eq())
    })
}

struct UnionWithStage {
    database: Option<String>,
    collection: Option<String>,
    pipeline: Vec<Document>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeWhenMatched {
    Replace,
    Merge,
    KeepExisting,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeWhenNotMatched {
    Insert,
    Discard,
    Fail,
}

fn parse_union_with_spec(
    spec: &Bson,
    default_database: &str,
) -> Result<UnionWithStage, QueryError> {
    match spec {
        Bson::String(collection) => Ok(UnionWithStage {
            database: Some(default_database.to_string()),
            collection: Some(collection.clone()),
            pipeline: Vec::new(),
        }),
        Bson::Document(document) => {
            let mut database = None;
            let mut collection = None;
            let mut pipeline = None;

            for (field, value) in document {
                match field.as_str() {
                    "db" => {
                        database =
                            Some(value.as_str().ok_or(QueryError::InvalidStage)?.to_string());
                    }
                    "coll" => {
                        collection =
                            Some(value.as_str().ok_or(QueryError::InvalidStage)?.to_string());
                    }
                    "pipeline" => {
                        pipeline = Some(
                            value
                                .as_array()
                                .ok_or(QueryError::InvalidStage)?
                                .iter()
                                .map(|value| {
                                    value.as_document().cloned().ok_or(QueryError::InvalidStage)
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        );
                    }
                    _ => return Err(QueryError::InvalidStage),
                }
            }

            let pipeline = pipeline.unwrap_or_default();
            if collection.is_none() {
                let starts_with_documents = pipeline
                    .first()
                    .and_then(|stage| stage.keys().next())
                    .is_some_and(|stage_name| stage_name == "$documents");
                if !starts_with_documents {
                    return Err(QueryError::InvalidStage);
                }
            }

            Ok(UnionWithStage {
                database,
                collection,
                pipeline,
            })
        }
        _ => Err(QueryError::InvalidStage),
    }
}

fn parse_out_stage(spec: &Bson) -> Result<(), QueryError> {
    match spec {
        Bson::String(_) => Ok(()),
        Bson::Document(document) => {
            let mut has_collection = false;
            for (field, value) in document {
                match field.as_str() {
                    "db" => {
                        value.as_str().ok_or(QueryError::InvalidStage)?;
                    }
                    "coll" => {
                        value.as_str().ok_or(QueryError::InvalidStage)?;
                        has_collection = true;
                    }
                    "timeseries" => return Err(QueryError::InvalidStage),
                    _ => return Err(QueryError::InvalidStage),
                }
            }
            if !has_collection {
                return Err(QueryError::InvalidStage);
            }
            Ok(())
        }
        _ => Err(QueryError::InvalidStage),
    }
}

fn parse_merge_stage(spec: &Bson) -> Result<(), QueryError> {
    let mut target_database = None;
    let mut target_collection = None;
    let mut on_fields = vec!["_id".to_string()];
    let mut when_matched = MergeWhenMatched::Merge;
    let mut when_not_matched = MergeWhenNotMatched::Insert;

    match spec {
        Bson::String(collection) => {
            target_collection = Some(collection.clone());
        }
        Bson::Document(document) => {
            for (field, value) in document {
                match field.as_str() {
                    "into" => match value {
                        Bson::String(collection) => target_collection = Some(collection.clone()),
                        Bson::Document(namespace) => {
                            for (field, value) in namespace {
                                match field.as_str() {
                                    "db" => {
                                        target_database = Some(
                                            value
                                                .as_str()
                                                .ok_or(QueryError::InvalidStage)?
                                                .to_string(),
                                        );
                                    }
                                    "coll" => {
                                        target_collection = Some(
                                            value
                                                .as_str()
                                                .ok_or(QueryError::InvalidStage)?
                                                .to_string(),
                                        );
                                    }
                                    _ => return Err(QueryError::InvalidStage),
                                }
                            }
                        }
                        _ => return Err(QueryError::InvalidStage),
                    },
                    "on" => {
                        on_fields = match value {
                            Bson::String(field) => vec![field.clone()],
                            Bson::Array(fields) => fields
                                .iter()
                                .map(|field| {
                                    field
                                        .as_str()
                                        .map(str::to_string)
                                        .ok_or(QueryError::InvalidStage)
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                            _ => return Err(QueryError::InvalidStage),
                        };
                        if on_fields.is_empty() {
                            return Err(QueryError::InvalidStage);
                        }
                    }
                    "whenMatched" => {
                        when_matched = match value {
                            Bson::String(mode) => match mode.as_str() {
                                "replace" => MergeWhenMatched::Replace,
                                "merge" => MergeWhenMatched::Merge,
                                "keepExisting" => MergeWhenMatched::KeepExisting,
                                "fail" => MergeWhenMatched::Fail,
                                _ => return Err(QueryError::InvalidStage),
                            },
                            Bson::Array(_) => return Err(QueryError::InvalidStage),
                            _ => return Err(QueryError::InvalidStage),
                        };
                    }
                    "whenNotMatched" => {
                        when_not_matched = match value.as_str().ok_or(QueryError::InvalidStage)? {
                            "insert" => MergeWhenNotMatched::Insert,
                            "discard" => MergeWhenNotMatched::Discard,
                            "fail" => MergeWhenNotMatched::Fail,
                            _ => return Err(QueryError::InvalidStage),
                        };
                    }
                    "let" | "targetCollectionVersion" | "allowMergeOnNullishValues" => {
                        return Err(QueryError::InvalidStage);
                    }
                    _ => return Err(QueryError::InvalidStage),
                }
            }
        }
        _ => return Err(QueryError::InvalidStage),
    }

    let target_collection = target_collection.ok_or(QueryError::InvalidStage)?;
    if !matches!(
        (when_matched, when_not_matched),
        (MergeWhenMatched::Replace, MergeWhenNotMatched::Insert)
            | (MergeWhenMatched::Replace, MergeWhenNotMatched::Discard)
            | (MergeWhenMatched::Replace, MergeWhenNotMatched::Fail)
            | (MergeWhenMatched::Merge, MergeWhenNotMatched::Insert)
            | (MergeWhenMatched::Merge, MergeWhenNotMatched::Discard)
            | (MergeWhenMatched::Merge, MergeWhenNotMatched::Fail)
            | (MergeWhenMatched::KeepExisting, MergeWhenNotMatched::Insert)
            | (MergeWhenMatched::Fail, MergeWhenNotMatched::Insert)
    ) {
        return Err(QueryError::InvalidStage);
    }

    let _ = (
        target_database,
        target_collection,
        on_fields,
        when_matched,
        when_not_matched,
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DensifyDateUnit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DensifyNumericKind {
    Integer,
    Double,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum DensifyValue {
    Number(f64),
    DateTime(bson::DateTime),
}

#[derive(Debug, Clone, PartialEq)]
enum DensifyBounds {
    Full,
    Partition,
    Explicit(DensifyValue, DensifyValue),
}

#[derive(Debug, Clone, PartialEq)]
struct DensifyRange {
    step: f64,
    bounds: DensifyBounds,
    unit: Option<DensifyDateUnit>,
    numeric_kind: Option<DensifyNumericKind>,
}

#[derive(Debug, Clone, PartialEq)]
struct DensifySpec {
    field: String,
    partition_by_fields: Vec<String>,
    range: DensifyRange,
}

fn densify_documents(documents: Vec<Document>, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    let spec = parse_densify_spec(spec)?;
    let sort_spec = densify_sort_spec(&spec);
    let mut documents = documents;
    documents.sort_by(|left, right| compare_documents_by_sort(left, right, &sort_spec));

    if spec.partition_by_fields.is_empty() {
        densify_partition(documents, &spec, &[])
    } else {
        let mut results = Vec::new();
        let mut current_partition = Vec::new();
        let mut current_key: Option<Vec<Bson>> = None;

        for document in documents {
            let key = partition_key(&document, &spec.partition_by_fields);
            if current_key
                .as_ref()
                .is_some_and(|existing| existing != &key)
            {
                results.extend(densify_partition(
                    std::mem::take(&mut current_partition),
                    &spec,
                    current_key.as_deref().unwrap_or(&[]),
                )?);
            }
            current_key = Some(key);
            current_partition.push(document);
        }

        if !current_partition.is_empty() {
            results.extend(densify_partition(
                current_partition,
                &spec,
                current_key.as_deref().unwrap_or(&[]),
            )?);
        }

        Ok(results)
    }
}

fn densify_partition(
    documents: Vec<Document>,
    spec: &DensifySpec,
    partition_values: &[Bson],
) -> Result<Vec<Document>, QueryError> {
    let non_null_values = documents
        .iter()
        .filter_map(|document| densify_value_from_document(document, &spec.field).transpose())
        .collect::<Result<Vec<_>, _>>()?;

    let explicit_bounds = match spec.range.bounds {
        DensifyBounds::Full => {
            let Some(first) = non_null_values.first().copied() else {
                return Ok(documents);
            };
            let Some(last) = non_null_values.last().copied() else {
                return Ok(documents);
            };
            Some((first, last))
        }
        DensifyBounds::Partition => {
            let Some(first) = non_null_values.first().copied() else {
                return Ok(documents);
            };
            let Some(last) = non_null_values.last().copied() else {
                return Ok(documents);
            };
            Some((first, last))
        }
        DensifyBounds::Explicit(lower, upper) => Some((lower, upper)),
    };

    let Some((lower, upper)) = explicit_bounds else {
        return Ok(documents);
    };

    validate_densify_runtime_type(spec, &lower)?;
    validate_densify_runtime_type(spec, &upper)?;

    let mut results = Vec::new();
    let mut current_value = densify_sub(lower, spec.range.step, spec.range.unit)?;

    for document in documents {
        let Some(next_value) = densify_value_from_document(&document, &spec.field)? else {
            results.push(document);
            continue;
        };

        validate_densify_runtime_type(spec, &next_value)?;

        let next_step =
            densify_next_step_from_base(current_value, lower, spec.range.step, spec.range.unit)?;
        results.extend(generate_densified_documents(
            spec,
            partition_values,
            next_step,
            next_value,
            lower,
            upper,
        )?);
        results.push(document);
        current_value = next_value;
    }

    results.extend(generate_densified_documents(
        spec,
        partition_values,
        densify_next_step_from_base(current_value, lower, spec.range.step, spec.range.unit)?,
        upper,
        lower,
        upper,
    )?);

    Ok(results)
}

fn parse_densify_spec(spec: &Bson) -> Result<DensifySpec, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;

    let mut field = None;
    let mut partition_by_fields = Vec::new();
    let mut range = None;

    for (name, value) in document {
        match name.as_str() {
            "field" => field = Some(parse_field_name(value)?),
            "partitionByFields" => {
                partition_by_fields = value
                    .as_array()
                    .ok_or(QueryError::InvalidStage)?
                    .iter()
                    .map(parse_field_name)
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "range" => range = Some(parse_densify_range(value)?),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let field = field.ok_or(QueryError::InvalidStage)?;
    let range = range.ok_or(QueryError::InvalidStage)?;

    for partition_field in &partition_by_fields {
        if partition_field.starts_with(&field) {
            return Err(QueryError::InvalidStage);
        }
        if field.starts_with(partition_field) {
            return Err(QueryError::InvalidStage);
        }
    }

    if matches!(range.bounds, DensifyBounds::Partition) && partition_by_fields.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    Ok(DensifySpec {
        field,
        partition_by_fields,
        range,
    })
}

fn parse_densify_range(spec: &Bson) -> Result<DensifyRange, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;

    let mut step = None;
    let mut unit = None;
    let mut bounds = None;

    for (field, value) in document {
        match field.as_str() {
            "step" => step = Some(parse_densify_step(value)?),
            "unit" => {
                unit = Some(parse_densify_date_unit(
                    value.as_str().ok_or(QueryError::InvalidStage)?,
                )?)
            }
            "bounds" => bounds = Some(parse_densify_bounds(value)?),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let (step, numeric_kind) = step.ok_or(QueryError::InvalidStage)?;
    if step <= 0.0 {
        return Err(QueryError::InvalidStage);
    }

    if unit.is_some() && step.fract() != 0.0 {
        return Err(QueryError::InvalidStage);
    }

    let bounds = bounds.ok_or(QueryError::InvalidStage)?;
    match bounds {
        DensifyBounds::Explicit(DensifyValue::Number(lower), DensifyValue::Number(upper)) => {
            if unit.is_some() || lower > upper {
                return Err(QueryError::InvalidStage);
            }
        }
        DensifyBounds::Explicit(DensifyValue::DateTime(lower), DensifyValue::DateTime(upper)) => {
            if unit.is_none()
                || compare_bson(&Bson::DateTime(lower), &Bson::DateTime(upper)).is_gt()
            {
                return Err(QueryError::InvalidStage);
            }
        }
        DensifyBounds::Explicit(_, _) => return Err(QueryError::InvalidStage),
        DensifyBounds::Full | DensifyBounds::Partition => {}
    }

    Ok(DensifyRange {
        step,
        bounds,
        unit,
        numeric_kind,
    })
}

fn parse_densify_step(value: &Bson) -> Result<(f64, Option<DensifyNumericKind>), QueryError> {
    match value {
        Bson::Int32(step) => Ok((*step as f64, Some(DensifyNumericKind::Integer))),
        Bson::Int64(step) => Ok((*step as f64, Some(DensifyNumericKind::Integer))),
        Bson::Double(step) => Ok((*step, Some(DensifyNumericKind::Double))),
        Bson::Decimal128(step) => step
            .to_string()
            .parse::<f64>()
            .map(|value| (value, Some(DensifyNumericKind::Double)))
            .map_err(|_| QueryError::InvalidStage),
        _ => Err(QueryError::InvalidStage),
    }
}

fn parse_densify_bounds(value: &Bson) -> Result<DensifyBounds, QueryError> {
    match value {
        Bson::String(bounds) if bounds == "full" => Ok(DensifyBounds::Full),
        Bson::String(bounds) if bounds == "partition" => Ok(DensifyBounds::Partition),
        Bson::Array(bounds) if bounds.len() == 2 => {
            let lower = densify_value_from_bson(&bounds[0]).ok_or(QueryError::InvalidStage)?;
            let upper = densify_value_from_bson(&bounds[1]).ok_or(QueryError::InvalidStage)?;
            Ok(DensifyBounds::Explicit(lower, upper))
        }
        _ => Err(QueryError::InvalidStage),
    }
}

fn parse_densify_date_unit(value: &str) -> Result<DensifyDateUnit, QueryError> {
    match value {
        "millisecond" => Ok(DensifyDateUnit::Millisecond),
        "second" => Ok(DensifyDateUnit::Second),
        "minute" => Ok(DensifyDateUnit::Minute),
        "hour" => Ok(DensifyDateUnit::Hour),
        "day" => Ok(DensifyDateUnit::Day),
        "week" => Ok(DensifyDateUnit::Week),
        "month" => Ok(DensifyDateUnit::Month),
        "quarter" => Ok(DensifyDateUnit::Quarter),
        "year" => Ok(DensifyDateUnit::Year),
        _ => Err(QueryError::InvalidStage),
    }
}

fn parse_field_name(value: &Bson) -> Result<String, QueryError> {
    let field = value.as_str().ok_or(QueryError::InvalidStage)?;
    if field.is_empty() || field.starts_with('$') {
        return Err(QueryError::InvalidStage);
    }
    Ok(field.to_string())
}

fn partition_key(document: &Document, fields: &[String]) -> Vec<Bson> {
    fields
        .iter()
        .map(|field| lookup_path(document, field).cloned().unwrap_or(Bson::Null))
        .collect()
}

fn densify_sort_spec(spec: &DensifySpec) -> Document {
    let mut sort_spec = Document::new();
    for field in &spec.partition_by_fields {
        sort_spec.insert(field, 1);
    }
    if !sort_spec.contains_key(&spec.field) {
        sort_spec.insert(&spec.field, 1);
    }
    sort_spec
}

fn densify_value_from_document(
    document: &Document,
    field: &str,
) -> Result<Option<DensifyValue>, QueryError> {
    match lookup_path(document, field) {
        Some(Bson::Null) | None => Ok(None),
        Some(value) => densify_value_from_bson(value)
            .map(Some)
            .ok_or(QueryError::InvalidStage),
    }
}

fn densify_value_from_bson(value: &Bson) -> Option<DensifyValue> {
    match value {
        Bson::Int32(value) => Some(DensifyValue::Number(*value as f64)),
        Bson::Int64(value) => Some(DensifyValue::Number(*value as f64)),
        Bson::Double(value) => Some(DensifyValue::Number(*value)),
        Bson::Decimal128(value) => value.to_string().parse().ok().map(DensifyValue::Number),
        Bson::DateTime(value) => Some(DensifyValue::DateTime(*value)),
        _ => None,
    }
}

fn validate_densify_runtime_type(
    spec: &DensifySpec,
    value: &DensifyValue,
) -> Result<(), QueryError> {
    match (value, spec.range.unit) {
        (DensifyValue::DateTime(_), Some(_)) => Ok(()),
        (DensifyValue::Number(_), None) => Ok(()),
        _ => Err(QueryError::InvalidStage),
    }
}

fn densify_next_step_from_base(
    current: DensifyValue,
    base: DensifyValue,
    step: f64,
    unit: Option<DensifyDateUnit>,
) -> Result<DensifyValue, QueryError> {
    let mut next = base;
    while densify_compare(next, current).is_le() {
        next = densify_add(next, step, unit)?;
    }
    Ok(next)
}

fn generate_densified_documents(
    spec: &DensifySpec,
    partition_values: &[Bson],
    mut value: DensifyValue,
    stop: DensifyValue,
    lower: DensifyValue,
    upper: DensifyValue,
) -> Result<Vec<Document>, QueryError> {
    let mut generated = Vec::new();
    while densify_compare(value, stop).is_lt() {
        if densify_compare(value, lower).is_ge() && densify_compare(value, upper).is_lt() {
            let mut document = Document::new();
            for (field, partition_value) in
                spec.partition_by_fields.iter().zip(partition_values.iter())
            {
                set_path(&mut document, field, partition_value.clone())
                    .map_err(|_| QueryError::InvalidStructure)?;
            }
            set_path(
                &mut document,
                &spec.field,
                densify_value_to_bson(value, spec.range.numeric_kind),
            )
            .map_err(|_| QueryError::InvalidStructure)?;
            generated.push(document);
        }
        value = densify_add(value, spec.range.step, spec.range.unit)?;
    }
    Ok(generated)
}

fn densify_compare(left: DensifyValue, right: DensifyValue) -> std::cmp::Ordering {
    match (left, right) {
        (DensifyValue::Number(left), DensifyValue::Number(right)) => left
            .partial_cmp(&right)
            .unwrap_or(std::cmp::Ordering::Equal),
        (DensifyValue::DateTime(left), DensifyValue::DateTime(right)) => {
            compare_bson(&Bson::DateTime(left), &Bson::DateTime(right))
        }
        _ => std::cmp::Ordering::Equal,
    }
}

fn densify_add(
    value: DensifyValue,
    step: f64,
    unit: Option<DensifyDateUnit>,
) -> Result<DensifyValue, QueryError> {
    match (value, unit) {
        (DensifyValue::Number(value), None) => Ok(DensifyValue::Number(value + step)),
        (DensifyValue::DateTime(value), Some(unit)) => {
            let step = step as i64;
            let chrono_value = DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis())
                .ok_or(QueryError::InvalidStage)?;
            let next = match unit {
                DensifyDateUnit::Millisecond => chrono_value + Duration::milliseconds(step),
                DensifyDateUnit::Second => chrono_value + Duration::seconds(step),
                DensifyDateUnit::Minute => chrono_value + Duration::minutes(step),
                DensifyDateUnit::Hour => chrono_value + Duration::hours(step),
                DensifyDateUnit::Day => chrono_value + Duration::days(step),
                DensifyDateUnit::Week => chrono_value + Duration::weeks(step),
                DensifyDateUnit::Month => checked_add_months(chrono_value, step)?,
                DensifyDateUnit::Quarter => checked_add_months(chrono_value, step * 3)?,
                DensifyDateUnit::Year => chrono_value
                    .with_year(chrono_value.year() + step as i32)
                    .ok_or(QueryError::InvalidStage)?,
            };
            Ok(DensifyValue::DateTime(bson::DateTime::from_millis(
                next.timestamp_millis(),
            )))
        }
        _ => Err(QueryError::InvalidStage),
    }
}

fn densify_sub(
    value: DensifyValue,
    step: f64,
    unit: Option<DensifyDateUnit>,
) -> Result<DensifyValue, QueryError> {
    densify_add(value, -step, unit)
}

fn checked_add_months(value: DateTime<Utc>, months: i64) -> Result<DateTime<Utc>, QueryError> {
    if months >= 0 {
        value
            .checked_add_months(Months::new(months as u32))
            .ok_or(QueryError::InvalidStage)
    } else {
        value
            .checked_sub_months(Months::new((-months) as u32))
            .ok_or(QueryError::InvalidStage)
    }
}

fn densify_value_to_bson(value: DensifyValue, numeric_kind: Option<DensifyNumericKind>) -> Bson {
    match value {
        DensifyValue::Number(value) => match numeric_kind.unwrap_or(DensifyNumericKind::Double) {
            DensifyNumericKind::Integer => Bson::Int64(value as i64),
            DensifyNumericKind::Double => Bson::Double(value),
        },
        DensifyValue::DateTime(value) => Bson::DateTime(value),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FillPartitionBy {
    Fields(Vec<String>),
    Expression(Bson),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillMethod {
    Locf,
    Linear,
}

#[derive(Debug, Clone, PartialEq)]
enum FillOutput {
    Value(Bson),
    Method(FillMethod),
}

#[derive(Debug, Clone, PartialEq)]
struct FillSpec {
    sort_by: Option<Document>,
    partition_by: Option<FillPartitionBy>,
    outputs: Vec<(String, FillOutput)>,
}

fn fill_documents(
    mut documents: Vec<Document>,
    spec: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let spec = parse_fill_spec(spec)?;
    let has_methods = spec
        .outputs
        .iter()
        .any(|(_, output)| matches!(output, FillOutput::Method(_)));

    if has_methods {
        if spec.sort_by.is_some() {
            documents.sort_by(|left, right| compare_fill_documents(left, right, &spec, variables));
            apply_sorted_fill_methods(&mut documents, &spec, variables)?;
        } else {
            apply_streaming_fill_methods(&mut documents, &spec, variables)?;
        }
    }

    for (field, output) in &spec.outputs {
        let FillOutput::Value(expression) = output else {
            continue;
        };

        for document in &mut documents {
            if !fill_needs_value(document, field) {
                continue;
            }
            let value = eval_expression_with_variables(document, expression, variables)?;
            set_path(document, field, value).map_err(|_| QueryError::InvalidStructure)?;
        }
    }

    Ok(documents)
}

fn parse_fill_spec(spec: &Bson) -> Result<FillSpec, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;

    let mut sort_by = None;
    let mut partition_by_fields = None;
    let mut partition_by = None;
    let mut outputs = None;

    for (field, value) in document {
        match field.as_str() {
            "sortBy" => {
                let sort = value.as_document().ok_or(QueryError::InvalidStage)?.clone();
                validate_sort_spec(&sort)?;
                sort_by = Some(sort);
            }
            "partitionByFields" => {
                partition_by_fields = Some(
                    value
                        .as_array()
                        .ok_or(QueryError::InvalidStage)?
                        .iter()
                        .map(parse_field_name)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            "partitionBy" => {
                partition_by = Some(match value {
                    Bson::String(path) => {
                        if !path.starts_with('$') {
                            return Err(QueryError::InvalidStage);
                        }
                        FillPartitionBy::Expression(Bson::String(path.clone()))
                    }
                    Bson::Document(_) => FillPartitionBy::Expression(value.clone()),
                    _ => return Err(QueryError::InvalidStage),
                });
            }
            "output" => outputs = Some(parse_fill_outputs(value)?),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let outputs = outputs.ok_or(QueryError::InvalidStage)?;
    if outputs.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    if partition_by_fields.is_some() && partition_by.is_some() {
        return Err(QueryError::InvalidStage);
    }

    let partition_by = match (partition_by_fields, partition_by) {
        (Some(fields), None) => Some(FillPartitionBy::Fields(fields)),
        (None, Some(expr)) => Some(expr),
        (None, None) => None,
        (Some(_), Some(_)) => return Err(QueryError::InvalidStage),
    };

    if outputs
        .iter()
        .any(|(_, output)| matches!(output, FillOutput::Method(FillMethod::Linear)))
        && sort_by.is_none()
    {
        return Err(QueryError::InvalidStage);
    }

    Ok(FillSpec {
        sort_by,
        partition_by,
        outputs,
    })
}

fn parse_fill_outputs(value: &Bson) -> Result<Vec<(String, FillOutput)>, QueryError> {
    let document = value.as_document().ok_or(QueryError::InvalidStage)?;
    let mut outputs = Vec::new();

    for (field, spec) in document {
        if field.is_empty() || field.starts_with('$') {
            return Err(QueryError::InvalidStage);
        }
        let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
        if spec.len() != 1 {
            return Err(QueryError::InvalidStage);
        }

        let (kind, value) = spec.iter().next().expect("single fill output spec");
        let output = match kind.as_str() {
            "value" => FillOutput::Value(value.clone()),
            "method" => match value.as_str().ok_or(QueryError::InvalidStage)? {
                "locf" => FillOutput::Method(FillMethod::Locf),
                "linear" => FillOutput::Method(FillMethod::Linear),
                _ => return Err(QueryError::InvalidStage),
            },
            _ => return Err(QueryError::InvalidStage),
        };
        outputs.push((field.clone(), output));
    }

    Ok(outputs)
}

fn compare_fill_documents(
    left: &Document,
    right: &Document,
    spec: &FillSpec,
    variables: &BTreeMap<String, Bson>,
) -> std::cmp::Ordering {
    if let Some(partition_by) = &spec.partition_by {
        let left_key = fill_partition_key(left, partition_by, variables);
        let right_key = fill_partition_key(right, partition_by, variables);
        let ordering = match (left_key, right_key) {
            (Ok(left_key), Ok(right_key)) => compare_bson(&left_key, &right_key),
            _ => std::cmp::Ordering::Equal,
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }

    let sort_by = spec.sort_by.as_ref().expect("sorted fill requires sortBy");
    compare_documents_by_sort(left, right, sort_by)
}

fn apply_streaming_fill_methods(
    documents: &mut [Document],
    spec: &FillSpec,
    variables: &BTreeMap<String, Bson>,
) -> Result<(), QueryError> {
    let method_fields = spec
        .outputs
        .iter()
        .filter_map(|(field, output)| match output {
            FillOutput::Method(method) => Some((field.as_str(), *method)),
            FillOutput::Value(_) => None,
        })
        .collect::<Vec<_>>();

    let mut states: Vec<(Bson, BTreeMap<String, Bson>)> = Vec::new();

    for document in documents {
        let partition_key = match &spec.partition_by {
            Some(partition_by) => fill_partition_key(document, partition_by, variables)?,
            None => Bson::Null,
        };

        let state_index = states
            .iter()
            .position(|(key, _)| compare_bson(key, &partition_key).is_eq())
            .unwrap_or_else(|| {
                states.push((partition_key.clone(), BTreeMap::new()));
                states.len() - 1
            });
        let (_, last_seen) = &mut states[state_index];

        for (field, method) in &method_fields {
            match method {
                FillMethod::Locf => {
                    if let Some(value) = lookup_path(document, field) {
                        if !matches!(value, Bson::Null) {
                            last_seen.insert((*field).to_string(), value.clone());
                            continue;
                        }
                    }
                    if let Some(previous) = last_seen.get(*field) {
                        set_path(document, field, previous.clone())
                            .map_err(|_| QueryError::InvalidStructure)?;
                    }
                }
                FillMethod::Linear => return Err(QueryError::InvalidStage),
            }
        }
    }

    Ok(())
}

fn apply_sorted_fill_methods(
    documents: &mut [Document],
    spec: &FillSpec,
    variables: &BTreeMap<String, Bson>,
) -> Result<(), QueryError> {
    let mut start = 0;
    while start < documents.len() {
        let end = fill_partition_end(documents, spec, variables, start)?;
        for (field, output) in &spec.outputs {
            match output {
                FillOutput::Method(FillMethod::Locf) => {
                    fill_locf_partition(&mut documents[start..end], field)?
                }
                FillOutput::Method(FillMethod::Linear) => {
                    let sort_by = spec.sort_by.as_ref().expect("validated");
                    fill_linear_partition(&mut documents[start..end], field, sort_by)?;
                }
                FillOutput::Value(_) => {}
            }
        }
        start = end;
    }
    Ok(())
}

fn fill_partition_end(
    documents: &[Document],
    spec: &FillSpec,
    variables: &BTreeMap<String, Bson>,
    start: usize,
) -> Result<usize, QueryError> {
    let Some(partition_by) = &spec.partition_by else {
        return Ok(documents.len());
    };

    let first_key = fill_partition_key(&documents[start], partition_by, variables)?;
    let mut end = start + 1;
    while end < documents.len() {
        let key = fill_partition_key(&documents[end], partition_by, variables)?;
        if !compare_bson(&first_key, &key).is_eq() {
            break;
        }
        end += 1;
    }
    Ok(end)
}

fn fill_locf_partition(documents: &mut [Document], field: &str) -> Result<(), QueryError> {
    let mut last_seen = None;
    for document in documents {
        if let Some(value) = lookup_path(document, field) {
            if !matches!(value, Bson::Null) {
                last_seen = Some(value.clone());
                continue;
            }
        }
        if let Some(previous) = &last_seen {
            set_path(document, field, previous.clone())
                .map_err(|_| QueryError::InvalidStructure)?;
        }
    }
    Ok(())
}

fn fill_linear_partition(
    documents: &mut [Document],
    field: &str,
    sort_by: &Document,
) -> Result<(), QueryError> {
    let mut previous_anchor: Option<(usize, f64, f64)> = None;

    for index in 0..documents.len() {
        let sort_value = fill_sort_scalar(&documents[index], sort_by)?;
        let current = lookup_path(&documents[index], field);

        let Some(current) = current else {
            continue;
        };
        if matches!(current, Bson::Null) {
            continue;
        }

        let current_value = numeric_value(current)?;
        if let Some((anchor_index, anchor_sort, anchor_value)) = previous_anchor {
            if index > anchor_index + 1 {
                let span = sort_value - anchor_sort;
                if span == 0.0 {
                    return Err(QueryError::InvalidStage);
                }

                for document in documents
                    .iter_mut()
                    .enumerate()
                    .skip(anchor_index + 1)
                    .take(index - anchor_index - 1)
                {
                    let (_, document) = document;
                    if !fill_needs_value(document, field) {
                        continue;
                    }
                    let missing_sort = fill_sort_scalar(document, sort_by)?;
                    let ratio = (missing_sort - anchor_sort) / span;
                    let filled_value = anchor_value + ((current_value - anchor_value) * ratio);
                    set_path(document, field, number_bson(filled_value))
                        .map_err(|_| QueryError::InvalidStructure)?;
                }
            }
        }
        previous_anchor = Some((index, sort_value, current_value));
    }

    Ok(())
}

fn fill_sort_scalar(document: &Document, sort_by: &Document) -> Result<f64, QueryError> {
    let (field, _) = sort_by.iter().next().ok_or(QueryError::InvalidStage)?;
    let value = lookup_path(document, field).ok_or(QueryError::InvalidStage)?;
    match value {
        Bson::DateTime(value) => Ok(value.timestamp_millis() as f64),
        _ => numeric_value(value),
    }
}

fn fill_partition_key(
    document: &Document,
    partition_by: &FillPartitionBy,
    variables: &BTreeMap<String, Bson>,
) -> Result<Bson, QueryError> {
    match partition_by {
        FillPartitionBy::Fields(fields) => Ok(Bson::Array(
            fields
                .iter()
                .map(|field| lookup_path(document, field).cloned().unwrap_or(Bson::Null))
                .collect(),
        )),
        FillPartitionBy::Expression(expression) => {
            eval_expression_with_variables(document, expression, variables)
        }
    }
}

fn fill_needs_value(document: &Document, field: &str) -> bool {
    matches!(lookup_path(document, field), None | Some(Bson::Null))
}

#[derive(Debug, Clone)]
struct SetWindowFieldsStage {
    partition_by: Option<Bson>,
    sort_by: Option<Document>,
    output_fields: Vec<(String, WindowOutputSpec)>,
}

#[derive(Debug, Clone)]
struct WindowOutputSpec {
    function: WindowFunctionSpec,
    window: Option<WindowBounds>,
}

#[derive(Debug, Clone)]
enum WindowFunctionSpec {
    Accumulator {
        accumulator: WindowAccumulatorKind,
        expression: Bson,
    },
    Count,
    Rank(WindowRankKind),
    Shift(WindowShiftSpec),
    Locf(Bson),
    LinearFill(Bson),
}

#[derive(Debug, Clone, Copy)]
enum WindowAccumulatorKind {
    Sum,
    Avg,
    First,
    Last,
    Push,
    AddToSet,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy)]
enum WindowRankKind {
    DocumentNumber,
    Rank,
    DenseRank,
}

#[derive(Debug, Clone)]
struct WindowShiftSpec {
    output: Bson,
    by: i64,
    default: Option<Bson>,
}

#[derive(Debug, Clone)]
enum WindowBounds {
    Documents(WindowDocumentBound, WindowDocumentBound),
    Range(WindowRangeBound, WindowRangeBound, Option<DensifyDateUnit>),
}

#[derive(Debug, Clone, Copy)]
enum WindowDocumentBound {
    Unbounded,
    Current,
    Offset(i64),
}

#[derive(Debug, Clone, Copy)]
enum WindowRangeBound {
    Unbounded,
    Current,
    Offset(f64),
}

#[derive(Debug, Clone)]
struct WindowRow {
    document: Document,
    partition_key: Bson,
    original_position: usize,
}

fn set_window_fields_documents(
    documents: Vec<Document>,
    spec: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let stage = parse_set_window_fields_spec(spec)?;
    let mut rows = documents
        .into_iter()
        .enumerate()
        .map(|(original_position, document)| {
            let partition_key = match &stage.partition_by {
                Some(expression) => {
                    let value = eval_expression_with_variables(&document, expression, variables)?;
                    if matches!(value, Bson::Array(_)) {
                        return Err(QueryError::InvalidStage);
                    }
                    value
                }
                None => Bson::Null,
            };
            Ok(WindowRow {
                document,
                partition_key,
                original_position,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if stage.partition_by.is_some() || stage.sort_by.is_some() {
        rows.sort_by(|left, right| compare_window_rows(left, right, &stage.sort_by));
    }

    if stage.output_fields.is_empty() {
        return Ok(rows.into_iter().map(|row| row.document).collect());
    }

    let mut start = 0_usize;
    while start < rows.len() {
        let end = if stage.partition_by.is_some() {
            window_partition_end(&rows, start)
        } else {
            rows.len()
        };
        apply_window_partition(&mut rows[start..end], &stage, variables)?;
        start = end;
    }

    Ok(rows.into_iter().map(|row| row.document).collect())
}

fn parse_set_window_fields_spec(spec: &Bson) -> Result<SetWindowFieldsStage, QueryError> {
    let document = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut partition_by = None;
    let mut sort_by = None;
    let mut raw_output = None;

    for (field, value) in document {
        match field.as_str() {
            "partitionBy" => {
                validate_expression(value)?;
                partition_by = Some(value.clone());
            }
            "sortBy" => {
                let sort = value.as_document().ok_or(QueryError::InvalidStage)?.clone();
                validate_sort_spec(&sort)?;
                sort_by = Some(sort);
            }
            "output" => raw_output = Some(value.as_document().ok_or(QueryError::InvalidStage)?),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let raw_output = raw_output.ok_or(QueryError::InvalidStage)?;
    validate_window_output_paths(raw_output)?;
    let output_fields = raw_output
        .iter()
        .map(|(field, value)| {
            parse_window_output_spec(field, value, sort_by.as_ref())
                .map(|spec| (field.clone(), spec))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SetWindowFieldsStage {
        partition_by,
        sort_by,
        output_fields,
    })
}

fn validate_window_output_paths(output: &Document) -> Result<(), QueryError> {
    let mut existing = Vec::new();
    for field in output.keys() {
        if field.is_empty()
            || field.starts_with('$')
            || field.contains('\0')
            || field
                .split('.')
                .any(|segment| segment.is_empty() || segment.starts_with('$'))
        {
            return Err(QueryError::InvalidStage);
        }

        if existing.iter().any(|prior: &String| {
            field == prior
                || field.starts_with(&format!("{prior}."))
                || prior.starts_with(&format!("{field}."))
        }) {
            return Err(QueryError::InvalidStage);
        }
        existing.push(field.clone());
    }

    Ok(())
}

fn parse_window_output_spec(
    field: &str,
    value: &Bson,
    sort_by: Option<&Document>,
) -> Result<WindowOutputSpec, QueryError> {
    if field.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    let spec = value.as_document().ok_or(QueryError::InvalidStage)?;
    if spec.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    let mut function = None;
    let mut window = None;
    for (key, value) in spec {
        if key == "window" {
            window = Some(parse_window_bounds(value, sort_by)?);
            continue;
        }

        if !key.starts_with('$') || function.is_some() {
            return Err(QueryError::InvalidStage);
        }
        function = Some(parse_window_function_spec(key, value, sort_by)?);
    }

    let function = function.ok_or(QueryError::InvalidStage)?;
    match &function {
        WindowFunctionSpec::Rank(_) | WindowFunctionSpec::Shift(_) => {
            if window.is_some() {
                return Err(QueryError::InvalidStage);
            }
        }
        WindowFunctionSpec::Locf(_) | WindowFunctionSpec::LinearFill(_) => {
            if window.is_some() {
                return Err(QueryError::InvalidStage);
            }
        }
        WindowFunctionSpec::Count | WindowFunctionSpec::Accumulator { .. } => {}
    }

    Ok(WindowOutputSpec { function, window })
}

fn parse_window_function_spec(
    name: &str,
    value: &Bson,
    sort_by: Option<&Document>,
) -> Result<WindowFunctionSpec, QueryError> {
    match name {
        "$sum" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::Sum,
                expression: value.clone(),
            })
        }
        "$avg" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::Avg,
                expression: value.clone(),
            })
        }
        "$first" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::First,
                expression: value.clone(),
            })
        }
        "$last" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::Last,
                expression: value.clone(),
            })
        }
        "$push" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::Push,
                expression: value.clone(),
            })
        }
        "$addToSet" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::AddToSet,
                expression: value.clone(),
            })
        }
        "$min" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::Min,
                expression: value.clone(),
            })
        }
        "$max" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Accumulator {
                accumulator: WindowAccumulatorKind::Max,
                expression: value.clone(),
            })
        }
        "$count" => {
            let document = value.as_document().ok_or(QueryError::InvalidStage)?;
            if !document.is_empty() {
                return Err(QueryError::InvalidStage);
            }
            Ok(WindowFunctionSpec::Count)
        }
        "$documentNumber" => {
            require_empty_window_function_args(value, sort_by)?;
            Ok(WindowFunctionSpec::Rank(WindowRankKind::DocumentNumber))
        }
        "$rank" => {
            require_empty_window_function_args(value, sort_by)?;
            Ok(WindowFunctionSpec::Rank(WindowRankKind::Rank))
        }
        "$denseRank" => {
            require_empty_window_function_args(value, sort_by)?;
            Ok(WindowFunctionSpec::Rank(WindowRankKind::DenseRank))
        }
        "$shift" => Ok(WindowFunctionSpec::Shift(parse_shift_spec(value, sort_by)?)),
        "$locf" => {
            validate_expression(value)?;
            Ok(WindowFunctionSpec::Locf(value.clone()))
        }
        "$linearFill" => {
            validate_expression(value)?;
            if single_sort_field(sort_by).is_none() {
                return Err(QueryError::InvalidStage);
            }
            Ok(WindowFunctionSpec::LinearFill(value.clone()))
        }
        _ => Err(QueryError::InvalidStage),
    }
}

fn require_empty_window_function_args(
    value: &Bson,
    sort_by: Option<&Document>,
) -> Result<(), QueryError> {
    let document = value.as_document().ok_or(QueryError::InvalidStage)?;
    if !document.is_empty() || sort_by.is_none() {
        return Err(QueryError::InvalidStage);
    }
    Ok(())
}

fn parse_shift_spec(
    value: &Bson,
    sort_by: Option<&Document>,
) -> Result<WindowShiftSpec, QueryError> {
    if sort_by.is_none() {
        return Err(QueryError::InvalidStage);
    }
    let document = value.as_document().ok_or(QueryError::InvalidStage)?;
    let mut output = None;
    let mut by = None;
    let mut default = None;

    for (field, value) in document {
        match field.as_str() {
            "output" => {
                validate_expression(value)?;
                output = Some(value.clone());
            }
            "by" => by = Some(integer_value(value).ok_or(QueryError::InvalidStage)?),
            "default" => {
                validate_expression(value)?;
                default = Some(value.clone());
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(WindowShiftSpec {
        output: output.ok_or(QueryError::InvalidStage)?,
        by: by.ok_or(QueryError::InvalidStage)?,
        default,
    })
}

fn parse_window_bounds(
    value: &Bson,
    sort_by: Option<&Document>,
) -> Result<WindowBounds, QueryError> {
    let document = value.as_document().ok_or(QueryError::InvalidStage)?;
    if document.is_empty() || document.len() > 2 {
        return Err(QueryError::InvalidStage);
    }

    match (document.get("documents"), document.get("range")) {
        (Some(bounds), None) if document.len() == 1 => {
            let (lower, upper) = parse_document_window_bounds(bounds)?;
            Ok(WindowBounds::Documents(lower, upper))
        }
        (None, Some(bounds)) => parse_range_window_bounds(bounds, document.get("unit"), sort_by),
        _ => Err(QueryError::InvalidStage),
    }
}

fn parse_document_window_bounds(
    value: &Bson,
) -> Result<(WindowDocumentBound, WindowDocumentBound), QueryError> {
    let bounds = value.as_array().ok_or(QueryError::InvalidStage)?;
    if bounds.len() != 2 {
        return Err(QueryError::InvalidStage);
    }
    let lower = parse_document_window_bound(&bounds[0])?;
    let upper = parse_document_window_bound(&bounds[1])?;
    if document_bound_rank(lower, true) > document_bound_rank(upper, false) {
        return Err(QueryError::InvalidStage);
    }
    Ok((lower, upper))
}

fn parse_document_window_bound(value: &Bson) -> Result<WindowDocumentBound, QueryError> {
    match value {
        Bson::String(keyword) if keyword == "unbounded" => Ok(WindowDocumentBound::Unbounded),
        Bson::String(keyword) if keyword == "current" => Ok(WindowDocumentBound::Current),
        _ => integer_value(value)
            .map(WindowDocumentBound::Offset)
            .ok_or(QueryError::InvalidStage),
    }
}

fn parse_range_window_bounds(
    value: &Bson,
    unit: Option<&Bson>,
    sort_by: Option<&Document>,
) -> Result<WindowBounds, QueryError> {
    if single_sort_field(sort_by).is_none() {
        return Err(QueryError::InvalidStage);
    }
    let bounds = value.as_array().ok_or(QueryError::InvalidStage)?;
    if bounds.len() != 2 {
        return Err(QueryError::InvalidStage);
    }

    let unit = unit
        .map(|value| parse_densify_date_unit(value.as_str().ok_or(QueryError::InvalidStage)?))
        .transpose()?;

    let lower = parse_range_window_bound(&bounds[0], unit.is_some())?;
    let upper = parse_range_window_bound(&bounds[1], unit.is_some())?;
    if range_bound_rank(lower, true) > range_bound_rank(upper, false) {
        return Err(QueryError::InvalidStage);
    }

    Ok(WindowBounds::Range(lower, upper, unit))
}

fn parse_range_window_bound(
    value: &Bson,
    integral_only: bool,
) -> Result<WindowRangeBound, QueryError> {
    match value {
        Bson::String(keyword) if keyword == "unbounded" => Ok(WindowRangeBound::Unbounded),
        Bson::String(keyword) if keyword == "current" => Ok(WindowRangeBound::Current),
        _ if integral_only => integer_value(value)
            .map(|value| WindowRangeBound::Offset(value as f64))
            .ok_or(QueryError::InvalidStage),
        _ => Ok(WindowRangeBound::Offset(numeric_value(value)?)),
    }
}

fn document_bound_rank(bound: WindowDocumentBound, lower: bool) -> f64 {
    match bound {
        WindowDocumentBound::Unbounded => {
            if lower {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }
        }
        WindowDocumentBound::Current => 0.0,
        WindowDocumentBound::Offset(value) => value as f64,
    }
}

fn range_bound_rank(bound: WindowRangeBound, lower: bool) -> f64 {
    match bound {
        WindowRangeBound::Unbounded => {
            if lower {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }
        }
        WindowRangeBound::Current => 0.0,
        WindowRangeBound::Offset(value) => value,
    }
}

fn compare_window_rows(
    left: &WindowRow,
    right: &WindowRow,
    sort_by: &Option<Document>,
) -> std::cmp::Ordering {
    let partition = compare_bson(&left.partition_key, &right.partition_key);
    if partition != std::cmp::Ordering::Equal {
        return partition;
    }

    if let Some(sort_by) = sort_by {
        let sort = compare_documents_by_sort(&left.document, &right.document, sort_by);
        if sort != std::cmp::Ordering::Equal {
            return sort;
        }
    }

    left.original_position.cmp(&right.original_position)
}

fn window_partition_end(rows: &[WindowRow], start: usize) -> usize {
    let mut end = start + 1;
    while end < rows.len()
        && compare_bson(&rows[start].partition_key, &rows[end].partition_key).is_eq()
    {
        end += 1;
    }
    end
}

fn apply_window_partition(
    rows: &mut [WindowRow],
    stage: &SetWindowFieldsStage,
    variables: &BTreeMap<String, Bson>,
) -> Result<(), QueryError> {
    let originals = rows
        .iter()
        .map(|row| row.document.clone())
        .collect::<Vec<_>>();

    for (field, output) in &stage.output_fields {
        let values = evaluate_window_output(&originals, output, stage.sort_by.as_ref(), variables)?;
        for (row, value) in rows.iter_mut().zip(values) {
            set_path(&mut row.document, field, value).map_err(|_| QueryError::InvalidStructure)?;
        }
    }

    Ok(())
}

fn evaluate_window_output(
    documents: &[Document],
    output: &WindowOutputSpec,
    sort_by: Option<&Document>,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Bson>, QueryError> {
    match &output.function {
        WindowFunctionSpec::Accumulator {
            accumulator,
            expression,
        } => (0..documents.len())
            .map(|index| {
                let indices = window_indices(documents, index, output.window.as_ref(), sort_by)?;
                evaluate_window_accumulator(
                    documents,
                    &indices,
                    *accumulator,
                    expression,
                    variables,
                )
            })
            .collect(),
        WindowFunctionSpec::Count => (0..documents.len())
            .map(|index| {
                let indices = window_indices(documents, index, output.window.as_ref(), sort_by)?;
                Ok(Bson::Int64(indices.len() as i64))
            })
            .collect(),
        WindowFunctionSpec::Rank(kind) => compute_rank_values(documents, sort_by, *kind),
        WindowFunctionSpec::Shift(spec) => compute_shift_values(documents, spec, variables),
        WindowFunctionSpec::Locf(expression) => {
            compute_locf_values(documents, expression, variables)
        }
        WindowFunctionSpec::LinearFill(expression) => {
            compute_linear_fill_values(documents, expression, sort_by, variables)
        }
    }
}

fn window_indices(
    documents: &[Document],
    index: usize,
    explicit_window: Option<&WindowBounds>,
    sort_by: Option<&Document>,
) -> Result<Vec<usize>, QueryError> {
    let bounds = match explicit_window {
        Some(bounds) => bounds,
        None => {
            if sort_by.is_some() {
                return document_window_indices(
                    documents.len(),
                    index,
                    WindowDocumentBound::Unbounded,
                    WindowDocumentBound::Current,
                );
            }
            return Ok((0..documents.len()).collect());
        }
    };

    match bounds {
        WindowBounds::Documents(lower, upper) => {
            if sort_by.is_none()
                && !matches!(
                    (lower, upper),
                    (
                        WindowDocumentBound::Unbounded,
                        WindowDocumentBound::Unbounded
                    )
                )
            {
                return Err(QueryError::InvalidStage);
            }
            document_window_indices(documents.len(), index, *lower, *upper)
        }
        WindowBounds::Range(lower, upper, unit) => {
            range_window_indices(documents, index, sort_by, *lower, *upper, *unit)
        }
    }
}

fn document_window_indices(
    len: usize,
    index: usize,
    lower: WindowDocumentBound,
    upper: WindowDocumentBound,
) -> Result<Vec<usize>, QueryError> {
    let lower = match lower {
        WindowDocumentBound::Unbounded => 0_i64,
        WindowDocumentBound::Current => index as i64,
        WindowDocumentBound::Offset(offset) => index as i64 + offset,
    };
    let upper = match upper {
        WindowDocumentBound::Unbounded => len as i64 - 1,
        WindowDocumentBound::Current => index as i64,
        WindowDocumentBound::Offset(offset) => index as i64 + offset,
    };
    if upper < 0 || lower >= len as i64 {
        return Ok(Vec::new());
    }

    let start = lower.max(0) as usize;
    let end = upper.min(len as i64 - 1);
    if start as i64 > end {
        return Ok(Vec::new());
    }

    Ok((start..=end as usize).collect())
}

fn range_window_indices(
    documents: &[Document],
    index: usize,
    sort_by: Option<&Document>,
    lower: WindowRangeBound,
    upper: WindowRangeBound,
    unit: Option<DensifyDateUnit>,
) -> Result<Vec<usize>, QueryError> {
    let (field, _) = single_sort_field(sort_by).ok_or(QueryError::InvalidStage)?;
    let current_value = lookup_path(&documents[index], field).ok_or(QueryError::InvalidStage)?;
    let current_value = densify_value_from_bson(current_value).ok_or(QueryError::InvalidStage)?;

    match (current_value, unit) {
        (DensifyValue::Number(current), None) => {
            let lower_value = match lower {
                WindowRangeBound::Unbounded => None,
                WindowRangeBound::Current => Some(current),
                WindowRangeBound::Offset(offset) => Some(current + offset),
            };
            let upper_value = match upper {
                WindowRangeBound::Unbounded => None,
                WindowRangeBound::Current => Some(current),
                WindowRangeBound::Offset(offset) => Some(current + offset),
            };
            Ok(documents
                .iter()
                .enumerate()
                .filter_map(|(candidate_index, document)| {
                    let value = lookup_path(document, field)
                        .and_then(densify_value_from_bson)
                        .and_then(|value| match value {
                            DensifyValue::Number(value) => Some(value),
                            DensifyValue::DateTime(_) => None,
                        })?;
                    let within_lower = lower_value.is_none_or(|lower| value >= lower);
                    let within_upper = upper_value.is_none_or(|upper| value <= upper);
                    (within_lower && within_upper).then_some(candidate_index)
                })
                .collect::<Vec<_>>())
        }
        (DensifyValue::DateTime(current), Some(unit)) => {
            let lower_value = match lower {
                WindowRangeBound::Unbounded => None,
                WindowRangeBound::Current => Some(current),
                WindowRangeBound::Offset(offset) => {
                    let value = densify_add(DensifyValue::DateTime(current), offset, Some(unit))?;
                    match value {
                        DensifyValue::DateTime(value) => Some(value),
                        DensifyValue::Number(_) => return Err(QueryError::InvalidStage),
                    }
                }
            };
            let upper_value = match upper {
                WindowRangeBound::Unbounded => None,
                WindowRangeBound::Current => Some(current),
                WindowRangeBound::Offset(offset) => {
                    let value = densify_add(DensifyValue::DateTime(current), offset, Some(unit))?;
                    match value {
                        DensifyValue::DateTime(value) => Some(value),
                        DensifyValue::Number(_) => return Err(QueryError::InvalidStage),
                    }
                }
            };
            Ok(documents
                .iter()
                .enumerate()
                .filter_map(|(candidate_index, document)| {
                    let value = lookup_path(document, field)
                        .and_then(densify_value_from_bson)
                        .and_then(|value| match value {
                            DensifyValue::DateTime(value) => Some(value),
                            DensifyValue::Number(_) => None,
                        })?;
                    let within_lower = lower_value.as_ref().is_none_or(|lower| {
                        compare_bson(&Bson::DateTime(value), &Bson::DateTime(*lower)).is_ge()
                    });
                    let within_upper = upper_value.as_ref().is_none_or(|upper| {
                        compare_bson(&Bson::DateTime(value), &Bson::DateTime(*upper)).is_le()
                    });
                    (within_lower && within_upper).then_some(candidate_index)
                })
                .collect::<Vec<_>>())
        }
        _ => Err(QueryError::InvalidStage),
    }
}

fn evaluate_window_accumulator(
    documents: &[Document],
    indices: &[usize],
    accumulator: WindowAccumulatorKind,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Bson, QueryError> {
    match accumulator {
        WindowAccumulatorKind::Sum => {
            let mut total = 0.0;
            for &index in indices {
                if let Some(value) =
                    eval_expression_for_window(&documents[index], expression, variables)?
                {
                    if let Ok(number) = numeric_value(&value) {
                        total += number;
                    }
                }
            }
            Ok(number_bson(total))
        }
        WindowAccumulatorKind::Avg => {
            let mut total = 0.0;
            let mut count = 0_u64;
            for &index in indices {
                if let Some(value) =
                    eval_expression_for_window(&documents[index], expression, variables)?
                {
                    if let Ok(number) = numeric_value(&value) {
                        total += number;
                        count += 1;
                    }
                }
            }
            if count == 0 {
                Ok(Bson::Null)
            } else {
                Ok(Bson::Double(total / count as f64))
            }
        }
        WindowAccumulatorKind::First => match indices.first() {
            Some(&index) => {
                Ok(
                    eval_expression_for_window(&documents[index], expression, variables)?
                        .unwrap_or(Bson::Null),
                )
            }
            None => Ok(Bson::Null),
        },
        WindowAccumulatorKind::Last => match indices.last() {
            Some(&index) => {
                Ok(
                    eval_expression_for_window(&documents[index], expression, variables)?
                        .unwrap_or(Bson::Null),
                )
            }
            None => Ok(Bson::Null),
        },
        WindowAccumulatorKind::Push => {
            let mut values = Vec::new();
            for &index in indices {
                if let Some(value) =
                    eval_expression_for_window(&documents[index], expression, variables)?
                {
                    values.push(value);
                }
            }
            Ok(Bson::Array(values))
        }
        WindowAccumulatorKind::AddToSet => {
            let mut values = Vec::new();
            for &index in indices {
                if let Some(value) =
                    eval_expression_for_window(&documents[index], expression, variables)?
                {
                    if !values
                        .iter()
                        .any(|existing| compare_bson(existing, &value).is_eq())
                    {
                        values.push(value);
                    }
                }
            }
            Ok(Bson::Array(values))
        }
        WindowAccumulatorKind::Min => {
            let mut current = None::<Bson>;
            for &index in indices {
                if let Some(value) =
                    eval_expression_for_window(&documents[index], expression, variables)?
                {
                    match &current {
                        Some(existing) if compare_bson(existing, &value).is_le() => {}
                        _ => current = Some(value),
                    }
                }
            }
            Ok(current.unwrap_or(Bson::Null))
        }
        WindowAccumulatorKind::Max => {
            let mut current = None::<Bson>;
            for &index in indices {
                if let Some(value) =
                    eval_expression_for_window(&documents[index], expression, variables)?
                {
                    match &current {
                        Some(existing) if compare_bson(existing, &value).is_ge() => {}
                        _ => current = Some(value),
                    }
                }
            }
            Ok(current.unwrap_or(Bson::Null))
        }
    }
}

fn eval_expression_for_window(
    document: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Option<Bson>, QueryError> {
    match expression {
        Bson::String(path) if path.starts_with('$') && !path.starts_with("$$") => {
            Ok(lookup_path_owned(document, &path[1..]))
        }
        _ => Ok(Some(eval_expression_with_variables(
            document, expression, variables,
        )?)),
    }
}

fn compute_rank_values(
    documents: &[Document],
    sort_by: Option<&Document>,
    kind: WindowRankKind,
) -> Result<Vec<Bson>, QueryError> {
    let sort_by = sort_by.ok_or(QueryError::InvalidStage)?;
    let mut values = Vec::with_capacity(documents.len());
    let mut dense_rank = 1_i64;
    let mut current_rank = 1_i64;

    for index in 0..documents.len() {
        if index > 0
            && compare_documents_by_sort(&documents[index - 1], &documents[index], sort_by)
                != std::cmp::Ordering::Equal
        {
            current_rank = index as i64 + 1;
            dense_rank += 1;
        }

        values.push(match kind {
            WindowRankKind::DocumentNumber => Bson::Int64(index as i64 + 1),
            WindowRankKind::Rank => Bson::Int64(current_rank),
            WindowRankKind::DenseRank => Bson::Int64(dense_rank),
        });
    }

    Ok(values)
}

fn compute_shift_values(
    documents: &[Document],
    spec: &WindowShiftSpec,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Bson>, QueryError> {
    (0..documents.len())
        .map(|index| {
            let target = index as i64 + spec.by;
            if (0..documents.len() as i64).contains(&target) {
                eval_expression_with_variables(&documents[target as usize], &spec.output, variables)
            } else {
                match &spec.default {
                    Some(default) => {
                        eval_expression_with_variables(&documents[index], default, variables)
                    }
                    None => Ok(Bson::Null),
                }
            }
        })
        .collect()
}

fn compute_locf_values(
    documents: &[Document],
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Bson>, QueryError> {
    let mut last = None;
    let mut values = Vec::with_capacity(documents.len());

    for document in documents {
        let current = eval_expression_for_window(document, expression, variables)?;
        match current {
            Some(Bson::Null) | None => values.push(last.clone().unwrap_or(Bson::Null)),
            Some(value) => {
                last = Some(value.clone());
                values.push(value);
            }
        }
    }

    Ok(values)
}

fn compute_linear_fill_values(
    documents: &[Document],
    expression: &Bson,
    sort_by: Option<&Document>,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Bson>, QueryError> {
    let (sort_field, _) = single_sort_field(sort_by).ok_or(QueryError::InvalidStage)?;
    let sort_values = documents
        .iter()
        .map(|document| window_sort_scalar(document, sort_field))
        .collect::<Result<Vec<_>, _>>()?;
    let values = documents
        .iter()
        .map(
            |document| match eval_expression_for_window(document, expression, variables)? {
                Some(Bson::Null) | None => Ok(None),
                Some(value) => Ok(Some(numeric_value(&value)?)),
            },
        )
        .collect::<Result<Vec<_>, _>>()?;

    let mut results = Vec::with_capacity(documents.len());
    for index in 0..documents.len() {
        match values[index] {
            Some(value) => results.push(number_bson(value)),
            None => {
                let previous = (0..index)
                    .rev()
                    .find_map(|candidate| values[candidate].map(|value| (candidate, value)));
                let next = ((index + 1)..documents.len())
                    .find_map(|candidate| values[candidate].map(|value| (candidate, value)));
                let filled = match (previous, next) {
                    (Some((previous_index, previous_value)), Some((next_index, next_value))) => {
                        let previous_sort = sort_values[previous_index];
                        let next_sort = sort_values[next_index];
                        if (next_sort - previous_sort).abs() < f64::EPSILON {
                            return Err(QueryError::InvalidStage);
                        }
                        let ratio =
                            (sort_values[index] - previous_sort) / (next_sort - previous_sort);
                        Some(previous_value + ((next_value - previous_value) * ratio))
                    }
                    _ => None,
                };
                results.push(filled.map(number_bson).unwrap_or(Bson::Null));
            }
        }
    }

    Ok(results)
}

fn window_sort_scalar(document: &Document, field: &str) -> Result<f64, QueryError> {
    let value = lookup_path(document, field).ok_or(QueryError::InvalidStage)?;
    match value {
        Bson::DateTime(value) => Ok(value.timestamp_millis() as f64),
        _ => numeric_value(value),
    }
}

fn single_sort_field(sort_by: Option<&Document>) -> Option<(&str, i64)> {
    let sort_by = sort_by?;
    if sort_by.len() != 1 {
        return None;
    }
    let (field, direction) = sort_by.iter().next()?;
    Some((field.as_str(), integer_value(direction)?))
}

fn validate_sort_spec(sort: &Document) -> Result<(), QueryError> {
    if sort.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    for (field, direction) in sort {
        if field.is_empty() || field.starts_with('$') {
            return Err(QueryError::InvalidStage);
        }

        match integer_value(direction) {
            Some(1 | -1) => {}
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(())
}

fn bucket_documents(
    documents: Vec<Document>,
    spec: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;

    let mut group_by = None;
    let mut boundaries = None;
    let mut default = None;
    let mut output_specs = None;

    for (field, value) in spec {
        match field.as_str() {
            "groupBy" => {
                let valid = match value {
                    Bson::String(path) => path.starts_with('$') && path.len() > 1,
                    Bson::Document(document) => document
                        .iter()
                        .next()
                        .is_some_and(|(name, _)| name.starts_with('$')),
                    _ => false,
                };
                if !valid {
                    return Err(QueryError::InvalidStage);
                }
                group_by = Some(value.clone());
            }
            "boundaries" => {
                let values = value.as_array().ok_or(QueryError::InvalidStage)?.clone();
                if values.len() < 2 {
                    return Err(QueryError::InvalidStage);
                }
                for window in values.windows(2) {
                    if bucket_value_type(&window[0]) != bucket_value_type(&window[1]) {
                        return Err(QueryError::InvalidStage);
                    }
                    if compare_bson(&window[0], &window[1]) != std::cmp::Ordering::Less {
                        return Err(QueryError::InvalidStage);
                    }
                }
                boundaries = Some(values);
            }
            "default" => {
                default = Some(value.clone());
            }
            "output" => {
                let output = value.as_document().ok_or(QueryError::InvalidStage)?;
                let accumulators = output
                    .iter()
                    .map(|(field, value)| Ok((field.clone(), parse_accumulator(value)?)))
                    .collect::<Result<Vec<_>, QueryError>>()?;
                output_specs = Some(accumulators);
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let group_by = group_by.ok_or(QueryError::InvalidStage)?;
    let boundaries = boundaries.ok_or(QueryError::InvalidStage)?;
    if let Some(default_value) = default.as_ref() {
        if bucket_value_type(default_value) == bucket_value_type(&boundaries[0]) {
            let below_lower =
                compare_bson(default_value, &boundaries[0]) == std::cmp::Ordering::Less;
            let above_or_equal_upper =
                compare_bson(default_value, boundaries.last().expect("boundaries"))
                    != std::cmp::Ordering::Less;
            if !below_lower && !above_or_equal_upper {
                return Err(QueryError::InvalidStage);
            }
        }
    }

    let output_specs = output_specs.unwrap_or_else(|| {
        vec![(
            "count".to_string(),
            GroupAccumulatorSpec::Sum(Bson::Int32(1)),
        )]
    });

    let mut groups: BTreeMap<Vec<u8>, (Bson, BTreeMap<String, GroupAccumulatorState>)> =
        BTreeMap::new();

    for document in documents {
        let value = eval_expression_with_variables(&document, &group_by, variables)?;
        let bucket_id = match bucket_assignment(&value, &boundaries, default.as_ref())? {
            Some(bucket_id) => bucket_id,
            None => continue,
        };
        let bucket_key = bson::to_vec(&doc! { "_id": bucket_id.clone() })
            .map_err(|_| QueryError::InvalidStage)?;
        let entry = groups.entry(bucket_key).or_insert_with(|| {
            let states = output_specs
                .iter()
                .map(|(field, spec)| (field.clone(), GroupAccumulatorState::from_spec(spec)))
                .collect::<BTreeMap<_, _>>();
            (bucket_id.clone(), states)
        });
        for (field, accumulator) in &output_specs {
            let state = entry.1.get_mut(field).expect("bucket state exists");
            state.apply(&document, accumulator, variables)?;
        }
    }

    let mut results = groups
        .into_values()
        .map(|(bucket_id, states)| {
            let mut output = Document::new();
            output.insert("_id", bucket_id);
            for (field, state) in states {
                output.insert(field, state.finish());
            }
            output
        })
        .collect::<Vec<_>>();
    let sort_spec = doc! { "_id": 1 };
    results.sort_by(|left, right| compare_documents_by_sort(left, right, &sort_spec));
    Ok(results)
}

fn bucket_auto_documents(
    documents: Vec<Document>,
    spec: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut group_by = None;
    let mut buckets = None;
    let mut output_specs = None;

    for (field, value) in spec {
        match field.as_str() {
            "groupBy" => {
                let valid = match value {
                    Bson::String(path) => path.starts_with('$') && path.len() > 1,
                    Bson::Document(document) => document
                        .iter()
                        .next()
                        .is_some_and(|(name, _)| name.starts_with('$')),
                    _ => false,
                };
                if !valid {
                    return Err(QueryError::InvalidStage);
                }
                group_by = Some(value.clone());
            }
            "buckets" => {
                let bucket_count = integer_value(value).ok_or(QueryError::InvalidStage)?;
                if bucket_count <= 0 {
                    return Err(QueryError::InvalidStage);
                }
                buckets = Some(bucket_count as usize);
            }
            "output" => {
                let output = value.as_document().ok_or(QueryError::InvalidStage)?;
                let accumulators = output
                    .iter()
                    .map(|(field, value)| Ok((field.clone(), parse_accumulator(value)?)))
                    .collect::<Result<Vec<_>, QueryError>>()?;
                output_specs = Some(accumulators);
            }
            "granularity" => return Err(QueryError::InvalidStage),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    let group_by = group_by.ok_or(QueryError::InvalidStage)?;
    let requested_bucket_count = buckets.ok_or(QueryError::InvalidStage)?;
    if documents.is_empty() {
        return Ok(Vec::new());
    }

    let output_specs = output_specs.unwrap_or_else(|| {
        vec![(
            "count".to_string(),
            GroupAccumulatorSpec::Sum(Bson::Int32(1)),
        )]
    });

    let mut keyed = documents
        .iter()
        .enumerate()
        .map(|(position, document)| {
            eval_expression_with_variables(document, &group_by, variables)
                .map(|value| (position, value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    keyed.sort_by(|left, right| compare_bson(&left.1, &right.1).then(left.0.cmp(&right.0)));

    let bucket_count = requested_bucket_count.min(keyed.len());
    let base_size = keyed.len() / bucket_count;
    let remainder = keyed.len() % bucket_count;
    let mut bucket_assignments = vec![0_usize; keyed.len()];
    let mut bucket_bounds = Vec::with_capacity(bucket_count);
    let mut offset = 0_usize;

    for bucket_index in 0..bucket_count {
        let bucket_size = base_size + usize::from(bucket_index < remainder);
        let start = offset;
        let end = start + bucket_size;
        let min = keyed[start].1.clone();
        let max = if bucket_index + 1 < bucket_count {
            keyed[end].1.clone()
        } else {
            keyed[end - 1].1.clone()
        };
        for (position, _) in &keyed[start..end] {
            bucket_assignments[*position] = bucket_index;
        }
        bucket_bounds.push((min, max));
        offset = end;
    }

    let mut grouped_states = (0..bucket_count)
        .map(|_| {
            output_specs
                .iter()
                .map(|(field, spec)| (field.clone(), GroupAccumulatorState::from_spec(spec)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    for (position, document) in documents.iter().enumerate() {
        let bucket_index = bucket_assignments[position];
        for (field, accumulator) in &output_specs {
            let state = grouped_states[bucket_index]
                .iter_mut()
                .find(|(existing_field, _)| existing_field == field)
                .map(|(_, state)| state)
                .expect("bucket state exists");
            state.apply(document, accumulator, variables)?;
        }
    }

    Ok(bucket_bounds
        .into_iter()
        .zip(grouped_states)
        .map(|((min, max), states)| {
            let mut output = Document::new();
            output.insert("_id", doc! { "min": min, "max": max });
            for (field, state) in states {
                output.insert(field, state.finish());
            }
            output
        })
        .collect())
}

fn bucket_assignment(
    value: &Bson,
    boundaries: &[Bson],
    default: Option<&Bson>,
) -> Result<Option<Bson>, QueryError> {
    for window in boundaries.windows(2) {
        let lower = &window[0];
        let upper = &window[1];
        if compare_bson(value, lower) != std::cmp::Ordering::Less
            && compare_bson(value, upper) == std::cmp::Ordering::Less
        {
            return Ok(Some(lower.clone()));
        }
    }

    if let Some(default_value) = default {
        return Ok(Some(default_value.clone()));
    }

    Err(QueryError::InvalidArgument(
        "bucket groupBy value did not match any bucket and no default was specified".to_string(),
    ))
}

fn bucket_value_type(value: &Bson) -> u8 {
    match value {
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::ObjectId(_) => 6,
        Bson::Boolean(_) => 7,
        Bson::DateTime(_) => 8,
        Bson::Null => 9,
        Bson::RegularExpression(_) => 10,
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => 11,
        Bson::Timestamp(_) => 12,
        Bson::Symbol(_) => 13,
        Bson::Undefined => 14,
        Bson::MaxKey => 15,
        Bson::MinKey => 16,
        Bson::DbPointer(_) => 17,
    }
}

fn validate_facet_name(name: &str) -> Result<(), QueryError> {
    if name.is_empty() || name.starts_with('$') || name.contains('.') || name.contains('\0') {
        return Err(QueryError::InvalidStage);
    }
    Ok(())
}

fn documents_stage(stage_index: usize, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let documents = spec.as_array().ok_or(QueryError::InvalidStage)?;
    documents
        .iter()
        .map(|value| {
            value
                .as_document()
                .cloned()
                .ok_or(QueryError::ExpectedDocument)
        })
        .collect()
}

fn current_op_documents(stage_index: usize, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut local_ops = None;
    for (key, value) in spec {
        match key.as_str() {
            "localOps" => {
                local_ops = Some(value.as_bool().ok_or(QueryError::InvalidStage)?);
            }
            "allUsers" | "idleConnections" | "idleCursors" | "idleSessions" | "targetAllNodes"
            | "truncateOps" => return Err(QueryError::InvalidStage),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    if local_ops != Some(true) {
        return Err(QueryError::InvalidStage);
    }

    Ok(vec![doc! {
        "type": "op",
        "ns": "admin.$cmd.aggregate",
        "command": {
            "aggregate": 1,
            "pipeline": [{ "$currentOp": { "localOps": true } }],
            "$db": "admin",
        },
    }])
}

fn coll_stats_documents(
    documents: Vec<Document>,
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let coll_stats = parse_coll_stats_spec(spec)?;
    Ok(vec![build_coll_stats_document(
        "app.synthetic",
        &documents,
        1,
        coll_stats.storage_stats_scale,
        coll_stats.include_count,
    )])
}

fn index_stats_documents(stage_index: usize, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    if !spec.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    Ok(vec![doc! {
        "name": "_id_",
        "key": { "_id": 1 },
        "spec": {
            "name": "_id_",
            "key": { "_id": 1 },
            "unique": true,
        },
        "accesses": {
            "ops": 0_i64,
            "since": bson::DateTime::from_millis(0),
        },
        "host": "mqlite",
    }])
}

fn plan_cache_stats_documents(
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    match spec.get("allHosts") {
        None if spec.is_empty() => {}
        Some(Bson::Boolean(false)) if spec.len() == 1 => {}
        Some(Bson::Boolean(true)) if spec.len() == 1 => return Err(QueryError::InvalidStage),
        _ => return Err(QueryError::InvalidStage),
    }

    Ok(vec![doc! {
        "namespace": "app.synthetic",
        "filterShape": "{\"qty\":{\"$gte\":\"?\"}}",
        "sortShape": "{}",
        "projectionShape": "{}",
        "sequence": 1_i64,
        "cachedPlan": { "type": "collectionScan" },
        "host": "mqlite",
    }])
}

fn list_catalog_documents(stage_index: usize, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    if !spec.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    Ok(vec![doc! {
        "db": "app",
        "ns": "app.synthetic",
        "name": "synthetic",
        "type": "collection",
        "options": {},
        "indexCount": 1_i64,
    }])
}

fn list_cluster_catalog_documents(
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let stage = parse_list_cluster_catalog_spec(spec)?;
    let mut result = doc! {
        "db": "app",
        "ns": "app.synthetic",
        "type": "collection",
        "options": {},
        "info": { "readOnly": false },
        "idIndex": { "name": "_id_", "key": { "_id": 1 }, "unique": true },
        "sharded": false,
    };
    if stage.tracked {
        result.insert("tracked", false);
    }
    if stage.shards {
        result.insert("shards", Bson::Array(Vec::new()));
    }
    Ok(vec![result])
}

fn list_cached_and_active_users_documents(
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    if !spec.is_empty() {
        return Err(QueryError::InvalidStage);
    }

    Ok(Vec::new())
}

fn list_local_sessions_documents(
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    parse_list_sessions_spec(spec)?;
    Ok(Vec::new())
}

fn list_sessions_documents(stage_index: usize, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    parse_list_sessions_spec(spec)?;
    Ok(Vec::new())
}

fn list_sampled_queries_documents(
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    parse_list_sampled_queries_spec(spec)?;
    Ok(Vec::new())
}

fn list_search_indexes_documents(
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    parse_list_search_indexes_spec(spec)?;
    Ok(Vec::new())
}

fn query_settings_documents(stage_index: usize, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    parse_query_settings_spec(spec)?;
    Ok(Vec::new())
}

fn list_mql_entities_documents(
    stage_index: usize,
    spec: &Bson,
) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    match spec.get_str("entityType") {
        Ok("aggregationStages") if spec.len() == 1 => {}
        _ => return Err(QueryError::InvalidStage),
    }

    let mut stage_names = SUPPORTED_AGGREGATION_STAGES.to_vec();
    stage_names.sort_unstable();
    Ok(stage_names
        .into_iter()
        .map(|name| doc! { "name": name })
        .collect())
}

#[derive(Debug, Clone, Copy)]
struct ListClusterCatalogStage {
    shards: bool,
    tracked: bool,
}

fn parse_list_cluster_catalog_spec(spec: &Bson) -> Result<ListClusterCatalogStage, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut stage = ListClusterCatalogStage {
        shards: false,
        tracked: false,
    };

    for (key, value) in spec {
        match key.as_str() {
            "shards" => {
                stage.shards = value.as_bool().ok_or(QueryError::InvalidStage)?;
            }
            "tracked" => {
                stage.tracked = value.as_bool().ok_or(QueryError::InvalidStage)?;
            }
            "balancingConfiguration" => {
                value.as_bool().ok_or(QueryError::InvalidStage)?;
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(stage)
}

fn parse_list_sessions_spec(spec: &Bson) -> Result<(), QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut all_users = false;
    let mut has_users = false;

    for (key, value) in spec {
        match key.as_str() {
            "allUsers" => {
                all_users = value.as_bool().ok_or(QueryError::InvalidStage)?;
            }
            "users" => {
                let users = value.as_array().ok_or(QueryError::InvalidStage)?;
                for user in users {
                    let user = user.as_document().ok_or(QueryError::InvalidStage)?;
                    match (user.get_str("user"), user.get_str("db"), user.len()) {
                        (Ok(_), Ok(_), 2) => {}
                        _ => return Err(QueryError::InvalidStage),
                    }
                }
                has_users = !users.is_empty();
            }
            "$_internalPredicate" => return Err(QueryError::InvalidStage),
            _ => return Err(QueryError::InvalidStage),
        }
    }

    if all_users && has_users {
        return Err(QueryError::InvalidStage);
    }

    Ok(())
}

fn parse_list_sampled_queries_spec(spec: &Bson) -> Result<(), QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;

    for (key, value) in spec {
        match key.as_str() {
            "namespace" => {
                let namespace = value.as_str().ok_or(QueryError::InvalidStage)?;
                validate_namespace_string(namespace)?;
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(())
}

fn parse_list_search_indexes_spec(spec: &Bson) -> Result<(), QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut has_id = false;
    let mut has_name = false;

    for (key, value) in spec {
        match key.as_str() {
            "id" => {
                value.as_str().ok_or(QueryError::InvalidStage)?;
                has_id = true;
            }
            "name" => {
                value.as_str().ok_or(QueryError::InvalidStage)?;
                has_name = true;
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    if has_id && has_name {
        return Err(QueryError::InvalidArgument(
            "Cannot set both `name` and `id` for $listSearchIndexes.".to_string(),
        ));
    }

    Ok(())
}

fn validate_namespace_string(namespace: &str) -> Result<(), QueryError> {
    if namespace.contains('\0') {
        return Err(QueryError::InvalidStage);
    }
    let Some((database, collection)) = namespace.split_once('.') else {
        return Err(QueryError::InvalidStage);
    };
    if database.is_empty() || collection.is_empty() {
        return Err(QueryError::InvalidStage);
    }
    Ok(())
}

fn parse_query_settings_spec(spec: &Bson) -> Result<(), QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;

    for (key, value) in spec {
        match key.as_str() {
            "showDebugQueryShape" => {
                value.as_bool().ok_or(QueryError::InvalidStage)?;
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct CollStatsStage {
    include_count: bool,
    storage_stats_scale: Option<i64>,
}

fn parse_coll_stats_spec(spec: &Bson) -> Result<CollStatsStage, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut include_count = false;
    let mut storage_stats_scale = None;

    for (key, value) in spec {
        match key.as_str() {
            "count" => {
                let document = value.as_document().ok_or(QueryError::InvalidStage)?;
                if !document.is_empty() {
                    return Err(QueryError::InvalidStage);
                }
                include_count = true;
            }
            "storageStats" => {
                storage_stats_scale = Some(parse_storage_stats_spec(value)?);
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }

    Ok(CollStatsStage {
        include_count,
        storage_stats_scale,
    })
}

fn parse_storage_stats_spec(spec: &Bson) -> Result<i64, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    let mut scale = 1_i64;
    for (key, value) in spec {
        match key.as_str() {
            "scale" => {
                scale = integer_value(value).ok_or(QueryError::InvalidStage)?;
                if scale <= 0 {
                    return Err(QueryError::InvalidStage);
                }
            }
            "verbose" | "waitForLock" | "numericOnly" => {
                value.as_bool().ok_or(QueryError::InvalidStage)?;
            }
            _ => return Err(QueryError::InvalidStage),
        }
    }
    Ok(scale)
}

fn build_coll_stats_document(
    namespace: &str,
    documents: &[Document],
    index_count: usize,
    storage_stats_scale: Option<i64>,
    include_count: bool,
) -> Document {
    let count = documents.len() as i64;
    let total_size = documents
        .iter()
        .map(|document| bson::to_vec(document).unwrap_or_default().len() as i64)
        .sum::<i64>();
    let average_size = if count == 0 { 0 } else { total_size / count };

    let mut result = doc! {
        "ns": namespace,
    };
    if include_count {
        result.insert("count", count);
    }
    if let Some(scale) = storage_stats_scale {
        let scale_size = |value: i64| value / scale.max(1);
        result.insert(
            "storageStats",
            doc! {
                "count": count,
                "size": scale_size(total_size),
                "avgObjSize": scale_size(average_size),
                "storageSize": scale_size(total_size),
                "nindexes": index_count as i64,
                "totalIndexSize": 0_i64,
                "indexSizes": {},
            },
        );
    }
    result
}

fn sample_documents(documents: Vec<Document>, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    if spec.len() != 1 {
        return Err(QueryError::InvalidStage);
    }

    let size = match spec.get("size") {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Double(value)) if value.fract() == 0.0 => *value as i64,
        _ => return Err(QueryError::InvalidStage),
    };

    if size <= 0 {
        return Err(QueryError::InvalidStage);
    }
    if size as usize >= documents.len() {
        return Ok(documents);
    }

    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mut scored = documents
        .into_iter()
        .enumerate()
        .map(|(position, document)| {
            let mut hasher = DefaultHasher::new();
            seed.hash(&mut hasher);
            position.hash(&mut hasher);
            bson::to_vec(&document)
                .unwrap_or_default()
                .hash(&mut hasher);
            (hasher.finish(), position, document)
        })
        .collect::<Vec<_>>();
    scored.sort_by_key(|(score, position, _)| (*score, *position));
    Ok(scored
        .into_iter()
        .take(size as usize)
        .map(|(_, _, document)| document)
        .collect())
}

fn sort_by_count(
    documents: Vec<Document>,
    spec: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let expression = match spec {
        Bson::String(path) if path.starts_with('$') && path.len() > 1 => Bson::String(path.clone()),
        Bson::Document(document)
            if document
                .iter()
                .next()
                .is_some_and(|(field, _)| field.starts_with('$')) =>
        {
            Bson::Document(document.clone())
        }
        _ => return Err(QueryError::InvalidStage),
    };

    let group_spec = doc! {
        "_id": expression,
        "count": { "$sum": 1 },
    };
    let mut results = group_documents(documents, &group_spec, variables)?;
    let sort_spec = doc! { "count": -1 };
    results.sort_by(|left, right| compare_documents_by_sort(left, right, &sort_spec));
    Ok(results)
}

fn parse_unset(spec: &Bson) -> Result<Vec<String>, QueryError> {
    match spec {
        Bson::String(value) => Ok(vec![value.clone()]),
        Bson::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or(QueryError::InvalidStage)
            })
            .collect(),
        _ => Err(QueryError::InvalidStage),
    }
}

fn compare_documents_by_sort(
    left: &Document,
    right: &Document,
    sort: &Document,
) -> std::cmp::Ordering {
    for (field, direction) in sort {
        let left_value = lookup_path(left, field).unwrap_or(&Bson::Null);
        let right_value = lookup_path(right, field).unwrap_or(&Bson::Null);
        let mut ordering = compare_bson(left_value, right_value);
        if integer_value(direction).unwrap_or(1) < 0 {
            ordering = ordering.reverse();
        }
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn unwind_documents(documents: Vec<Document>, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    let (path, preserve) = match spec {
        Bson::String(path) => (path.trim_start_matches('$').to_string(), false),
        Bson::Document(spec) => (
            spec.get_str("path")
                .map_err(|_| QueryError::InvalidStage)?
                .trim_start_matches('$')
                .to_string(),
            spec.get_bool("preserveNullAndEmptyArrays").unwrap_or(false),
        ),
        _ => return Err(QueryError::InvalidStage),
    };

    let mut unwound = Vec::new();
    for document in documents {
        match lookup_path(&document, &path) {
            Some(Bson::Array(items)) if !items.is_empty() => {
                for item in items {
                    let mut clone = document.clone();
                    set_path(&mut clone, &path, item.clone())
                        .map_err(|_| QueryError::InvalidStructure)?;
                    unwound.push(clone);
                }
            }
            Some(Bson::Array(_)) if preserve => {
                let mut clone = document;
                remove_path(&mut clone, &path).map_err(|_| QueryError::InvalidStructure)?;
                unwound.push(clone);
            }
            Some(_) if preserve => unwound.push(document),
            None if preserve => unwound.push(document),
            _ => {}
        }
    }
    Ok(unwound)
}

fn group_documents(
    documents: Vec<Document>,
    spec: &Document,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let id_expression = spec.get("_id").cloned().unwrap_or(Bson::Null);
    let accumulator_specs = spec
        .iter()
        .filter(|(field, _)| field.as_str() != "_id")
        .map(|(field, value)| Ok((field.clone(), parse_accumulator(value)?)))
        .collect::<Result<Vec<_>, QueryError>>()?;

    let mut groups: BTreeMap<Vec<u8>, (Bson, BTreeMap<String, GroupAccumulatorState>)> =
        BTreeMap::new();

    for document in documents {
        let group_key = eval_expression_with_variables(&document, &id_expression, variables)?;
        let group_id = bson::to_vec(&doc! { "_id": group_key.clone() })
            .map_err(|_| QueryError::InvalidStage)?;
        let entry = groups.entry(group_id).or_insert_with(|| {
            let states = accumulator_specs
                .iter()
                .map(|(field, spec)| (field.clone(), GroupAccumulatorState::from_spec(spec)))
                .collect::<BTreeMap<_, _>>();
            (group_key.clone(), states)
        });

        for (field, accumulator) in &accumulator_specs {
            let state = entry.1.get_mut(field).expect("state exists");
            state.apply(&document, accumulator, variables)?;
        }
    }

    groups
        .into_values()
        .map(|(group_key, states)| {
            let mut output = Document::new();
            output.insert("_id", group_key);
            for (field, state) in states {
                output.insert(field, state.finish());
            }
            Ok(output)
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedactDecision {
    Descend,
    Keep,
    Prune,
}

fn redact_document(
    document: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Option<Document>, QueryError> {
    redact_document_with_root(document, document, expression, variables)
}

fn redact_document_with_root(
    root: &Document,
    current: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Option<Document>, QueryError> {
    match redact_decision(root, current, expression, variables)? {
        RedactDecision::Keep => Ok(Some(current.clone())),
        RedactDecision::Prune => Ok(None),
        RedactDecision::Descend => {
            let mut redacted = Document::new();
            for (field, value) in current {
                if let Some(value) = redact_value(root, value, expression, variables)? {
                    redacted.insert(field.clone(), value);
                }
            }
            Ok(Some(redacted))
        }
    }
}

fn redact_value(
    root: &Document,
    value: &Bson,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Option<Bson>, QueryError> {
    match value {
        Bson::Document(document) => Ok(redact_document_with_root(
            root, document, expression, variables,
        )?
        .map(Bson::Document)),
        Bson::Array(items) => {
            let mut redacted = Vec::new();
            for item in items {
                if let Some(item) = redact_value(root, item, expression, variables)? {
                    redacted.push(item);
                }
            }
            Ok(Some(Bson::Array(redacted)))
        }
        _ => Ok(Some(value.clone())),
    }
}

fn redact_decision(
    root: &Document,
    current: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<RedactDecision, QueryError> {
    let mut variables = variables.clone();
    variables.insert("ROOT".to_string(), Bson::Document(root.clone()));
    variables.insert("CURRENT".to_string(), Bson::Document(current.clone()));
    variables.insert("DESCEND".to_string(), Bson::String("descend".to_string()));
    variables.insert("KEEP".to_string(), Bson::String("keep".to_string()));
    variables.insert("PRUNE".to_string(), Bson::String("prune".to_string()));

    match eval_expression_with_variables(current, expression, &variables)? {
        Bson::String(value) if value == "descend" => Ok(RedactDecision::Descend),
        Bson::String(value) if value == "keep" => Ok(RedactDecision::Keep),
        Bson::String(value) if value == "prune" => Ok(RedactDecision::Prune),
        _ => Err(QueryError::InvalidStage),
    }
}

fn replace_root(
    documents: Vec<Document>,
    spec: &Document,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    let new_root = spec.get("newRoot").ok_or(QueryError::InvalidStage)?;
    documents
        .into_iter()
        .map(
            |document| match eval_expression_with_variables(&document, new_root, variables)? {
                Bson::Document(document) => Ok(document),
                _ => Err(QueryError::ExpectedDocument),
            },
        )
        .collect()
}

fn replace_with(
    documents: Vec<Document>,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Vec<Document>, QueryError> {
    documents
        .into_iter()
        .map(
            |document| match eval_expression_with_variables(&document, expression, variables)? {
                Bson::Document(document) => Ok(document),
                _ => Err(QueryError::ExpectedDocument),
            },
        )
        .collect()
}

fn parse_accumulator(value: &Bson) -> Result<GroupAccumulatorSpec, QueryError> {
    let document = value.as_document().ok_or(QueryError::InvalidStage)?;
    if document.len() != 1 {
        return Err(QueryError::InvalidStage);
    }
    let (name, expression) = document.iter().next().expect("single accumulator");
    match name.as_str() {
        "$sum" => Ok(GroupAccumulatorSpec::Sum(expression.clone())),
        "$first" => Ok(GroupAccumulatorSpec::First(expression.clone())),
        "$push" => Ok(GroupAccumulatorSpec::Push(expression.clone())),
        "$addToSet" => Ok(GroupAccumulatorSpec::AddToSet(expression.clone())),
        "$avg" => Ok(GroupAccumulatorSpec::Avg(expression.clone())),
        other => Err(QueryError::UnsupportedOperator(other.to_string())),
    }
}

enum GroupAccumulatorSpec {
    Sum(Bson),
    First(Bson),
    Push(Bson),
    AddToSet(Bson),
    Avg(Bson),
}

enum GroupAccumulatorState {
    Sum(f64),
    First(Option<Bson>),
    Push(Vec<Bson>),
    AddToSet(Vec<Bson>),
    Avg { sum: f64, count: u64 },
}

impl GroupAccumulatorState {
    fn from_spec(spec: &GroupAccumulatorSpec) -> Self {
        match spec {
            GroupAccumulatorSpec::Sum(_) => Self::Sum(0.0),
            GroupAccumulatorSpec::First(_) => Self::First(None),
            GroupAccumulatorSpec::Push(_) => Self::Push(Vec::new()),
            GroupAccumulatorSpec::AddToSet(_) => Self::AddToSet(Vec::new()),
            GroupAccumulatorSpec::Avg(_) => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    fn apply(
        &mut self,
        document: &Document,
        spec: &GroupAccumulatorSpec,
        variables: &BTreeMap<String, Bson>,
    ) -> Result<(), QueryError> {
        match (self, spec) {
            (Self::Sum(total), GroupAccumulatorSpec::Sum(expression)) => {
                *total += numeric_value(&eval_expression_with_variables(
                    document, expression, variables,
                )?)?;
                Ok(())
            }
            (Self::First(current), GroupAccumulatorSpec::First(expression)) => {
                if current.is_none() {
                    *current = Some(eval_expression_with_variables(
                        document, expression, variables,
                    )?);
                }
                Ok(())
            }
            (Self::Push(values), GroupAccumulatorSpec::Push(expression)) => {
                values.push(eval_expression_with_variables(
                    document, expression, variables,
                )?);
                Ok(())
            }
            (Self::AddToSet(values), GroupAccumulatorSpec::AddToSet(expression)) => {
                let value = eval_expression_with_variables(document, expression, variables)?;
                if !values
                    .iter()
                    .any(|existing| compare_bson(existing, &value).is_eq())
                {
                    values.push(value);
                }
                Ok(())
            }
            (Self::Avg { sum, count }, GroupAccumulatorSpec::Avg(expression)) => {
                *sum += numeric_value(&eval_expression_with_variables(
                    document, expression, variables,
                )?)?;
                *count += 1;
                Ok(())
            }
            _ => Err(QueryError::InvalidStage),
        }
    }

    fn finish(self) -> Bson {
        match self {
            Self::Sum(total) => number_bson(total),
            Self::First(value) => value.unwrap_or(Bson::Null),
            Self::Push(values) => Bson::Array(values),
            Self::AddToSet(values) => Bson::Array(values),
            Self::Avg { sum, count } => {
                if count == 0 {
                    Bson::Null
                } else {
                    Bson::Double(sum / count as f64)
                }
            }
        }
    }
}
