use std::{
    collections::{BTreeMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, lookup_path, remove_path, set_path};

use crate::{
    QueryError,
    capabilities::SUPPORTED_AGGREGATION_STAGES,
    expression::{eval_expression_with_variables, integer_value, number_bson, numeric_value},
    filter::document_matches_with_variables,
    projection::apply_projection_with_variables,
};

pub trait CollectionResolver {
    fn resolve_collection(&self, database: &str, collection: &str) -> Vec<Document>;
}

#[derive(Debug, Default, Clone, Copy)]
struct NoopResolver;

impl CollectionResolver for NoopResolver {
    fn resolve_collection(&self, _database: &str, _collection: &str) -> Vec<Document> {
        Vec::new()
    }
}

pub fn run_pipeline(
    documents: Vec<Document>,
    pipeline: &[Document],
) -> Result<Vec<Document>, QueryError> {
    let resolver = NoopResolver;
    run_pipeline_with_resolver(documents, pipeline, "", &resolver)
}

pub fn run_pipeline_with_resolver<R: CollectionResolver>(
    documents: Vec<Document>,
    pipeline: &[Document],
    database: &str,
    resolver: &R,
) -> Result<Vec<Document>, QueryError> {
    let context = PipelineContext {
        inside_facet: false,
        database,
        resolver,
        variables: BTreeMap::new(),
    };
    run_pipeline_with_context(documents, pipeline, &context)
}

#[derive(Debug, Clone)]
struct PipelineContext<'a, R> {
    inside_facet: bool,
    database: &'a str,
    resolver: &'a R,
    variables: BTreeMap<String, Bson>,
}

fn run_pipeline_with_context<R: CollectionResolver>(
    documents: Vec<Document>,
    pipeline: &[Document],
    context: &PipelineContext<'_, R>,
) -> Result<Vec<Document>, QueryError> {
    let mut current = documents;

    for (stage_index, stage) in pipeline.iter().enumerate() {
        if stage.len() != 1 {
            return Err(QueryError::InvalidStage);
        }

        let (stage_name, stage_spec) = stage.iter().next().expect("single stage");
        current = match stage_name.as_str() {
            "$bucket" => bucket_documents(current, stage_spec, &context.variables)?,
            "$bucketAuto" => bucket_auto_documents(current, stage_spec, &context.variables)?,
            "$collStats" => coll_stats_documents(current, stage_index, stage_spec)?,
            "$currentOp" => current_op_documents(stage_index, stage_spec)?,
            "$indexStats" => index_stats_documents(stage_index, stage_spec)?,
            "$listCatalog" => list_catalog_documents(stage_index, stage_spec)?,
            "$listCachedAndActiveUsers" => {
                list_cached_and_active_users_documents(stage_index, stage_spec)?
            }
            "$listLocalSessions" => list_local_sessions_documents(stage_index, stage_spec)?,
            "$listSampledQueries" => list_sampled_queries_documents(stage_index, stage_spec)?,
            "$listSessions" => list_sessions_documents(stage_index, stage_spec)?,
            "$listMqlEntities" => list_mql_entities_documents(stage_index, stage_spec)?,
            "$planCacheStats" => plan_cache_stats_documents(stage_index, stage_spec)?,
            "$documents" if context.inside_facet => return Err(QueryError::InvalidStage),
            "$documents" => documents_stage(stage_index, stage_spec)?,
            "$facet" if context.inside_facet => return Err(QueryError::InvalidStage),
            "$facet" => facet_documents(current, stage_spec, context)?,
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
