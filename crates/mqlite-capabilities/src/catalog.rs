use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use mqlite_query::{
    SUPPORTED_AGGREGATION_ACCUMULATORS, SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
    SUPPORTED_AGGREGATION_STAGES, SUPPORTED_AGGREGATION_WINDOW_OPERATORS,
    SUPPORTED_QUERY_OPERATORS,
};
use regex::Regex;
use serde::{Deserialize, Serialize};

pub const UPSTREAM_JSON_PATH: &str = "capabilities/mongodb/upstream-capabilities.generated.json";
pub const SUPPORT_JSON_PATH: &str = "capabilities/mqlite/support.generated.json";
pub const GAP_JSON_PATH: &str = "capabilities/mqlite/gap-analysis.generated.json";
pub const GAP_MARKDOWN_PATH: &str = "capabilities/mqlite/gap-analysis.generated.md";

const QUERY_OPERATOR_SOURCE: &str =
    "../mongo/src/mongo/db/query/compiler/parsers/matcher/expression_parser.cpp";
const FIND_COMMAND_IDL: &str = "../mongo/src/mongo/db/query/find_command.idl";
const AGGREGATE_COMMAND_IDL: &str = "../mongo/src/mongo/db/pipeline/aggregate_command.idl";
const WRITE_OPS_IDL: &str = "../mongo/src/mongo/db/query/write_ops/write_ops.idl";
const DBREF_PSEUDO_OPERATORS: &[&str] = &["$db", "$id", "$ref"];
const IGNORED_AGGREGATION_STAGES: &[&str] = &[
    "$rankFusion",
    "$score",
    "$scoreFusion",
    "$search",
    "$searchBeta",
    "$searchMeta",
    "$shardedDataDistribution",
    "$vectorSearch",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceAnchors {
    pub operator_sources: Vec<String>,
    pub command_idl_anchors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityItem {
    pub name: String,
    pub internal: bool,
    pub source_kinds: Vec<String>,
    pub source_files: Vec<String>,
    pub macro_kinds: Vec<String>,
    pub feature_flagged: bool,
    pub conditional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpstreamCatalog {
    pub reference_anchors: ReferenceAnchors,
    pub query_operators: Vec<CapabilityItem>,
    pub aggregation_stages: Vec<CapabilityItem>,
    pub aggregation_expression_operators: Vec<CapabilityItem>,
    pub aggregation_accumulator_operators: Vec<CapabilityItem>,
    pub aggregation_window_operators: Vec<CapabilityItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MqliteSupportCatalog {
    pub query_operators: Vec<String>,
    pub aggregation_stages: Vec<String>,
    pub aggregation_expression_operators: Vec<String>,
    pub aggregation_accumulator_operators: Vec<String>,
    pub aggregation_window_operators: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SupportStatus {
    Supported,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMode {
    Positive,
    Rejection,
    BlockedByStage { stage: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapItem {
    pub name: String,
    pub internal: bool,
    pub source_kinds: Vec<String>,
    pub source_files: Vec<String>,
    pub macro_kinds: Vec<String>,
    pub feature_flagged: bool,
    pub conditional: bool,
    pub ignored: bool,
    pub status: SupportStatus,
    pub validation: ValidationMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapCategory {
    pub total_upstream: usize,
    pub public_upstream: usize,
    pub total_supported: usize,
    pub public_supported: usize,
    pub total_unsupported: usize,
    pub public_unsupported: usize,
    pub public_ignored: usize,
    pub items: Vec<GapItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapAnalysis {
    pub reference_anchors: ReferenceAnchors,
    pub query_operators: GapCategory,
    pub aggregation_stages: GapCategory,
    pub aggregation_expression_operators: GapCategory,
    pub aggregation_accumulator_operators: GapCategory,
    pub aggregation_window_operators: GapCategory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedArtifacts {
    pub upstream: UpstreamCatalog,
    pub support: MqliteSupportCatalog,
    pub gap: GapAnalysis,
    pub upstream_json: String,
    pub support_json: String,
    pub gap_json: String,
    pub gap_markdown: String,
}

#[derive(Debug, Default)]
struct CapabilityAccumulator {
    source_kinds: BTreeSet<String>,
    source_files: BTreeSet<String>,
    macro_kinds: BTreeSet<String>,
    feature_flagged: bool,
    conditional: bool,
}

pub fn find_repo_root(start: &Path) -> Option<PathBuf> {
    let mut current = Some(start);
    while let Some(path) = current {
        if path.join("Cargo.toml").is_file() && path.join("AGENTS.md").is_file() {
            return Some(path.to_path_buf());
        }
        current = path.parent();
    }
    None
}

pub fn current_support_catalog() -> MqliteSupportCatalog {
    MqliteSupportCatalog {
        query_operators: sorted_names(SUPPORTED_QUERY_OPERATORS),
        aggregation_stages: sorted_names(SUPPORTED_AGGREGATION_STAGES),
        aggregation_expression_operators: sorted_names(SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS),
        aggregation_accumulator_operators: sorted_names(SUPPORTED_AGGREGATION_ACCUMULATORS),
        aggregation_window_operators: sorted_names(SUPPORTED_AGGREGATION_WINDOW_OPERATORS),
    }
}

pub fn extract_upstream_catalog(repo_root: &Path) -> Result<UpstreamCatalog> {
    let query_parser = repo_root.join(QUERY_OPERATOR_SOURCE);
    let query_source = fs::read_to_string(&query_parser).with_context(|| {
        format!(
            "failed to read MongoDB query parser at {}",
            query_parser.display()
        )
    })?;

    let pathless_section = extract_section(
        &query_source,
        "pathlessOperatorMap = std::make_unique",
        "// Maps from query operator string name to operator PathAcceptingKeyword.",
    )?;
    let path_accepting_section = extract_section(
        &query_source,
        "queryOperatorMap =",
        "/**\n * Returns the proper parser for the indicated pathless operator.",
    )?;
    let entry_regex = Regex::new(r#"\{\s*"([^"]+)"\s*,"#).expect("valid regex");

    let mut query_accumulator = BTreeMap::new();
    collect_query_items(
        &mut query_accumulator,
        &entry_regex,
        pathless_section,
        "pathless",
        &query_parser,
        repo_root,
    );
    collect_query_items(
        &mut query_accumulator,
        &entry_regex,
        path_accepting_section,
        "path_accepting",
        &query_parser,
        repo_root,
    );
    add_item(
        &mut query_accumulator,
        "$not",
        "special_field",
        relative_workspace_path(repo_root, &query_parser),
        "parseSubField special case".to_string(),
        false,
        false,
    );

    let pipeline_root = repo_root.join("../mongo/src/mongo/db/pipeline");
    let pipeline_files = collect_pipeline_source_files(&pipeline_root)?;

    let stage_regex = Regex::new(
        r#"(?m)(REGISTER_[A-Z_]*DOCUMENT_SOURCE(?:_[A-Z_]+)?)\(\s*([A-Za-z_][A-Za-z0-9_]*)"#,
    )
    .expect("valid regex");
    let expression_regex =
        Regex::new(r#"(?m)(REGISTER_[A-Z_]*EXPRESSION(?:_[A-Z_]+)?)\(\s*([A-Za-z_][A-Za-z0-9_]*)"#)
            .expect("valid regex");
    let accumulator_regex =
        Regex::new(r#"(?m)(REGISTER_ACCUMULATOR(?:_[A-Z_]+)?)\(\s*([A-Za-z_][A-Za-z0-9_]*)"#)
            .expect("valid regex");
    let window_regex = Regex::new(
        r#"(?m)(REGISTER_[A-Z_]*WINDOW_FUNCTION(?:_[A-Z_]+)?)\(\s*([A-Za-z_][A-Za-z0-9_]*)"#,
    )
    .expect("valid regex");

    let aggregation_stages = collect_macro_category(
        repo_root,
        &pipeline_files,
        &stage_regex,
        "stage_registration",
        true,
    )?;
    let aggregation_expression_operators = collect_macro_category(
        repo_root,
        &pipeline_files,
        &expression_regex,
        "expression_registration",
        true,
    )?;
    let aggregation_accumulator_operators = collect_macro_category(
        repo_root,
        &pipeline_files,
        &accumulator_regex,
        "accumulator_registration",
        true,
    )?;
    let aggregation_window_operators = collect_macro_category(
        repo_root,
        &pipeline_files,
        &window_regex,
        "window_registration",
        true,
    )?;

    Ok(UpstreamCatalog {
        reference_anchors: reference_anchors(),
        query_operators: finalize_items(query_accumulator),
        aggregation_stages,
        aggregation_expression_operators,
        aggregation_accumulator_operators,
        aggregation_window_operators,
    })
}

pub fn build_gap_analysis(upstream: &UpstreamCatalog) -> GapAnalysis {
    let support = current_support_catalog();
    let set_window_fields_supported = support
        .aggregation_stages
        .iter()
        .any(|stage| stage == "$setWindowFields");
    GapAnalysis {
        reference_anchors: upstream.reference_anchors.clone(),
        query_operators: build_gap_category(
            &upstream.query_operators,
            &support.query_operators,
            |_item| false,
            |_item| ValidationMode::Rejection,
        ),
        aggregation_stages: build_gap_category(
            &upstream.aggregation_stages,
            &support.aggregation_stages,
            |item| item.feature_flagged || IGNORED_AGGREGATION_STAGES.contains(&item.name.as_str()),
            |_item| ValidationMode::Rejection,
        ),
        aggregation_expression_operators: build_gap_category(
            &upstream.aggregation_expression_operators,
            &support.aggregation_expression_operators,
            |item| item.feature_flagged,
            |_item| ValidationMode::Rejection,
        ),
        aggregation_accumulator_operators: build_gap_category(
            &upstream.aggregation_accumulator_operators,
            &support.aggregation_accumulator_operators,
            |_item| false,
            |_item| ValidationMode::Rejection,
        ),
        aggregation_window_operators: build_gap_category(
            &upstream.aggregation_window_operators,
            &support.aggregation_window_operators,
            |_item| false,
            |_item| {
                if set_window_fields_supported {
                    ValidationMode::Rejection
                } else {
                    ValidationMode::BlockedByStage {
                        stage: "$setWindowFields".to_string(),
                    }
                }
            },
        ),
    }
}

pub fn render_artifacts_from_upstream(repo_root: &Path) -> Result<RenderedArtifacts> {
    let upstream = extract_upstream_catalog(repo_root)?;
    Ok(render_artifacts(upstream))
}

pub fn render_artifacts(upstream: UpstreamCatalog) -> RenderedArtifacts {
    let support = current_support_catalog();
    let gap = build_gap_analysis(&upstream);
    let upstream_json = json_string(&upstream);
    let support_json = json_string(&support);
    let gap_json = json_string(&gap);
    let gap_markdown = render_gap_markdown(&gap);
    RenderedArtifacts {
        upstream,
        support,
        gap,
        upstream_json,
        support_json,
        gap_json,
        gap_markdown,
    }
}

pub fn write_artifacts(repo_root: &Path, artifacts: &RenderedArtifacts) -> Result<()> {
    write_file(repo_root, UPSTREAM_JSON_PATH, &artifacts.upstream_json)?;
    write_file(repo_root, SUPPORT_JSON_PATH, &artifacts.support_json)?;
    write_file(repo_root, GAP_JSON_PATH, &artifacts.gap_json)?;
    write_file(repo_root, GAP_MARKDOWN_PATH, &artifacts.gap_markdown)?;
    Ok(())
}

pub fn check_artifacts(repo_root: &Path, artifacts: &RenderedArtifacts) -> Result<()> {
    check_file(repo_root, UPSTREAM_JSON_PATH, &artifacts.upstream_json)?;
    check_file(repo_root, SUPPORT_JSON_PATH, &artifacts.support_json)?;
    check_file(repo_root, GAP_JSON_PATH, &artifacts.gap_json)?;
    check_file(repo_root, GAP_MARKDOWN_PATH, &artifacts.gap_markdown)?;
    Ok(())
}

pub fn load_upstream_snapshot(repo_root: &Path) -> Result<UpstreamCatalog> {
    load_json(repo_root.join(UPSTREAM_JSON_PATH))
}

pub fn load_support_snapshot(repo_root: &Path) -> Result<MqliteSupportCatalog> {
    load_json(repo_root.join(SUPPORT_JSON_PATH))
}

pub fn load_gap_snapshot(repo_root: &Path) -> Result<GapAnalysis> {
    load_json(repo_root.join(GAP_JSON_PATH))
}

fn reference_anchors() -> ReferenceAnchors {
    ReferenceAnchors {
        operator_sources: vec![
            QUERY_OPERATOR_SOURCE.to_string(),
            "../mongo/src/mongo/db/pipeline/expression.cpp".to_string(),
            "../mongo/src/mongo/db/pipeline".to_string(),
        ],
        command_idl_anchors: vec![
            FIND_COMMAND_IDL.to_string(),
            AGGREGATE_COMMAND_IDL.to_string(),
            WRITE_OPS_IDL.to_string(),
        ],
    }
}

fn collect_query_items(
    target: &mut BTreeMap<String, CapabilityAccumulator>,
    entry_regex: &Regex,
    section: &str,
    source_kind: &str,
    source_file: &Path,
    repo_root: &Path,
) {
    let source_path = relative_workspace_path(repo_root, source_file);
    for capture in entry_regex.captures_iter(section) {
        let raw_name = capture.get(1).expect("operator name").as_str();
        add_item(
            target,
            &format!("${raw_name}"),
            source_kind,
            source_path.clone(),
            "initializer_map".to_string(),
            false,
            false,
        );
    }
}

fn collect_macro_category(
    repo_root: &Path,
    files: &[PathBuf],
    regex: &Regex,
    source_kind: &str,
    prefix_dollar: bool,
) -> Result<Vec<CapabilityItem>> {
    let mut items = BTreeMap::new();
    for file in files {
        let source = fs::read_to_string(file)
            .with_context(|| format!("failed to read pipeline source {}", file.display()))?;
        let source_path = relative_workspace_path(repo_root, file);
        for capture in regex.captures_iter(&source) {
            let macro_kind = capture.get(1).expect("macro kind").as_str().to_string();
            let raw_name = capture.get(2).expect("registration name").as_str();
            let name = if prefix_dollar {
                format!("${raw_name}")
            } else {
                raw_name.to_string()
            };
            add_item(
                &mut items,
                &name,
                source_kind,
                source_path.clone(),
                macro_kind.clone(),
                macro_kind.contains("FEATURE_FLAG"),
                macro_kind.contains("CONDITIONALLY"),
            );
        }
    }
    Ok(finalize_items(items))
}

fn add_item(
    target: &mut BTreeMap<String, CapabilityAccumulator>,
    name: &str,
    source_kind: &str,
    source_file: String,
    macro_kind: String,
    feature_flagged: bool,
    conditional: bool,
) {
    let entry = target.entry(name.to_string()).or_default();
    entry.source_kinds.insert(source_kind.to_string());
    entry.source_files.insert(source_file);
    entry.macro_kinds.insert(macro_kind);
    entry.feature_flagged |= feature_flagged;
    entry.conditional |= conditional;
}

fn finalize_items(items: BTreeMap<String, CapabilityAccumulator>) -> Vec<CapabilityItem> {
    items
        .into_iter()
        .filter(|(name, _)| !DBREF_PSEUDO_OPERATORS.contains(&name.as_str()))
        .map(|(name, entry)| {
            let macro_kinds = entry.macro_kinds.into_iter().collect::<Vec<_>>();
            CapabilityItem {
                internal: name.starts_with("$_")
                    || macro_kinds
                        .iter()
                        .any(|macro_kind| macro_kind.contains("REGISTER_INTERNAL")),
                name,
                source_kinds: entry.source_kinds.into_iter().collect(),
                source_files: entry.source_files.into_iter().collect(),
                macro_kinds,
                feature_flagged: entry.feature_flagged,
                conditional: entry.conditional,
            }
        })
        .collect()
}

fn build_gap_category(
    upstream: &[CapabilityItem],
    supported_names: &[String],
    is_ignored: impl Fn(&CapabilityItem) -> bool,
    unsupported_validation: impl Fn(&CapabilityItem) -> ValidationMode,
) -> GapCategory {
    let supported_set = supported_names.iter().cloned().collect::<BTreeSet<_>>();
    let items = upstream
        .iter()
        .map(|item| {
            let supported = supported_set.contains(&item.name);
            let ignored = !supported && !item.internal && is_ignored(item);
            GapItem {
                name: item.name.clone(),
                internal: item.internal,
                source_kinds: item.source_kinds.clone(),
                source_files: item.source_files.clone(),
                macro_kinds: item.macro_kinds.clone(),
                feature_flagged: item.feature_flagged,
                conditional: item.conditional,
                ignored,
                status: if supported {
                    SupportStatus::Supported
                } else {
                    SupportStatus::Unsupported
                },
                validation: if supported {
                    ValidationMode::Positive
                } else {
                    unsupported_validation(item)
                },
            }
        })
        .collect::<Vec<_>>();

    let total_upstream = items.len();
    let public_upstream = items.iter().filter(|item| !item.internal).count();
    let public_ignored = items
        .iter()
        .filter(|item| !item.internal && item.ignored)
        .count();
    let total_supported = items
        .iter()
        .filter(|item| item.status == SupportStatus::Supported)
        .count();
    let public_supported = items
        .iter()
        .filter(|item| !item.internal && item.status == SupportStatus::Supported)
        .count();

    GapCategory {
        total_upstream,
        public_upstream,
        total_supported,
        public_supported,
        total_unsupported: total_upstream - total_supported,
        public_unsupported: public_upstream - public_supported - public_ignored,
        public_ignored,
        items,
    }
}

fn render_gap_markdown(gap: &GapAnalysis) -> String {
    let mut output = String::new();
    output.push_str("# MongoDB Capability Gap Analysis\n\n");
    output.push_str(
        "This report is generated from MongoDB source registries for query and aggregation capabilities.\n",
    );
    output.push_str(
        "The command IDL files are included as reference anchors for command shapes, but the operator and stage lists come from the source registries that actually register them.\n\n",
    );
    output.push_str("Resync from a sibling `mongo` checkout with:\n\n");
    output.push_str("```text\ncargo run -p mqlite-capabilities -- sync\n```\n\n");
    output.push_str("Check the checked-in artifacts without rewriting them with:\n\n");
    output.push_str("```text\ncargo run -p mqlite-capabilities -- sync --check\n```\n\n");
    output.push_str("## Reference Anchors\n\n");
    for anchor in &gap.reference_anchors.operator_sources {
        output.push_str(&format!("- `{anchor}`\n"));
    }
    for anchor in &gap.reference_anchors.command_idl_anchors {
        output.push_str(&format!("- `{anchor}`\n"));
    }
    output.push('\n');
    output.push_str("## Summary\n\n");
    output.push_str("| Category | Public upstream | Supported | Unsupported | Ignored |\n");
    output.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    append_summary_row(&mut output, "Query operators", &gap.query_operators);
    append_summary_row(&mut output, "Aggregation stages", &gap.aggregation_stages);
    append_summary_row(
        &mut output,
        "Aggregation expression operators",
        &gap.aggregation_expression_operators,
    );
    append_summary_row(
        &mut output,
        "Aggregation accumulators",
        &gap.aggregation_accumulator_operators,
    );
    append_summary_row(
        &mut output,
        "Aggregation window functions",
        &gap.aggregation_window_operators,
    );
    output.push('\n');

    append_category_section(&mut output, "Query Operators", &gap.query_operators);
    append_category_section(&mut output, "Aggregation Stages", &gap.aggregation_stages);
    append_category_section(
        &mut output,
        "Aggregation Expression Operators",
        &gap.aggregation_expression_operators,
    );
    append_category_section(
        &mut output,
        "Aggregation Accumulators",
        &gap.aggregation_accumulator_operators,
    );
    append_category_section(
        &mut output,
        "Aggregation Window Functions",
        &gap.aggregation_window_operators,
    );

    output
}

fn append_summary_row(output: &mut String, label: &str, category: &GapCategory) {
    output.push_str(&format!(
        "| {label} | {} | {} | {} | {} |\n",
        category.public_upstream,
        category.public_supported,
        category.public_unsupported,
        category.public_ignored
    ));
}

fn append_category_section(output: &mut String, title: &str, category: &GapCategory) {
    output.push_str(&format!("## {title}\n\n"));
    output.push_str(&format!(
        "Public upstream: {}. Supported: {}. Unsupported: {}. Ignored: {}.\n\n",
        category.public_upstream,
        category.public_supported,
        category.public_unsupported,
        category.public_ignored
    ));

    append_item_list(
        output,
        "### Supported Public\n\n",
        category
            .items
            .iter()
            .filter(|item| !item.internal && item.status == SupportStatus::Supported),
    );
    append_item_list(
        output,
        "### Unsupported Public\n\n",
        category.items.iter().filter(|item| {
            !item.internal && item.status == SupportStatus::Unsupported && !item.ignored
        }),
    );
    append_item_list(
        output,
        "### Ignored Public\n\n",
        category
            .items
            .iter()
            .filter(|item| !item.internal && item.ignored),
    );
    append_item_list(
        output,
        "### Internal Or Server-Only Upstream\n\n",
        category.items.iter().filter(|item| item.internal),
    );
}

fn append_item_list<'a>(
    output: &mut String,
    heading: &str,
    items: impl Iterator<Item = &'a GapItem>,
) {
    let items = items.collect::<Vec<_>>();
    output.push_str(heading);
    if items.is_empty() {
        output.push_str("- None\n\n");
        return;
    }
    for item in items {
        output.push_str(&format!("- {}\n", render_item_label(item)));
    }
    output.push('\n');
}

fn render_item_label(item: &GapItem) -> String {
    let mut flags = Vec::new();
    if item.feature_flagged {
        flags.push("feature-flagged");
    }
    if item.conditional {
        flags.push("conditional");
    }
    if item.ignored {
        flags.push("ignored");
    }
    if flags.is_empty() {
        format!("`{}`", item.name)
    } else {
        format!("`{}` ({})", item.name, flags.join(", "))
    }
}

fn collect_pipeline_source_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    visit_pipeline_source_files(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn visit_pipeline_source_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| {
        format!(
            "failed to list pipeline source directory {}",
            root.display()
        )
    })? {
        let entry = entry.with_context(|| format!("failed to read entry in {}", root.display()))?;
        let path = entry.path();
        if path.is_dir() {
            visit_pipeline_source_files(&path, files)?;
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("cpp") {
            continue;
        }
        if is_test_source(&path) {
            continue;
        }
        files.push(path);
    }
    Ok(())
}

fn is_test_source(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let lower = name.to_ascii_lowercase();
    lower.contains("test") || lower.ends_with("_bm.cpp")
}

fn extract_section<'a>(source: &'a str, start_marker: &str, end_marker: &str) -> Result<&'a str> {
    let start = source
        .find(start_marker)
        .with_context(|| format!("failed to locate source marker `{start_marker}`"))?;
    let remaining = &source[start..];
    let end = remaining
        .find(end_marker)
        .with_context(|| format!("failed to locate source marker `{end_marker}`"))?;
    Ok(&remaining[..end])
}

fn json_string<T: Serialize>(value: &T) -> String {
    let mut json = serde_json::to_string_pretty(value).expect("serializable");
    json.push('\n');
    json
}

fn write_file(repo_root: &Path, relative_path: &str, contents: &str) -> Result<()> {
    let path = repo_root.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&path, contents).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn check_file(repo_root: &Path, relative_path: &str, expected: &str) -> Result<()> {
    let path = repo_root.join(relative_path);
    let actual =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    if actual != expected {
        bail!(
            "{} is stale; run `cargo run -p mqlite-capabilities -- sync`",
            relative_path
        );
    }
    Ok(())
}

fn load_json<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<T> {
    let contents =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse JSON from {}", path.display()))
}

fn sorted_names(values: &[&str]) -> Vec<String> {
    let mut names = values
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn relative_workspace_path(repo_root: &Path, path: &Path) -> String {
    let normalized_repo_root = normalize_components(repo_root);
    let normalized_path = normalize_components(path);
    let Some(workspace_root) = normalized_repo_root.parent() else {
        return normalize_path(&normalized_path);
    };

    if let Ok(relative) = normalized_path.strip_prefix(workspace_root) {
        return format!("../{}", normalize_path(relative));
    }

    normalize_path(&normalized_path)
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn normalize_components(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bson::{Bson, Document, doc};
    use pretty_assertions::assert_eq;

    use super::{
        GAP_MARKDOWN_PATH, SupportStatus, UPSTREAM_JSON_PATH, ValidationMode, add_item,
        build_gap_analysis, current_support_catalog, extract_upstream_catalog, finalize_items,
        find_repo_root, load_gap_snapshot, load_support_snapshot, load_upstream_snapshot,
        render_artifacts,
    };
    use mqlite_query::{
        QueryError, apply_projection, document_matches, parse_filter, run_pipeline,
    };

    fn repo_root() -> std::path::PathBuf {
        find_repo_root(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))).expect("repo root")
    }

    #[test]
    fn generated_support_and_gap_snapshots_match_current_code() {
        let repo_root = repo_root();
        let upstream = load_upstream_snapshot(&repo_root).expect("upstream snapshot");
        let support = load_support_snapshot(&repo_root).expect("support snapshot");
        let gap = load_gap_snapshot(&repo_root).expect("gap snapshot");
        let expected_support = current_support_catalog();
        let expected_gap = build_gap_analysis(&upstream);

        assert_eq!(support, expected_support);
        assert_eq!(gap, expected_gap);

        let markdown = std::fs::read_to_string(repo_root.join(GAP_MARKDOWN_PATH))
            .expect("gap markdown snapshot");
        assert_eq!(markdown, render_artifacts(upstream).gap_markdown);
    }

    #[test]
    fn upstream_snapshot_contains_expected_known_features() {
        let repo_root = repo_root();
        let upstream = load_upstream_snapshot(&repo_root).expect("upstream snapshot");

        assert!(
            upstream
                .query_operators
                .iter()
                .any(|item| item.name == "$and")
        );
        assert!(
            upstream
                .query_operators
                .iter()
                .any(|item| item.name == "$regex")
        );
        assert!(
            upstream
                .query_operators
                .iter()
                .any(|item| item.name == "$not")
        );
        assert!(
            upstream
                .aggregation_stages
                .iter()
                .any(|item| item.name == "$lookup")
        );
        assert!(
            upstream
                .aggregation_stages
                .iter()
                .any(|item| item.name == "$setWindowFields")
        );
        assert!(
            upstream
                .aggregation_expression_operators
                .iter()
                .any(|item| item.name == "$add")
        );
        assert!(
            upstream
                .aggregation_accumulator_operators
                .iter()
                .any(|item| item.name == "$sum")
        );
        assert!(
            upstream
                .aggregation_window_operators
                .iter()
                .any(|item| item.name == "$rank")
        );
        assert!(
            upstream
                .query_operators
                .iter()
                .all(|item| !matches!(item.name.as_str(), "$db" | "$id" | "$ref"))
        );
    }

    #[test]
    fn internally_registered_stages_are_not_counted_as_public() {
        let mut items = BTreeMap::new();
        add_item(
            &mut items,
            "$setMetadata",
            "stage_registration",
            "../mongo/src/mongo/db/pipeline/document_source_set_metadata.cpp".to_string(),
            "REGISTER_INTERNAL_LITE_PARSED_DOCUMENT_SOURCE".to_string(),
            false,
            false,
        );
        add_item(
            &mut items,
            "$setMetadata",
            "stage_registration",
            "../mongo/src/mongo/db/pipeline/document_source_set_metadata.cpp".to_string(),
            "REGISTER_DOCUMENT_SOURCE_WITH_STAGE_PARAMS_DEFAULT".to_string(),
            false,
            false,
        );

        let finalized = finalize_items(items);
        assert_eq!(finalized.len(), 1);
        assert!(finalized[0].internal);
    }

    #[test]
    fn ignored_aggregation_stages_are_not_counted_as_backlog() {
        let repo_root = repo_root();
        let upstream = load_upstream_snapshot(&repo_root).expect("upstream snapshot");
        let gap = build_gap_analysis(&upstream);

        assert!(
            gap.aggregation_stages
                .items
                .iter()
                .find(|item| item.name == "$search")
                .is_some_and(|item| item.ignored)
        );
        assert!(
            gap.aggregation_stages
                .items
                .iter()
                .find(|item| item.name == "$vectorSearch")
                .is_some_and(|item| item.ignored)
        );
        assert!(
            gap.aggregation_stages
                .items
                .iter()
                .find(|item| item.name == "$queryStats")
                .is_some_and(|item| item.ignored)
        );
        assert_eq!(gap.aggregation_stages.public_unsupported, 0);
    }

    #[test]
    fn ignored_feature_flagged_aggregation_expressions_are_not_counted_as_backlog() {
        let repo_root = repo_root();
        let upstream = load_upstream_snapshot(&repo_root).expect("upstream snapshot");
        let gap = build_gap_analysis(&upstream);

        assert!(
            gap.aggregation_expression_operators
                .items
                .iter()
                .find(|item| item.name == "$bottom")
                .is_some_and(|item| item.ignored)
        );
        assert!(
            gap.aggregation_expression_operators
                .items
                .iter()
                .find(|item| item.name == "$createObjectId")
                .is_some_and(|item| item.ignored)
        );
        assert!(
            gap.aggregation_expression_operators
                .items
                .iter()
                .find(|item| item.name == "$toUUID")
                .is_some_and(|item| item.ignored)
        );
    }

    #[test]
    fn query_operator_contracts_match_support_snapshot() {
        let repo_root = repo_root();
        let gap = load_gap_snapshot(&repo_root).expect("gap snapshot");

        for item in &gap.query_operators.items {
            match item.status {
                SupportStatus::Supported => {
                    let (document, filter) = supported_query_fixture(&item.name);
                    assert!(
                        document_matches(&document, &filter).expect("supported query matches"),
                        "{} should be accepted",
                        item.name
                    );
                }
                SupportStatus::Unsupported => {
                    assert_eq!(item.validation, ValidationMode::Rejection);
                    let filter = unsupported_query_fixture(item);
                    let error = parse_filter(&filter).expect_err("unsupported query operator");
                    assert!(matches!(
                        error,
                        QueryError::UnsupportedOperator(ref operator) if operator == &item.name
                    ));
                }
            }
        }
    }

    #[test]
    fn aggregation_stage_contracts_match_support_snapshot() {
        let repo_root = repo_root();
        let gap = load_gap_snapshot(&repo_root).expect("gap snapshot");

        for item in &gap.aggregation_stages.items {
            match item.status {
                SupportStatus::Supported => {
                    let (documents, pipeline) = supported_stage_fixture(&item.name);
                    run_pipeline(documents, &pipeline).expect("supported stage");
                }
                SupportStatus::Unsupported => {
                    assert_eq!(item.validation, ValidationMode::Rejection);
                    let mut stage = Document::new();
                    stage.insert(item.name.clone(), 1);
                    let error =
                        run_pipeline(vec![doc! { "value": 1 }], &[stage]).expect_err("unsupported");
                    assert!(matches!(
                        error,
                        QueryError::UnsupportedStage(ref stage) if stage == &item.name
                    ));
                }
            }
        }
    }

    #[test]
    fn aggregation_expression_contracts_match_support_snapshot() {
        let repo_root = repo_root();
        let gap = load_gap_snapshot(&repo_root).expect("gap snapshot");
        let document = doc! {
            "_id": 1,
            "left": 5,
            "right": 3,
            "text": "abc",
            "array": [1, 2, 3],
            "emptyArray": [],
            "object": { "a": 1, "b": 2 },
            "pairs": [["price", 24], ["item", "apple"]]
        };

        for item in &gap.aggregation_expression_operators.items {
            match item.status {
                SupportStatus::Supported => {
                    let projection = supported_expression_projection(&item.name);
                    apply_projection(&document, Some(&projection))
                        .expect("supported expression operator");
                }
                SupportStatus::Unsupported => {
                    assert_eq!(item.validation, ValidationMode::Rejection);
                    let projection = unsupported_expression_projection(&item.name);
                    let error = apply_projection(&document, Some(&projection))
                        .expect_err("unsupported expression operator");
                    assert!(
                        matches!(
                            error,
                            QueryError::UnsupportedOperator(ref operator) if operator == &item.name
                        ),
                        "{} rejected with {:?}",
                        item.name,
                        error
                    );
                }
            }
        }
    }

    #[test]
    fn aggregation_accumulator_contracts_match_support_snapshot() {
        let repo_root = repo_root();
        let gap = load_gap_snapshot(&repo_root).expect("gap snapshot");

        for item in &gap.aggregation_accumulator_operators.items {
            match item.status {
                SupportStatus::Supported => {
                    let (documents, pipeline) = supported_accumulator_fixture(&item.name);
                    run_pipeline(documents, &pipeline).expect("supported accumulator");
                }
                SupportStatus::Unsupported => {
                    assert_eq!(item.validation, ValidationMode::Rejection);
                    let mut accumulator = Document::new();
                    accumulator.insert(item.name.clone(), 1);
                    let mut group_spec = Document::new();
                    group_spec.insert("_id", Bson::Null);
                    group_spec.insert("value", Bson::Document(accumulator));
                    let error = run_pipeline(
                        vec![doc! { "value": 1 }],
                        &[doc! { "$group": Bson::Document(group_spec) }],
                    )
                    .expect_err("unsupported accumulator");
                    assert!(matches!(
                        error,
                        QueryError::UnsupportedOperator(ref operator) if operator == &item.name
                    ));
                }
            }
        }
    }

    #[test]
    fn aggregation_window_function_contracts_match_support_snapshot() {
        let repo_root = repo_root();
        let gap = load_gap_snapshot(&repo_root).expect("gap snapshot");

        for item in &gap.aggregation_window_operators.items {
            match item.status {
                SupportStatus::Supported => {
                    let (documents, pipeline) = supported_window_fixture(&item.name);
                    run_pipeline(documents, &pipeline).expect("supported window function");
                }
                SupportStatus::Unsupported => {
                    assert_eq!(item.validation, ValidationMode::Rejection);
                    let error = run_pipeline(
                        vec![doc! { "_id": 1, "qty": 1 }],
                        &[unsupported_window_stage(&item.name)],
                    )
                    .expect_err("unsupported window function");
                    assert!(matches!(error, QueryError::InvalidStage));
                }
            }
        }
    }

    #[test]
    fn extracted_upstream_snapshot_matches_checked_in_files_when_mongo_checkout_is_present() {
        let repo_root = repo_root();
        if !repo_root.join("../mongo").exists() {
            return;
        }

        let extracted = extract_upstream_catalog(&repo_root).expect("extract upstream catalog");
        let checked_in = load_upstream_snapshot(&repo_root).expect("checked in upstream catalog");

        assert_eq!(checked_in, extracted);
        assert!(repo_root.join(UPSTREAM_JSON_PATH).is_file());
    }

    fn supported_query_fixture(name: &str) -> (Document, Document) {
        match name {
            "$alwaysFalse" => (doc! { "qty": 5 }, doc! { "$nor": [{ "$alwaysFalse": 1 }] }),
            "$alwaysTrue" => (doc! { "qty": 5 }, doc! { "$alwaysTrue": 1 }),
            "$and" => (
                doc! { "qty": 5, "sku": "abc" },
                doc! { "$and": [{ "qty": { "$gte": 5 } }, { "sku": "abc" }] },
            ),
            "$bitsAllClear" => (doc! { "qty": 54 }, doc! { "qty": { "$bitsAllClear": 129 } }),
            "$bitsAllSet" => (doc! { "qty": 54 }, doc! { "qty": { "$bitsAllSet": 54 } }),
            "$bitsAnyClear" => (doc! { "qty": 54 }, doc! { "qty": { "$bitsAnyClear": 9 } }),
            "$bitsAnySet" => (doc! { "qty": 54 }, doc! { "qty": { "$bitsAnySet": 18 } }),
            "$or" => (
                doc! { "qty": 5, "sku": "abc" },
                doc! { "$or": [{ "qty": { "$lt": 0 } }, { "sku": "abc" }] },
            ),
            "$nor" => (
                doc! { "qty": 5, "sku": "abc" },
                doc! { "$nor": [{ "qty": { "$lt": 0 } }, { "sku": "missing" }] },
            ),
            "$eq" => (doc! { "qty": 5 }, doc! { "qty": { "$eq": 5 } }),
            "$ne" => (doc! { "qty": 5 }, doc! { "qty": { "$ne": 4 } }),
            "$gt" => (doc! { "qty": 5 }, doc! { "qty": { "$gt": 4 } }),
            "$gte" => (doc! { "qty": 5 }, doc! { "qty": { "$gte": 5 } }),
            "$lt" => (doc! { "qty": 5 }, doc! { "qty": { "$lt": 6 } }),
            "$lte" => (doc! { "qty": 5 }, doc! { "qty": { "$lte": 5 } }),
            "$in" => (
                doc! { "sku": "abc" },
                doc! { "sku": { "$in": ["def", "abc"] } },
            ),
            "$nin" => (
                doc! { "sku": "abc" },
                doc! { "sku": { "$nin": ["def", "ghi"] } },
            ),
            "$exists" => (
                doc! { "meta": { "enabled": true } },
                doc! { "meta.enabled": { "$exists": true } },
            ),
            "$size" => (
                doc! { "tags": ["red", "blue"] },
                doc! { "tags": { "$size": 2 } },
            ),
            "$mod" => (doc! { "qty": 12 }, doc! { "qty": { "$mod": [5, 2] } }),
            "$all" => (
                doc! { "tags": ["red", "blue"] },
                doc! { "tags": { "$all": ["red", "blue"] } },
            ),
            "$comment" => (
                doc! { "qty": 5, "sku": "abc" },
                doc! { "qty": 5, "$comment": "metadata only" },
            ),
            "$not" => (
                doc! { "qty": 12 },
                doc! { "qty": { "$not": { "$mod": [5, 1] } } },
            ),
            "$type" => (doc! { "sku": "abc" }, doc! { "sku": { "$type": "string" } }),
            "$regex" => (doc! { "name": "Ada" }, doc! { "name": { "$regex": "^A" } }),
            "$options" => (
                doc! { "name": "Ada" },
                doc! { "name": { "$regex": "^a", "$options": "i" } },
            ),
            "$elemMatch" => (
                doc! { "values": [3, 5, 7] },
                doc! { "values": { "$elemMatch": { "$lt": 6, "$gt": 4 } } },
            ),
            "$expr" => (
                doc! { "qty": 5, "limit": 4 },
                doc! { "$expr": { "$gt": ["$qty", "$limit"] } },
            ),
            "$sampleRate" => (doc! { "qty": 5 }, doc! { "$sampleRate": 1.0 }),
            other => panic!("missing supported query fixture for {other}"),
        }
    }

    fn unsupported_query_fixture(item: &super::GapItem) -> Document {
        if item.source_kinds.iter().any(|kind| kind == "pathless") {
            let mut filter = Document::new();
            filter.insert(item.name.clone(), 1);
            return filter;
        }

        let mut inner = Document::new();
        inner.insert(item.name.clone(), 1);
        let mut filter = Document::new();
        filter.insert("field", Bson::Document(inner));
        filter
    }

    fn supported_stage_fixture(name: &str) -> (Vec<Document>, Vec<Document>) {
        match name {
            "$bucket" => (
                vec![doc! { "price": 10 }],
                vec![doc! { "$bucket": { "groupBy": "$price", "boundaries": [0, 20] } }],
            ),
            "$bucketAuto" => (
                vec![doc! { "price": 10 }, doc! { "price": 20 }],
                vec![doc! { "$bucketAuto": { "groupBy": "$price", "buckets": 2 } }],
            ),
            "$changeStream" => (Vec::new(), vec![doc! { "$changeStream": {} }]),
            "$changeStreamSplitLargeEvent" => (
                Vec::new(),
                vec![
                    doc! { "$changeStream": {} },
                    doc! { "$changeStreamSplitLargeEvent": {} },
                ],
            ),
            "$collStats" => (
                vec![doc! { "_id": 1 }, doc! { "_id": 2 }],
                vec![doc! { "$collStats": { "count": {}, "storageStats": {} } }],
            ),
            "$currentOp" => (
                Vec::new(),
                vec![doc! { "$currentOp": { "localOps": true } }],
            ),
            "$densify" => (
                vec![doc! { "val": 0 }, doc! { "val": 2 }],
                vec![
                    doc! { "$densify": { "field": "val", "range": { "step": 1, "bounds": "full" } } },
                ],
            ),
            "$indexStats" => (Vec::new(), vec![doc! { "$indexStats": {} }]),
            "$documents" => (
                vec![doc! { "ignored": true }],
                vec![doc! { "$documents": [{ "qty": 1 }] }],
            ),
            "$facet" => (
                vec![doc! { "qty": 1 }],
                vec![doc! { "$facet": { "all": [{ "$project": { "qty": 1 } }] } }],
            ),
            "$fill" => (
                vec![
                    doc! { "_id": 1, "qty": 1 },
                    doc! { "_id": 2, "qty": Bson::Null },
                    doc! { "_id": 3, "qty": 3 },
                ],
                vec![
                    doc! { "$fill": { "sortBy": { "_id": 1 }, "output": { "qty": { "method": "linear" } } } },
                ],
            ),
            "$graphLookup" => (
                vec![doc! { "start": 1 }],
                vec![doc! {
                    "$graphLookup": {
                        "from": "foreign",
                        "startWith": "$start",
                        "connectFromField": "neighbors",
                        "connectToField": "_id",
                        "as": "matches"
                    }
                }],
            ),
            "$geoNear" => (
                vec![doc! { "loc": [0.0, 0.0] }, doc! { "loc": [1.0, 0.0] }],
                vec![doc! {
                    "$geoNear": {
                        "near": [0.0, 0.0],
                        "key": "loc",
                        "distanceField": "dist"
                    }
                }],
            ),
            "$lookup" => (
                vec![doc! { "wanted": 2 }],
                vec![doc! {
                    "$lookup": {
                        "as": "matches",
                        "let": { "wanted": "$wanted" },
                        "pipeline": [
                            { "$documents": [{ "x": 1 }, { "x": 2 }] },
                            { "$match": { "$expr": { "$eq": ["$$wanted", "$x"] } } }
                        ]
                    }
                }],
            ),
            "$listCatalog" => (Vec::new(), vec![doc! { "$listCatalog": {} }]),
            "$listClusterCatalog" => (
                Vec::new(),
                vec![
                    doc! { "$listClusterCatalog": { "shards": true, "tracked": true, "balancingConfiguration": true } },
                ],
            ),
            "$listCachedAndActiveUsers" => (
                vec![doc! { "_id": 1 }],
                vec![doc! { "$listCachedAndActiveUsers": {} }],
            ),
            "$listLocalSessions" => (
                Vec::new(),
                vec![doc! { "$listLocalSessions": { "allUsers": true } }],
            ),
            "$listSampledQueries" => (
                Vec::new(),
                vec![doc! { "$listSampledQueries": { "namespace": "app.widgets" } }],
            ),
            "$listSearchIndexes" => (
                Vec::new(),
                vec![doc! { "$listSearchIndexes": { "name": "search-index" } }],
            ),
            "$listSessions" => (
                Vec::new(),
                vec![doc! { "$listSessions": { "allUsers": true } }],
            ),
            "$listMqlEntities" => (
                Vec::new(),
                vec![doc! { "$listMqlEntities": { "entityType": "aggregationStages" } }],
            ),
            "$merge" => (vec![doc! { "_id": 1 }], vec![doc! { "$merge": "archive" }]),
            "$out" => (vec![doc! { "_id": 1 }], vec![doc! { "$out": "archive" }]),
            "$planCacheStats" => (Vec::new(), vec![doc! { "$planCacheStats": {} }]),
            "$sample" => (
                vec![doc! { "_id": 1 }],
                vec![doc! { "$sample": { "size": 1 } }],
            ),
            "$setWindowFields" => (
                vec![
                    doc! { "_id": 1, "team": "a", "seq": 0, "qty": 1 },
                    doc! { "_id": 2, "team": "a", "seq": 1, "qty": 2 },
                ],
                vec![doc! {
                    "$setWindowFields": {
                        "partitionBy": "$team",
                        "sortBy": { "seq": 1 },
                        "output": {
                            "runningQty": {
                                "$sum": "$qty",
                                "window": { "documents": ["unbounded", "current"] }
                            }
                        }
                    }
                }],
            ),
            "$sortByCount" => (
                vec![doc! { "team": "red" }],
                vec![doc! { "$sortByCount": "$team" }],
            ),
            "$unionWith" => (
                vec![doc! { "_id": "base" }],
                vec![
                    doc! { "$unionWith": { "pipeline": [{ "$documents": [{ "_id": "other" }] }] } },
                ],
            ),
            "$match" => (
                vec![doc! { "qty": 1 }],
                vec![doc! { "$match": { "qty": { "$gte": 1 } } }],
            ),
            "$project" => (
                vec![doc! { "_id": 1, "qty": 1 }],
                vec![doc! { "$project": { "qty": 1 } }],
            ),
            "$querySettings" => (
                Vec::new(),
                vec![doc! { "$querySettings": { "showDebugQueryShape": true } }],
            ),
            "$redact" => (
                vec![doc! { "_id": 1, "qty": 1 }],
                vec![doc! { "$redact": "$$KEEP" }],
            ),
            "$set" => (
                vec![doc! { "_id": 1, "qty": 1 }],
                vec![doc! { "$set": { "copied": "$qty" } }],
            ),
            "$addFields" => (
                vec![doc! { "_id": 1, "qty": 1 }],
                vec![doc! { "$addFields": { "copied": "$qty" } }],
            ),
            "$unset" => (
                vec![doc! { "_id": 1, "qty": 1 }],
                vec![doc! { "$unset": "qty" }],
            ),
            "$limit" => (
                vec![doc! { "_id": 1 }, doc! { "_id": 2 }],
                vec![doc! { "$limit": 1 }],
            ),
            "$skip" => (
                vec![doc! { "_id": 1 }, doc! { "_id": 2 }],
                vec![doc! { "$skip": 1 }],
            ),
            "$sort" => (
                vec![doc! { "qty": 2 }, doc! { "qty": 1 }],
                vec![doc! { "$sort": { "qty": 1 } }],
            ),
            "$count" => (
                vec![doc! { "_id": 1 }, doc! { "_id": 2 }],
                vec![doc! { "$count": "total" }],
            ),
            "$unwind" => (
                vec![doc! { "tags": ["a", "b"] }],
                vec![doc! { "$unwind": "$tags" }],
            ),
            "$group" => (
                vec![doc! { "team": "red", "qty": 1 }],
                vec![doc! { "$group": { "_id": "$team", "total": { "$sum": "$qty" } } }],
            ),
            "$replaceRoot" => (
                vec![doc! { "payload": { "qty": 1 } }],
                vec![doc! { "$replaceRoot": { "newRoot": "$payload" } }],
            ),
            "$replaceWith" => (
                vec![doc! { "payload": { "qty": 1 } }],
                vec![doc! { "$replaceWith": "$payload" }],
            ),
            other => panic!("missing supported stage fixture for {other}"),
        }
    }

    fn supported_expression_projection(name: &str) -> Document {
        match name {
            "$abs" => doc! { "value": { "$abs": -5 } },
            "$add" => doc! { "value": { "$add": ["$left", "$right", 2] } },
            "$allElementsTrue" => doc! { "value": { "$allElementsTrue": [true, 1, "ok"] } },
            "$and" => doc! { "value": { "$and": [true, { "$eq": ["$left", "$left"] }] } },
            "$anyElementTrue" => doc! { "value": { "$anyElementTrue": [0, false, "ok"] } },
            "$arrayElemAt" => doc! { "value": { "$arrayElemAt": ["$array", 1] } },
            "$arrayToObject" => doc! { "value": { "$arrayToObject": "$pairs" } },
            "$bitAnd" => doc! { "value": { "$bitAnd": ["$left", 6] } },
            "$bitNot" => doc! { "value": { "$bitNot": "$left" } },
            "$bitOr" => doc! { "value": { "$bitOr": ["$left", 2] } },
            "$bitXor" => doc! { "value": { "$bitXor": ["$left", 1] } },
            "$ceil" => doc! { "value": { "$ceil": 2.2 } },
            "$cmp" => doc! { "value": { "$cmp": ["$left", "$right"] } },
            "$concat" => doc! { "value": { "$concat": ["ab", "cd"] } },
            "$concatArrays" => doc! { "value": { "$concatArrays": ["$array", [4, 5]] } },
            "$cond" => doc! { "value": { "$cond": [{ "$eq": ["$left", "$left"] }, "yes", "no"] } },
            "$const" => doc! { "value": { "$const": 5 } },
            "$divide" => doc! { "value": { "$divide": [9, 3] } },
            "$eq" => doc! { "value": { "$eq": ["$left", "$right"] } },
            "$expr" => doc! { "value": { "$expr": { "$eq": ["$left", "$left"] } } },
            "$filter" => {
                doc! { "value": { "$filter": { "input": "$array", "as": "value", "cond": { "$gt": ["$$value", 1] } } } }
            }
            "$first" => doc! { "value": { "$first": "$array" } },
            "$floor" => doc! { "value": { "$floor": 2.8 } },
            "$getField" => doc! { "value": { "$getField": { "field": "a", "input": "$object" } } },
            "$gt" => doc! { "value": { "$gt": ["$left", "$right"] } },
            "$gte" => doc! { "value": { "$gte": ["$left", "$right"] } },
            "$ifNull" => doc! { "value": { "$ifNull": [null, "$left"] } },
            "$in" => doc! { "value": { "$in": ["$left", [1, 5, 9]] } },
            "$indexOfArray" => doc! { "value": { "$indexOfArray": ["$array", 2] } },
            "$isArray" => doc! { "value": { "$isArray": "$array" } },
            "$isNumber" => doc! { "value": { "$isNumber": "$left" } },
            "$last" => doc! { "value": { "$last": "$array" } },
            "$let" => {
                doc! { "value": { "$let": { "vars": { "factor": 10 }, "in": { "$add": ["$left", "$$factor"] } } } }
            }
            "$literal" => doc! { "value": { "$literal": 5 } },
            "$lt" => doc! { "value": { "$lt": ["$left", "$right"] } },
            "$lte" => doc! { "value": { "$lte": ["$left", "$right"] } },
            "$map" => {
                doc! { "value": { "$map": { "input": "$array", "as": "value", "in": { "$add": ["$$value", 1] } } } }
            }
            "$mergeObjects" => doc! { "value": { "$mergeObjects": ["$object", { "c": 3 }] } },
            "$mod" => doc! { "value": { "$mod": [17, 5] } },
            "$multiply" => doc! { "value": { "$multiply": ["$left", 2] } },
            "$ne" => doc! { "value": { "$ne": ["$left", "$right"] } },
            "$not" => doc! { "value": { "$not": [{ "$eq": ["$left", "$right"] }] } },
            "$objectToArray" => doc! { "value": { "$objectToArray": "$object" } },
            "$or" => doc! { "value": { "$or": [false, { "$eq": ["$left", "$left"] }] } },
            "$range" => doc! { "value": { "$range": [0, 5, 2] } },
            "$reduce" => {
                doc! { "value": { "$reduce": { "input": "$array", "initialValue": 0, "in": { "$add": ["$$value", "$$this"] } } } }
            }
            "$reverseArray" => doc! { "value": { "$reverseArray": "$array" } },
            "$round" => doc! { "value": { "$round": [2.65, 1] } },
            "$setDifference" => {
                doc! { "value": { "$setDifference": ["$array", [2, 4]] } }
            }
            "$setEquals" => {
                doc! { "value": { "$setEquals": ["$array", [1, 2, 3, 2]] } }
            }
            "$setIntersection" => {
                doc! { "value": { "$setIntersection": ["$array", [2, 4]] } }
            }
            "$setIsSubset" => {
                doc! { "value": { "$setIsSubset": [[2, 3], "$array"] } }
            }
            "$setUnion" => {
                doc! { "value": { "$setUnion": ["$array", [3, 4]] } }
            }
            "$setField" => {
                doc! { "value": { "$setField": { "field": "a", "input": "$object", "value": 9 } } }
            }
            "$size" => doc! { "value": { "$size": "$array" } },
            "$slice" => doc! { "value": { "$slice": ["$array", 2] } },
            "$strcasecmp" => doc! { "value": { "$strcasecmp": ["Ab", "aB"] } },
            "$subtract" => doc! { "value": { "$subtract": ["$left", "$right"] } },
            "$switch" => {
                doc! { "value": { "$switch": { "branches": [{ "case": { "$eq": ["$left", "$left"] }, "then": "yes" }], "default": "no" } } }
            }
            "$toLower" => doc! { "value": { "$toLower": "AbC" } },
            "$toUpper" => doc! { "value": { "$toUpper": "AbC" } },
            "$type" => doc! { "value": { "$type": "$left" } },
            "$trunc" => doc! { "value": { "$trunc": [2.65, 1] } },
            "$unsetField" => {
                doc! { "value": { "$unsetField": { "field": "a", "input": "$object" } } }
            }
            other => panic!("missing supported expression fixture for {other}"),
        }
    }

    fn unsupported_expression_projection(name: &str) -> Document {
        let mut expression = Document::new();
        expression.insert(name.to_string(), 1);
        let mut projection = Document::new();
        projection.insert("value", Bson::Document(expression));
        projection
    }

    fn supported_accumulator_fixture(name: &str) -> (Vec<Document>, Vec<Document>) {
        match name {
            "$sum" => (
                vec![doc! { "qty": 1 }, doc! { "qty": 2 }],
                vec![doc! { "$group": { "_id": Bson::Null, "value": { "$sum": "$qty" } } }],
            ),
            "$first" => (
                vec![doc! { "qty": 1 }, doc! { "qty": 2 }],
                vec![doc! { "$group": { "_id": Bson::Null, "value": { "$first": "$qty" } } }],
            ),
            "$push" => (
                vec![doc! { "qty": 1 }, doc! { "qty": 2 }],
                vec![doc! { "$group": { "_id": Bson::Null, "value": { "$push": "$qty" } } }],
            ),
            "$addToSet" => (
                vec![doc! { "qty": 1 }, doc! { "qty": 1 }, doc! { "qty": 2 }],
                vec![doc! { "$group": { "_id": Bson::Null, "value": { "$addToSet": "$qty" } } }],
            ),
            "$avg" => (
                vec![doc! { "qty": 1 }, doc! { "qty": 3 }],
                vec![doc! { "$group": { "_id": Bson::Null, "value": { "$avg": "$qty" } } }],
            ),
            other => panic!("missing supported accumulator fixture for {other}"),
        }
    }

    fn supported_window_fixture(name: &str) -> (Vec<Document>, Vec<Document>) {
        match name {
            "$sum" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 2 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$sum": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$avg" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 3 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$avg": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$first" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 3 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$first": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$last" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 3 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$last": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$push" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 2 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$push": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$addToSet" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 1 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$addToSet": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$min" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 3 },
                    doc! { "_id": 2, "seq": 2, "qty": 1 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$min": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$max" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 3 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$max": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$count" => (
                vec![doc! { "_id": 1, "seq": 1 }, doc! { "_id": 2, "seq": 2 }],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$count": {}, "window": { "documents": ["unbounded", "current"] } } } } },
                ],
            ),
            "$documentNumber" => (
                vec![doc! { "_id": 1, "seq": 1 }, doc! { "_id": 2, "seq": 2 }],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$documentNumber": {} } } } },
                ],
            ),
            "$rank" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "score": 1 },
                    doc! { "_id": 2, "seq": 2, "score": 1 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "score": 1 }, "output": { "value": { "$rank": {} } } } },
                ],
            ),
            "$denseRank" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "score": 1 },
                    doc! { "_id": 2, "seq": 2, "score": 2 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "score": 1 }, "output": { "value": { "$denseRank": {} } } } },
                ],
            ),
            "$shift" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": 2 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$shift": { "output": "$qty", "by": -1, "default": 0 } } } } },
                ],
            ),
            "$locf" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": Bson::Null },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$locf": "$qty" } } } },
                ],
            ),
            "$linearFill" => (
                vec![
                    doc! { "_id": 1, "seq": 1, "qty": 1 },
                    doc! { "_id": 2, "seq": 2, "qty": Bson::Null },
                    doc! { "_id": 3, "seq": 3, "qty": 3 },
                ],
                vec![
                    doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "value": { "$linearFill": "$qty" } } } },
                ],
            ),
            other => panic!("missing supported window fixture for {other}"),
        }
    }

    fn unsupported_window_stage(name: &str) -> Document {
        let mut output_spec = Document::new();
        output_spec.insert(name.to_string(), Bson::Document(Document::new()));
        let mut output = Document::new();
        output.insert("value", Bson::Document(output_spec));
        doc! {
            "$setWindowFields": {
                "sortBy": { "_id": 1 },
                "output": Bson::Document(output),
            }
        }
    }
}
