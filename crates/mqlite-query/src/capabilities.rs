pub const SUPPORTED_QUERY_OPERATORS: &[&str] = &[
    "$alwaysFalse",
    "$alwaysTrue",
    "$and",
    "$or",
    "$nor",
    "$bitsAllClear",
    "$bitsAllSet",
    "$bitsAnyClear",
    "$bitsAnySet",
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
    "$exists",
    "$size",
    "$mod",
    "$all",
    "$comment",
    "$not",
    "$type",
    "$regex",
    "$options",
    "$elemMatch",
    "$expr",
    "$sampleRate",
];

pub const SUPPORTED_AGGREGATION_STAGES: &[&str] = &[
    "$bucket",
    "$bucketAuto",
    "$collStats",
    "$currentOp",
    "$documents",
    "$facet",
    "$indexStats",
    "$lookup",
    "$sample",
    "$sortByCount",
    "$unionWith",
    "$match",
    "$merge",
    "$out",
    "$project",
    "$set",
    "$addFields",
    "$unset",
    "$limit",
    "$skip",
    "$sort",
    "$count",
    "$unwind",
    "$group",
    "$replaceRoot",
    "$replaceWith",
];

pub const SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS: &[&str] = &[
    "$literal", "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$and", "$or", "$not", "$in",
];

pub const SUPPORTED_AGGREGATION_ACCUMULATORS: &[&str] =
    &["$sum", "$first", "$push", "$addToSet", "$avg"];

pub const SUPPORTED_AGGREGATION_WINDOW_OPERATORS: &[&str] = &[];
